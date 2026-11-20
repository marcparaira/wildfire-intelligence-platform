"""Live detections pushed to the browser with Server-Sent Events.

Each connection gets its own Kafka consumer with no consumer group. It assigns
itself every partition of ``firms.detections``, so every API instance sees every
event, filters it for the connected client, and keeps no offsets on the broker.

Each SSE message carries one poll's worth of detections, and its ID is the
stream position after that batch, written as ``partition:offset`` pairs. When
the connection drops, EventSource sends that ID back as ``Last-Event-ID`` and the
stream resumes exactly where it stopped. A new connection without an ID starts a
short look-back before the end of the topic, which covers events the sink worker
had not yet written to PostGIS when the browser loaded its snapshot; the browser
drops detections it already has by ``event_id``.

Kafka or schema-registry failures end the stream; the browser reconnects after
the ``retry`` delay and resumes from its last ID.
"""

import logging
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import datetime
from functools import cache
from typing import Annotated, Protocol, cast
from uuid import UUID

import anyio
from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import StreamingResponse
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from wildfire_api.bounds import Bounds, BoundsDep
from wildfire_api.settings import Settings, SettingsDep
from wildfire_events.codec import EventCodec, InvalidEvent
from wildfire_events.models import FirmsDetection
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import FIRMS_DETECTIONS, value_subject

LOGGER = logging.getLogger(__name__)
router = APIRouter(tags=["live"])

TOPIC = FIRMS_DETECTIONS.name
POLL_TIMEOUT_MS = 1000
MAX_BATCH = 500
KEEPALIVE_SECONDS = 15
RECONNECT_MS = 5000
LOOKBACK_MS = 2 * 60 * 1000
REQUEST_TIMEOUT_MS = 30_000

Position = dict[int, int]
"""Next offset to read, per partition."""


class LiveDetection(BaseModel):
    """A detection as ``GET /hotspots`` returns it, before the sink worker stores it.

    ``id`` is the PostGIS row ID and is not known yet. ``duplicate_of`` is always
    null: cross-sensor duplicates are only resolved in the projection, so a
    refresh can hide a detection that arrived live. ``country``, ``risk``, and
    ``weather`` are null too: enrichment happens downstream of this topic, and a
    refresh fills them in.
    """

    id: None = None
    event_id: UUID
    source: str
    instrument: str
    satellite: str
    observed_at: datetime
    latitude: float
    longitude: float
    attributes: dict[str, str]
    duplicate_of: None = None
    country: None = None
    risk: None = None
    weather: None = None

    @classmethod
    def from_event(cls, event: FirmsDetection) -> "LiveDetection":
        return cls(
            event_id=event.event_id,
            source=event.product,
            instrument=event.instrument,
            satellite=event.satellite,
            observed_at=event.observed_at,
            latitude=event.latitude,
            longitude=event.longitude,
            attributes=event.attributes,
        )


def format_position(position: Mapping[int, int]) -> str:
    return ",".join(f"{partition}:{offset}" for partition, offset in sorted(position.items()))


def parse_position(value: str | None) -> Position:
    """Parse a ``Last-Event-ID``; anything malformed means "no position"."""
    if not value:
        return {}
    position: Position = {}
    try:
        for pair in value.split(","):
            partition, offset = pair.split(":")
            position[int(partition)] = int(offset)
    except ValueError:
        LOGGER.warning("Ignoring malformed Last-Event-ID %r", value[:100])
        return {}
    if any(partition < 0 or offset < 0 for partition, offset in position.items()):
        return {}
    return position


def sse_message(event: str | None, data: str | None, event_id: str) -> str:
    lines = [f"id: {event_id}"]
    if event:
        lines.append(f"event: {event}")
    if data is not None:
        lines.append(f"data: {data}")
    return "\n".join(lines) + "\n\n"


@cache
def registry(url: str) -> SchemaRegistry:
    """One registry client per URL, so schema IDs are looked up once per process."""
    return SchemaRegistry(url)


class DetectionDecoder(Protocol):
    """Decodes a record value; ``EventCodec[FirmsDetection]`` implements it."""

    def decode(self, value: bytes | None) -> FirmsDetection: ...


class PolledRecord(Protocol):
    """The parts of a ``ConsumerRecord`` the live feed reads."""

    @property
    def offset(self) -> int: ...

    @property
    def value(self) -> object: ...


class DetectionBatcher:
    """Turns polled records into the detections one client should see.

    ``position`` advances past every record read, including invalid and filtered
    ones, so a resumed stream never re-reads them.
    """

    def __init__(self, decoder: DetectionDecoder, position: Mapping[int, int]) -> None:
        self.decoder: DetectionDecoder = decoder
        self.position: Position = dict(position)

    def matching(
        self, polled: Mapping[TopicPartition, Sequence[PolledRecord]], bounds: Bounds | None
    ) -> list[LiveDetection]:
        batch: list[LiveDetection] = []
        for tp, records in polled.items():
            for record in records:
                try:
                    event = self.decoder.decode(cast(bytes | None, record.value))
                except InvalidEvent as exc:
                    # The sink worker dead-letters these; the live view just skips them.
                    LOGGER.debug("Skipping %s[%d]@%d: %s", TOPIC, tp.partition, record.offset, exc)
                    continue
                if bounds is None or bounds.contains(event.latitude, event.longitude):
                    batch.append(LiveDetection.from_event(event))
            if records:
                self.position[tp.partition] = records[-1].offset + 1
        return batch


class DetectionFeed:
    """Blocking Kafka side of one SSE connection; every call runs in a worker thread."""

    def __init__(self, consumer: KafkaConsumer, decoder: DetectionDecoder) -> None:
        self.consumer: KafkaConsumer = consumer
        self.batcher: DetectionBatcher = DetectionBatcher(decoder, {})

    @classmethod
    def connect(cls, bootstrap_servers: str, decoder: DetectionDecoder) -> "DetectionFeed":
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            client_id="api-live-detections",
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            max_poll_records=MAX_BATCH,
            request_timeout_ms=REQUEST_TIMEOUT_MS,
        )
        return cls(consumer, decoder)

    @property
    def position(self) -> Position:
        return self.batcher.position

    def start(self, resume_from: Mapping[int, int]) -> None:
        """Assign every partition and seek to ``resume_from``, or the look-back start."""
        partitions = cast(set[int] | None, self.consumer.partitions_for_topic(TOPIC))
        if not partitions:
            raise RuntimeError(f"topic {TOPIC} has no partitions (does it exist?)")
        assigned = [TopicPartition(TOPIC, partition) for partition in sorted(partitions)]
        self.consumer.assign(assigned)
        fresh = [tp for tp in assigned if tp.partition not in resume_from]
        for tp in assigned:
            if tp.partition in resume_from:
                self.consumer.seek(tp, resume_from[tp.partition])
        if fresh:
            self._seek_to_lookback(fresh)
        self.batcher.position = {
            tp.partition: cast(int, self.consumer.position(tp)) for tp in assigned
        }

    def _seek_to_lookback(self, partitions: list[TopicPartition]) -> None:
        since = int(time.time() * 1000) - LOOKBACK_MS
        found = self.consumer.offsets_for_times(dict.fromkeys(partitions, since))
        for tp in partitions:
            match = found.get(tp)
            if match is None:
                self.consumer.seek_to_end(tp)
            else:
                self.consumer.seek(tp, match.offset)

    def poll(self, bounds: Bounds | None) -> list[LiveDetection]:
        """Next batch of matching detections; advances ``position`` past every record read."""
        polled = self.consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=MAX_BATCH)
        return self.batcher.matching(polled, bounds)

    def close(self) -> None:
        self.consumer.close(autocommit=False)


async def detection_events(
    request: Request, settings: Settings, bounds: Bounds | None, resume_from: Position
) -> AsyncIterator[str]:
    """The SSE body. Failures end the stream (logged); the browser reconnects and resumes."""
    yield f"retry: {RECONNECT_MS}\n\n"
    codec = EventCodec(FirmsDetection, value_subject(TOPIC), registry(settings.schema_registry_url))
    try:
        feed = await run_in_threadpool(
            DetectionFeed.connect, settings.kafka_bootstrap_servers, codec
        )
    except Exception:
        # The response has started, so an error status is impossible: ending the
        # stream makes the browser retry after RECONNECT_MS.
        LOGGER.exception("Live detections: Kafka is unavailable")
        return
    try:
        await run_in_threadpool(feed.start, resume_from)
        # Browsers drop events without data, so "ready" carries an empty object.
        yield sse_message("ready", "{}", format_position(feed.position))
        last_sent = time.monotonic()
        while not await request.is_disconnected():
            batch = await run_in_threadpool(feed.poll, bounds)
            event_id = format_position(feed.position)
            if batch:
                data = "[" + ",".join(item.model_dump_json() for item in batch) + "]"
                yield sse_message("detections", data, event_id)
                last_sent = time.monotonic()
            elif time.monotonic() - last_sent >= KEEPALIVE_SECONDS:
                # An ID-only message dispatches nothing in the browser but still moves
                # EventSource's last event ID, and keeps proxies from timing out.
                yield sse_message(None, None, event_id)
                last_sent = time.monotonic()
    except Exception:
        # Same boundary as above: any Kafka or schema-registry failure ends this one
        # stream, and the client resumes from its last event ID.
        LOGGER.exception("Live detections stream stopped; the client will reconnect")
    finally:
        # Cancellation (client gone) waits for any running poll to return first, so the
        # consumer is never used from two threads; shield the close from that cancel.
        with anyio.CancelScope(shield=True):
            await run_in_threadpool(feed.close)


@router.get("/events/detections", response_class=StreamingResponse)
async def live_detections(
    request: Request,
    settings: SettingsDep,
    bounds: BoundsDep,
    last_event_id: Annotated[str | None, Header()] = None,
    resume_from: Annotated[str | None, Query(alias="last_event_id")] = None,
) -> StreamingResponse:
    """Server-Sent Events stream of new FIRMS detections.

    Emits a ``ready`` event once the stream is positioned, then ``detections``
    events whose data is a JSON array shaped like ``GET /hotspots`` items, with a
    null ``id``. The optional bounds limit the stream to one area.

    EventSource resumes with the ``Last-Event-ID`` header on its own. The
    ``last_event_id`` query parameter does the same for a client that has to open
    a new EventSource, which cannot set headers.
    """
    return StreamingResponse(
        detection_events(request, settings, bounds, parse_position(last_event_id or resume_from)),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
