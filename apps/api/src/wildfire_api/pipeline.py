"""Pipeline health: per-source freshness, throughput, consumer lag, and dead letters.

Freshness and per-source throughput come from the PostGIS projections, which hold
the event times (``observed_at``, ``ingested_at``, ``generated_at``). Topic
throughput, consumer lag, and dead-letter counts come from Kafka offsets and the
dead-letter topic. Either half can fail on its own: the response then carries the
other half and names the failure in ``errors``.

Collecting the status costs a full scan of ``fire_detections`` and a round of
Kafka metadata requests, so results are cached for ``CACHE_SECONDS``.
"""

import logging
import os
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import Literal, Protocol, cast

import psycopg
from fastapi import APIRouter
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.structs import OffsetAndTimestamp, TopicPartition
from pydantic import BaseModel

from wildfire_api.db import connect, fetch_all
from wildfire_events.models import FIRMS_PRODUCTS, FirmsProduct
from wildfire_events.topics import (
    ALL_TOPICS,
    CONSUMER_GROUPS,
    FIRMS_DETECTIONS,
    SINK_WORKER_DLQ,
    WEATHER_FORECASTS,
)

LOGGER = logging.getLogger(__name__)
router = APIRouter(tags=["pipeline"])

CACHE_SECONDS = 10
DEAD_LETTER_SAMPLE = 5000
RECENT_DEAD_LETTERS = 10
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)

Freshness = Literal["fresh", "delayed", "stale", "no_data", "unknown"]


@dataclass(frozen=True)
class SourceSpec:
    id: str
    label: str
    kind: Literal["observation", "forecast"]
    topic: str
    fresh_within: timedelta
    stale_after: timedelta


# Polar orbiters pass over the region about twice a day and FIRMS NRT data arrives
# about three hours after acquisition, so the newest detection of a healthy product
# can be up to ~15 hours old.
FIRMS_FRESH_WITHIN = timedelta(hours=18)
FIRMS_STALE_AFTER = timedelta(hours=36)
FIRMS_LABELS: dict[FirmsProduct, str] = {
    "VIIRS_SNPP_NRT": "VIIRS S-NPP",
    "VIIRS_NOAA20_NRT": "VIIRS NOAA-20",
    "VIIRS_NOAA21_NRT": "VIIRS NOAA-21",
    "MODIS_NRT": "MODIS Terra/Aqua",
}
# The ingestor refreshes forecasts every three hours by default.
FORECAST_FRESH_WITHIN = timedelta(hours=6)
FORECAST_STALE_AFTER = timedelta(hours=24)

SOURCES = (
    *(
        SourceSpec(
            product,
            FIRMS_LABELS[product],
            "observation",
            FIRMS_DETECTIONS.name,
            FIRMS_FRESH_WITHIN,
            FIRMS_STALE_AFTER,
        )
        for product in FIRMS_PRODUCTS
    ),
    SourceSpec(
        "open-meteo",
        "Open-Meteo forecast",
        "forecast",
        WEATHER_FORECASTS.name,
        FORECAST_FRESH_WITHIN,
        FORECAST_STALE_AFTER,
    ),
)


class SourceStatus(BaseModel):
    id: str
    label: str
    kind: Literal["observation", "forecast"]
    topic: str
    freshness: Freshness
    latest_at: datetime | None
    """Newest observation time, or for a forecast the newest retrieval time."""
    oldest_retrieved_at: datetime | None = None
    """Forecasts only: the oldest cell forecast's retrieval time."""
    forecast_until: datetime | None = None
    latest_ingested_at: datetime | None
    latest_stored_at: datetime | None
    ingested_last_hour: int | None
    ingested_last_day: int | None
    stored: int | None
    """Detections, or forecast cells, in PostGIS."""
    fresh_within_hours: float
    stale_after_hours: float


class TopicStatus(BaseModel):
    name: str
    partitions: int
    retained: int
    """Offset span of the retained log; compacted topics hold fewer records."""
    messages_last_hour: int
    messages_last_day: int
    dead_letters_last_day: int


class PartitionLag(BaseModel):
    partition: int
    committed: int | None
    end: int
    lag: int


class ConsumerStatus(BaseModel):
    group_id: str
    topic: str
    state: str
    members: int
    lag: int
    partitions: list[PartitionLag]


class DeadLetter(BaseModel):
    failed_at: datetime
    topic: str | None
    partition: int | None
    offset: int | None
    group_id: str | None
    error: str


class DeadLetterStatus(BaseModel):
    topic: str
    retained: int
    last_day: int
    by_topic: dict[str, int]
    """Dead letters of the last 24 hours per original topic."""
    sample_truncated: bool
    """``by_topic`` only counts the newest ``DEAD_LETTER_SAMPLE`` records."""
    recent: list[DeadLetter]


class PipelineStatus(BaseModel):
    checked_at: datetime
    sources: list[SourceStatus]
    topics: list[TopicStatus] | None
    consumers: list[ConsumerStatus] | None
    dead_letters: DeadLetterStatus | None
    errors: list[str]


def classify(
    latest: datetime | None, now: datetime, fresh_within: timedelta, stale_after: timedelta
) -> Freshness:
    if latest is None:
        return "no_data"
    age = now - latest
    if age <= fresh_within:
        return "fresh"
    return "delayed" if age <= stale_after else "stale"


# --- PostGIS -----------------------------------------------------------------------


class _DetectionStats(BaseModel):
    product: str
    latest_observed_at: datetime
    latest_ingested_at: datetime
    latest_stored_at: datetime
    ingested_last_hour: int
    ingested_last_day: int
    stored: int


class _ForecastStats(BaseModel):
    newest: datetime | None
    oldest: datetime | None
    latest_stored_at: datetime | None
    forecast_until: datetime | None
    ingested_last_hour: int
    ingested_last_day: int
    stored: int


class _Exists(BaseModel):
    detections: bool
    forecasts: bool


DETECTION_STATS = """
SELECT product,
       max(observed_at) AS latest_observed_at,
       max(ingested_at) AS latest_ingested_at,
       max(stored_at) AS latest_stored_at,
       count(*) FILTER (WHERE ingested_at >= now() - interval '1 hour') AS ingested_last_hour,
       count(*) FILTER (WHERE ingested_at >= now() - interval '1 day') AS ingested_last_day,
       count(*) AS stored
FROM fire_detections
GROUP BY product
"""

FORECAST_STATS = """
SELECT max(generated_at) AS newest,
       min(generated_at) AS oldest,
       max(stored_at) AS latest_stored_at,
       (SELECT max(valid_at) FROM weather_hourly) AS forecast_until,
       count(*) FILTER (WHERE generated_at >= now() - interval '1 hour') AS ingested_last_hour,
       count(*) FILTER (WHERE generated_at >= now() - interval '1 day') AS ingested_last_day,
       count(*) AS stored
FROM weather_cells
"""


def _hours(value: timedelta) -> float:
    return value.total_seconds() / 3600


def unknown_source(spec: SourceSpec) -> SourceStatus:
    """A source whose projection could not be read."""
    return SourceStatus(
        id=spec.id,
        label=spec.label,
        kind=spec.kind,
        topic=spec.topic,
        freshness="unknown",
        latest_at=None,
        latest_ingested_at=None,
        latest_stored_at=None,
        ingested_last_hour=None,
        ingested_last_day=None,
        stored=None,
        fresh_within_hours=_hours(spec.fresh_within),
        stale_after_hours=_hours(spec.stale_after),
    )


def source_statuses(
    detections: Iterable[_DetectionStats], forecast: _ForecastStats | None, now: datetime
) -> list[SourceStatus]:
    by_product = {row.product: row for row in detections}
    statuses: list[SourceStatus] = []
    for spec in SOURCES:
        status = unknown_source(spec)
        if spec.kind == "observation":
            row = by_product.get(spec.id)
            status.ingested_last_hour = status.ingested_last_day = status.stored = 0
            if row is not None:
                status.latest_at = row.latest_observed_at
                status.latest_ingested_at = row.latest_ingested_at
                status.latest_stored_at = row.latest_stored_at
                status.ingested_last_hour = row.ingested_last_hour
                status.ingested_last_day = row.ingested_last_day
                status.stored = row.stored
        else:
            status.ingested_last_hour = status.ingested_last_day = status.stored = 0
            if forecast is not None:
                status.latest_at = status.latest_ingested_at = forecast.newest
                status.oldest_retrieved_at = forecast.oldest
                status.forecast_until = forecast.forecast_until
                status.latest_stored_at = forecast.latest_stored_at
                status.ingested_last_hour = forecast.ingested_last_hour
                status.ingested_last_day = forecast.ingested_last_day
                status.stored = forecast.stored
        status.freshness = classify(status.latest_at, now, spec.fresh_within, spec.stale_after)
        statuses.append(status)
    return statuses


def collect_sources(now: datetime) -> list[SourceStatus]:
    """Freshness per source from PostGIS; raises ``psycopg.Error`` if it is unavailable."""
    with connect() as connection:
        [exists] = fetch_all(
            connection,
            _Exists,
            "SELECT to_regclass('fire_detections') IS NOT NULL AS detections, "
            + "to_regclass('weather_cells') IS NOT NULL "
            + "AND to_regclass('weather_hourly') IS NOT NULL AS forecasts",
        )
        detections = (
            fetch_all(connection, _DetectionStats, DETECTION_STATS) if exists.detections else []
        )
        forecast = (
            fetch_all(connection, _ForecastStats, FORECAST_STATS)[0] if exists.forecasts else None
        )
    return source_statuses(detections, forecast, now)


# --- Kafka -------------------------------------------------------------------------


class HeaderRecord(Protocol):
    """The parts of a ``ConsumerRecord`` read from the dead-letter topic."""

    @property
    def timestamp(self) -> int: ...

    @property
    def headers(self) -> Sequence[tuple[str, bytes | None]] | None: ...


def messages_since(
    end: Mapping[TopicPartition, int], found: Mapping[TopicPartition, OffsetAndTimestamp | None]
) -> int:
    """Records appended after a point in time, from ``offsets_for_times`` results."""
    return sum(
        end[tp] - match.offset
        for tp, match in found.items()
        if match is not None and tp in end
    )


def consumer_status(
    group_id: str,
    topic: str,
    state: str,
    members: int,
    committed: Mapping[int, int],
    beginning: Mapping[int, int],
    end: Mapping[int, int],
) -> ConsumerStatus:
    """Lag per partition; without a committed offset the group starts at the beginning."""
    partitions: list[PartitionLag] = []
    for partition in sorted(end):
        offset = committed.get(partition)
        start = offset if offset is not None else beginning.get(partition, 0)
        partitions.append(
            PartitionLag(
                partition=partition,
                committed=offset,
                end=end[partition],
                lag=max(0, end[partition] - start),
            )
        )
    return ConsumerStatus(
        group_id=group_id,
        topic=topic,
        state=state,
        members=members,
        lag=sum(item.lag for item in partitions),
        partitions=partitions,
    )


def _int_or_none(value: str | None) -> int | None:
    try:
        return None if value is None else int(value)
    except ValueError:
        return None


def dead_letter(record: HeaderRecord) -> DeadLetter:
    headers = {
        key: value.decode(errors="replace")
        for key, value in record.headers or []
        if key.startswith("dlq.") and value is not None
    }
    return DeadLetter(
        failed_at=datetime.fromtimestamp(record.timestamp / 1000, UTC),
        topic=headers.get("dlq.original.topic"),
        partition=_int_or_none(headers.get("dlq.original.partition")),
        offset=_int_or_none(headers.get("dlq.original.offset")),
        group_id=headers.get("dlq.consumer.group"),
        error=headers.get("dlq.error", "")[:300],
    )


def summarize_dead_letters(
    records: Sequence[HeaderRecord], retained: int, last_day: int
) -> DeadLetterStatus:
    letters = sorted((dead_letter(record) for record in records), key=lambda item: item.failed_at)
    counts = Counter(letter.topic or "unknown" for letter in letters)
    return DeadLetterStatus(
        topic=SINK_WORKER_DLQ.name,
        retained=retained,
        last_day=last_day,
        by_topic=dict(counts),
        sample_truncated=last_day > len(letters),
        recent=list(reversed(letters[-RECENT_DEAD_LETTERS:])),
    )


@dataclass
class KafkaSection:
    topics: list[TopicStatus]
    consumers: list[ConsumerStatus]
    dead_letters: DeadLetterStatus


def _offsets(raw: object) -> dict[TopicPartition, int]:
    return cast(dict[TopicPartition, int], raw)


def _found(raw: object) -> dict[TopicPartition, OffsetAndTimestamp | None]:
    return cast(dict[TopicPartition, OffsetAndTimestamp | None], raw)


class KafkaInspector:
    """Short-lived Kafka clients that read offsets, group state, and dead letters."""

    def __init__(self, bootstrap_servers: str) -> None:
        self.consumer: KafkaConsumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            client_id="api-pipeline-status",
            group_id=None,
            enable_auto_commit=False,
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=5000,
        )
        try:
            self.admin: KafkaAdminClient = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="api-pipeline-status-admin",
                request_timeout_ms=10000,
                api_version_auto_timeout_ms=5000,
            )
        except Exception:
            self.consumer.close()
            raise

    def close(self) -> None:
        self.admin.close()
        self.consumer.close(autocommit=False)

    def _partitions(self, topic: str) -> list[TopicPartition]:
        partitions = cast(set[int] | None, self.consumer.partitions_for_topic(topic)) or set()
        return [TopicPartition(topic, partition) for partition in sorted(partitions)]

    def collect(self, now: datetime) -> KafkaSection:
        hour_ago = int((now - HOUR).timestamp() * 1000)
        day_ago = int((now - DAY).timestamp() * 1000)
        assignments = {spec.name: self._partitions(spec.name) for spec in ALL_TOPICS}
        everything = [tp for tps in assignments.values() for tp in tps]
        beginning = _offsets(self.consumer.beginning_offsets(everything))
        end = _offsets(self.consumer.end_offsets(everything))
        since_hour = _found(self.consumer.offsets_for_times({tp: hour_ago for tp in everything}))
        since_day = _found(self.consumer.offsets_for_times({tp: day_ago for tp in everything}))

        dlq = assignments[SINK_WORKER_DLQ.name]
        letters = self._recent_dead_letters(dlq, end, since_day)
        dead_letters = summarize_dead_letters(
            letters,
            retained=sum(end[tp] - beginning[tp] for tp in dlq),
            last_day=messages_since(end, {tp: since_day.get(tp) for tp in dlq}),
        )

        topics: list[TopicStatus] = []
        for spec in ALL_TOPICS:
            tps = assignments[spec.name]
            topics.append(
                TopicStatus(
                    name=spec.name,
                    partitions=len(tps),
                    retained=sum(end[tp] - beginning[tp] for tp in tps),
                    messages_last_hour=messages_since(end, {tp: since_hour.get(tp) for tp in tps}),
                    messages_last_day=messages_since(end, {tp: since_day.get(tp) for tp in tps}),
                    dead_letters_last_day=dead_letters.by_topic.get(spec.name, 0),
                )
            )

        groups = {
            group.group: group
            for group in self.admin.describe_consumer_groups(list(CONSUMER_GROUPS))
        }
        consumers: list[ConsumerStatus] = []
        for group_id, topic in CONSUMER_GROUPS.items():
            committed = self.admin.list_consumer_group_offsets(group_id)
            described = groups.get(group_id)
            tps = assignments.get(topic, [])
            consumers.append(
                consumer_status(
                    group_id,
                    topic,
                    state=str(described.state) if described else "Unknown",
                    members=len(cast(list[object], described.members)) if described else 0,
                    committed={
                        tp.partition: meta.offset
                        for tp, meta in committed.items()
                        if tp.topic == topic and meta.offset >= 0
                    },
                    beginning={tp.partition: beginning[tp] for tp in tps},
                    end={tp.partition: end[tp] for tp in tps},
                )
            )
        return KafkaSection(topics, consumers, dead_letters)

    def _recent_dead_letters(
        self,
        partitions: list[TopicPartition],
        end: Mapping[TopicPartition, int],
        since_day: Mapping[TopicPartition, OffsetAndTimestamp | None],
    ) -> list[HeaderRecord]:
        """Dead letters of the last 24 hours, at most the newest ``DEAD_LETTER_SAMPLE``."""
        starts: dict[TopicPartition, int] = {}
        for tp in partitions:
            match = since_day.get(tp)
            if match is not None and max(match.offset, end[tp] - DEAD_LETTER_SAMPLE) < end[tp]:
                starts[tp] = max(match.offset, end[tp] - DEAD_LETTER_SAMPLE)
        pending = {tp: end[tp] for tp in starts}
        if starts:
            self.consumer.assign(list(starts))
            for tp, start in starts.items():
                self.consumer.seek(tp, start)
        records: list[HeaderRecord] = []
        deadline = time.monotonic() + 5
        while pending and time.monotonic() < deadline:
            polled = self.consumer.poll(timeout_ms=500, max_records=DEAD_LETTER_SAMPLE)
            for tp, batch in polled.items():
                for record in batch:
                    if record.offset < pending.get(tp, 0):
                        records.append(cast(HeaderRecord, record))
            pending = {
                tp: stop
                for tp, stop in pending.items()
                if cast(int, self.consumer.position(tp)) < stop
            }
        if pending:
            LOGGER.warning("Timed out reading %s; the sample is partial", SINK_WORKER_DLQ.name)
        return records[-DEAD_LETTER_SAMPLE:]


def collect_kafka(now: datetime) -> KafkaSection:
    inspector = KafkaInspector(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    try:
        return inspector.collect(now)
    finally:
        inspector.close()


# --- Endpoint ----------------------------------------------------------------------


def collect_status() -> PipelineStatus:
    now = datetime.now(UTC)
    errors: list[str] = []
    try:
        sources = collect_sources(now)
    except psycopg.Error:
        LOGGER.exception("Pipeline status: PostGIS is unavailable")
        errors.append("PostGIS is unavailable; source freshness is unknown")
        sources = [unknown_source(spec) for spec in SOURCES]
    kafka: KafkaSection | None = None
    try:
        kafka = collect_kafka(now)
    except Exception:
        LOGGER.exception("Pipeline status: Kafka is unavailable")
        errors.append("Kafka is unavailable; throughput, lag, and dead letters are unknown")
    return PipelineStatus(
        checked_at=now,
        sources=sources,
        topics=kafka.topics if kafka else None,
        consumers=kafka.consumers if kafka else None,
        dead_letters=kafka.dead_letters if kafka else None,
        errors=errors,
    )


_cache_lock = Lock()
_cached: tuple[float, PipelineStatus] | None = None


def clear_cache() -> None:
    global _cached
    with _cache_lock:
        _cached = None


@router.get("/pipeline/status", response_model=PipelineStatus)
def pipeline_status() -> PipelineStatus:
    """Freshness, throughput, consumer lag, and dead-letter counts for each source.

    A source is ``fresh`` while its newest observation (or forecast retrieval) is
    younger than ``fresh_within_hours``, ``delayed`` until ``stale_after_hours``,
    then ``stale``; ``no_data`` means nothing was ingested and ``unknown`` that
    PostGIS could not be read. Kafka sections are null when Kafka is unavailable.
    Results are cached for a few seconds.
    """
    global _cached
    with _cache_lock:
        if _cached is not None and time.monotonic() - _cached[0] < CACHE_SECONDS:
            return _cached[1]
        status = collect_status()
        _cached = (time.monotonic(), status)
        return status
