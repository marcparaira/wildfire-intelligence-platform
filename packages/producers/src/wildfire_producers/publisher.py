"""Publishing events through the schema-registry codec with an idempotent producer."""

import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from threading import Lock
from typing import Protocol

from pydantic import BaseModel

EVENT_ID_HEADER = "event_id"
SEND_TIMEOUT_SECONDS = 60


class PublishedEvents:
    """Event IDs already in a topic within a sliding publish-time window.

    Thread-safe: the scheduled producers share one instance.
    """

    def __init__(self, window: timedelta) -> None:
        self.window: timedelta = window
        self._ids: dict[str, float] = {}
        self._lock: Lock = Lock()

    def __contains__(self, event_id: object) -> bool:
        with self._lock:
            return str(event_id) in self._ids

    def __len__(self) -> int:
        with self._lock:
            return len(self._ids)

    def add(self, event_id: str, published_at: float | None = None) -> None:
        """Remember an ID; ``published_at`` is a Unix timestamp (default: now)."""
        with self._lock:
            self._ids[event_id] = time.time() if published_at is None else published_at

    def prune(self, now: float | None = None) -> None:
        """Forget IDs published before the window."""
        cutoff = (time.time() if now is None else now) - self.window.total_seconds()
        with self._lock:
            self._ids = {key: value for key, value in self._ids.items() if value >= cutoff}


class EventEncoder[T: BaseModel](Protocol):
    """Encodes an event as a record value; ``EventCodec`` implements it."""

    def encode(self, event: T) -> bytes: ...


class PublishedRecord(Protocol):
    @property
    def timestamp(self) -> int: ...


class SendResult(Protocol):
    def get(self, timeout: float | None = None) -> PublishedRecord: ...


class RecordSender(Protocol):
    """The part of ``KafkaProducer`` the publisher uses."""

    def send(
        self,
        topic: str,
        value: object = None,
        key: object = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
    ) -> SendResult: ...

    def flush(self, timeout: float | None = None) -> None: ...


@dataclass(frozen=True, slots=True)
class Publisher[T: BaseModel]:
    """Publishes events to one topic, keyed for partitioning.

    With ``event_id`` and ``published``, events whose ID is already in the topic
    (or earlier in the same batch) are skipped, and each published ID is recorded
    in ``published`` with its broker timestamp.
    """

    producer: RecordSender
    codec: EventEncoder[T]
    topic: str
    key: Callable[[T], str]
    event_id: Callable[[T], object] | None = None
    published: PublishedEvents | None = None

    def publish(self, events: Iterable[T]) -> tuple[int, int]:
        """Publish events not already in the topic; return (published, skipped).

        Blocks until every record is acknowledged; a failed send raises.
        """
        if self.published is not None:
            self.published.prune()
        pending: list[tuple[str | None, SendResult]] = []
        skipped = 0
        batch_ids: set[str] = set()
        for event in events:
            event_id = None if self.event_id is None else str(self.event_id(event))
            if event_id is not None and self._already_published(event_id, batch_ids):
                skipped += 1
                continue
            headers = [] if event_id is None else [(EVENT_ID_HEADER, event_id.encode())]
            future = self.producer.send(
                self.topic,
                key=self.key(event).encode(),
                value=self.codec.encode(event),
                headers=headers,
            )
            if event_id is not None:
                batch_ids.add(event_id)
            pending.append((event_id, future))
        self.producer.flush()
        for event_id, future in pending:
            metadata = future.get(timeout=SEND_TIMEOUT_SECONDS)
            if event_id is not None and self.published is not None:
                self.published.add(event_id, metadata.timestamp / 1000)
        return len(pending), skipped

    def _already_published(self, event_id: str, batch_ids: set[str]) -> bool:
        return event_id in batch_ids or (self.published is not None and event_id in self.published)
