"""Reading recent state back from Kafka topics.

The ingestor keeps no local state: on startup it learns what it already published
by reading the tail of its own topics.
"""

import logging
import time
from datetime import UTC, datetime, timedelta
from threading import Lock

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

LOGGER = logging.getLogger(__name__)
EVENT_ID_HEADER = "event_id"


def _assigned_consumer(bootstrap_servers: str, topic: str) -> tuple[KafkaConsumer, list]:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=None,
        enable_auto_commit=False,
        client_id="wildfire-ingestor-state",
    )
    partitions = consumer.partitions_for_topic(topic) or set()
    assignment = [TopicPartition(topic, partition) for partition in sorted(partitions)]
    consumer.assign(assignment)
    return consumer, assignment


def latest_publish_time(bootstrap_servers: str, topic: str) -> datetime | None:
    """Timestamp of the newest record in a topic, or None if it is empty."""
    consumer, assignment = _assigned_consumer(bootstrap_servers, topic)
    try:
        if not assignment:
            return None
        end = consumer.end_offsets(assignment)
        pending = {tp for tp in assignment if end[tp] > 0}
        for tp in pending:
            consumer.seek(tp, end[tp] - 1)
        newest = None
        deadline = time.monotonic() + 30
        while pending and time.monotonic() < deadline:
            for tp, records in consumer.poll(timeout_ms=1000).items():
                for record in records:
                    newest = max(newest or record.timestamp, record.timestamp)
                pending.discard(tp)
        return None if newest is None else datetime.fromtimestamp(newest / 1000, UTC)
    finally:
        consumer.close()


class PublishedEvents:
    """Event IDs already in a topic within a sliding publish-time window."""

    def __init__(self, window: timedelta) -> None:
        self.window = window
        self._ids: dict[str, float] = {}
        self._lock = Lock()

    def __contains__(self, event_id: object) -> bool:
        with self._lock:
            return str(event_id) in self._ids

    def __len__(self) -> int:
        with self._lock:
            return len(self._ids)

    def add(self, event_id: object, published_at: float | None = None) -> None:
        with self._lock:
            self._ids[str(event_id)] = time.time() if published_at is None else published_at

    def prune(self, now: float | None = None) -> None:
        cutoff = (time.time() if now is None else now) - self.window.total_seconds()
        with self._lock:
            self._ids = {key: value for key, value in self._ids.items() if value >= cutoff}

    def load(self, bootstrap_servers: str, topic: str) -> int:
        """Read event-ID headers of records published within the window."""
        since = datetime.now(UTC) - self.window
        consumer, assignment = _assigned_consumer(bootstrap_servers, topic)
        loaded = 0
        try:
            if not assignment:
                return 0
            end = consumer.end_offsets(assignment)
            starts = consumer.offsets_for_times(
                {tp: int(since.timestamp() * 1000) for tp in assignment}
            )
            pending = set()
            for tp in assignment:
                start = starts.get(tp)
                consumer.seek(tp, end[tp] if start is None else start.offset)
                if start is not None and start.offset < end[tp]:
                    pending.add(tp)
            deadline = time.monotonic() + 300
            while pending and time.monotonic() < deadline:
                for records in consumer.poll(timeout_ms=1000, max_records=5000).values():
                    for record in records:
                        for key, value in record.headers or []:
                            if key == EVENT_ID_HEADER:
                                self.add(value.decode(), record.timestamp / 1000)
                                loaded += 1
                pending = {tp for tp in pending if consumer.position(tp) < end[tp]}
            if pending:
                raise TimeoutError(f"timed out reading published events from {topic}")
        finally:
            consumer.close()
        return loaded
