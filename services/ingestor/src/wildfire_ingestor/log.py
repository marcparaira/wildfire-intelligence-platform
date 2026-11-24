"""Reading recent state back from Kafka topics.

The ingestor keeps no local state: on startup it learns what it already published
by reading the tail of its own topics, and the imagery producer reads the current
incidents from ``fire.incidents``.
"""

import time
from datetime import UTC, datetime
from typing import Final, cast

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import TopicPartition

from wildfire_ingestor.publisher import EVENT_ID_HEADER, PublishedEvents

CLIENT_ID: Final = "wildfire-ingestor-state"
POLL_TIMEOUT_MS = 1000
LOAD_MAX_RECORDS = 5000
LATEST_DEADLINE_SECONDS = 30
LOAD_DEADLINE_SECONDS = 300
# kafka-python 2.3 fetches up to 50 MB per request but by default rejects network frames
# over 1 MB, so reading a topic's backlog fails and reconnects. The frame limit must
# cover the largest fetch response.
FETCH_MAX_BYTES = 50 * 1024 * 1024
RECEIVE_MAX_BYTES = FETCH_MAX_BYTES + 1024 * 1024


def _behind(consumer: KafkaConsumer, partition: TopicPartition, end: int) -> bool:
    """Whether the consumer has not yet read up to ``end`` (unknown position counts)."""
    position = consumer.position(partition)
    return position is None or position < end


def _assigned_consumer(
    bootstrap_servers: str, topic: str
) -> tuple[KafkaConsumer, list[TopicPartition]]:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=None,
        enable_auto_commit=False,
        client_id=CLIENT_ID,
        fetch_max_bytes=FETCH_MAX_BYTES,
        receive_message_max_bytes=RECEIVE_MAX_BYTES,  # pyright: ignore[reportCallIssue] -- missing from the typeshed stubs
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
        newest: int | None = None
        deadline = time.monotonic() + LATEST_DEADLINE_SECONDS
        while pending and time.monotonic() < deadline:
            for tp, records in consumer.poll(timeout_ms=POLL_TIMEOUT_MS).items():
                for record in records:
                    newest = record.timestamp if newest is None else max(newest, record.timestamp)
                pending.discard(tp)
        return None if newest is None else datetime.fromtimestamp(newest / 1000, UTC)
    finally:
        consumer.close()


def load_published_events(bootstrap_servers: str, topic: str, published: PublishedEvents) -> int:
    """Add the event-ID headers of records published within the window; return the count.

    Raises ``TimeoutError`` if the topic tail cannot be read in time, because
    starting with a partial set would republish events.
    """
    since = datetime.now(UTC) - published.window
    consumer, assignment = _assigned_consumer(bootstrap_servers, topic)
    loaded = 0
    try:
        if not assignment:
            return 0
        end = consumer.end_offsets(assignment)
        pending = _seek_to_time(consumer, end, since)
        deadline = time.monotonic() + LOAD_DEADLINE_SECONDS
        while pending and time.monotonic() < deadline:
            polled = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=LOAD_MAX_RECORDS)
            for records in polled.values():
                for record in records:
                    for key, value in record.headers or []:
                        if key == EVENT_ID_HEADER:
                            published.add(value.decode(), record.timestamp / 1000)
                            loaded += 1
            pending = {tp for tp in pending if _behind(consumer, tp, end[tp])}
        if pending:
            raise TimeoutError(f"timed out reading published events from {topic}")
    finally:
        consumer.close()
    return loaded


def _seek_to_time(
    consumer: KafkaConsumer, end: dict[TopicPartition, int], since: datetime
) -> set[TopicPartition]:
    """Seek each partition to its first record at or after ``since``.

    Returns the partitions that have records to read before their ``end`` offset.
    """
    starts = consumer.offsets_for_times({tp: int(since.timestamp() * 1000) for tp in end})
    pending: set[TopicPartition] = set()
    for tp in end:
        start = starts.get(tp)
        consumer.seek(tp, end[tp] if start is None else start.offset)
        if start is not None and start.offset < end[tp]:
            pending.add(tp)
    return pending


def latest_values(
    bootstrap_servers: str, topic: str, since: datetime | None = None
) -> dict[bytes, bytes | None]:
    """The newest value per key among records published since ``since`` (default: all).

    Meant for compacted topics, read like a table; a tombstone yields ``None``.
    Records without a key are ignored. Raises ``TimeoutError`` if the topic cannot
    be read to its current end in time.
    """
    consumer, assignment = _assigned_consumer(bootstrap_servers, topic)
    values: dict[bytes, bytes | None] = {}
    try:
        if not assignment:
            return values
        end = consumer.end_offsets(assignment)
        if since is None:
            consumer.seek_to_beginning(*assignment)
            pending = {tp for tp in assignment if _behind(consumer, tp, end[tp])}
        else:
            pending = _seek_to_time(consumer, end, since)
        deadline = time.monotonic() + LOAD_DEADLINE_SECONDS
        while pending and time.monotonic() < deadline:
            polled = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=LOAD_MAX_RECORDS)
            for records in polled.values():
                for record in records:
                    _keep_latest(values, record)
            pending = {tp for tp in pending if _behind(consumer, tp, end[tp])}
        if pending:
            raise TimeoutError(f"timed out reading {topic}")
    finally:
        consumer.close()
    return values


def _keep_latest(values: dict[bytes, bytes | None], record: ConsumerRecord) -> None:
    # A key always maps to one partition, so records of a key arrive in offset order.
    key = cast(bytes | None, record.key)
    if key is not None:
        values[key] = cast(bytes | None, record.value)
