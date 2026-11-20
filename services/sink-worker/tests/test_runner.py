from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import Event as StopEvent
from typing import Self, cast

import psycopg
import pytest
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import OffsetAndMetadata, TopicPartition
from pydantic import BaseModel

from wildfire_events.codec import InvalidEvent
from wildfire_sink.runner import DeadLetters, ProjectionWorker, RecordConsumer, process_batch

PARTITION = TopicPartition("firms.detections", 2)
GROUP_ID = "sink-worker.test"


class Event(BaseModel):
    name: str


Batch = Sequence[tuple[ConsumerRecord, Event | None]]


def record(offset: int, value: bytes | None = b"good") -> ConsumerRecord:
    return ConsumerRecord(
        topic=PARTITION.topic,
        partition=PARTITION.partition,
        leader_epoch=None,
        offset=offset,
        timestamp=0,
        timestamp_type=0,
        key=b"cell",
        value=value,
        headers=[],
        checksum=None,
        serialized_key_size=4,
        serialized_value_size=-1 if value is None else len(value),
        serialized_header_size=0,
    )


class FakeCodec:
    def decode(self, value: bytes | None) -> Event:
        if value != b"good":
            raise InvalidEvent("bad record")
        return Event(name="event")


@dataclass
class FakeProjection:
    fail: Callable[[Batch], None] = lambda _events: None
    tombstones: bool = False
    name: str = "test"
    topic: str = PARTITION.topic
    group_id: str = GROUP_ID
    codec: FakeCodec = field(default_factory=FakeCodec)
    applied: list[list[tuple[ConsumerRecord, Event | None]]] = field(default_factory=list)

    def initialize(self, connection: psycopg.Connection) -> None:
        del connection

    def apply(self, connection: psycopg.Connection, events: Batch) -> None:
        del connection
        self.fail(events)
        self.applied.append(list(events))


@dataclass
class FakeConsumer:
    commits: list[Mapping[TopicPartition, OffsetAndMetadata] | None] = field(default_factory=list)

    def commit(
        self,
        offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        del timeout_ms
        self.commits.append(offsets)


@dataclass
class FakeDeadLetters:
    sent: list[tuple[ConsumerRecord, str, str]] = field(default_factory=list)

    def send(self, record: ConsumerRecord, group_id: str, reason: str) -> None:
        self.sent.append((record, group_id, reason))


class FakeConnection:
    def transaction(self) -> AbstractContextManager[None]:
        return nullcontext()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


def connection() -> psycopg.Connection:
    return cast(psycopg.Connection, cast(object, FakeConnection()))


def test_invalid_records_are_dead_lettered_and_offsets_committed() -> None:
    consumer, dead, job = FakeConsumer(), FakeDeadLetters(), FakeProjection()
    records = [record(10), record(11, b"bad"), record(12)]

    assert process_batch(job, connection(), consumer, dead, PARTITION, records) == (2, 1)
    assert dead.sent == [(records[1], GROUP_ID, "bad record")]
    assert [[item[0].offset for item in batch] for batch in job.applied] == [[10, 12]]
    assert consumer.commits == [{PARTITION: OffsetAndMetadata(13, "", -1)}]


def test_transient_database_errors_never_commit() -> None:
    def fail(_events: Batch) -> None:
        raise psycopg.OperationalError("connection lost")

    consumer, dead, job = FakeConsumer(), FakeDeadLetters(), FakeProjection(fail)
    with pytest.raises(psycopg.OperationalError):
        _ = process_batch(job, connection(), consumer, dead, PARTITION, [record(1)])
    assert consumer.commits == []
    assert dead.sent == []


def test_rows_rejected_by_the_database_are_isolated_and_dead_lettered() -> None:
    def fail(events: Batch) -> None:
        if len(events) > 1 or events[0][0].offset == 6:
            raise psycopg.errors.CheckViolation("latitude out of range")

    consumer, dead = FakeConsumer(), FakeDeadLetters()
    records = [record(5), record(6), record(7)]
    result = process_batch(FakeProjection(fail), connection(), consumer, dead, PARTITION, records)
    assert result == (2, 1)
    assert [sent[0] for sent in dead.sent] == [records[1]]
    assert consumer.commits == [{PARTITION: OffsetAndMetadata(8, "", -1)}]


def test_tombstones_reach_projections_that_accept_them() -> None:
    job = FakeProjection(tombstones=True)
    _ = process_batch(
        job, connection(), FakeConsumer(), FakeDeadLetters(), PARTITION, [record(3, None)]
    )
    assert job.applied[0][0][1] is None


@dataclass
class FakeFuture:
    timeouts: list[float | None] = field(default_factory=list)

    def get(self, timeout: float | None = None) -> object:
        self.timeouts.append(timeout)
        return None


@dataclass
class FakeProducer:
    sent: list[tuple[str, bytes | None, bytes | None, list[tuple[str, bytes]]]] = field(
        default_factory=list
    )
    future: FakeFuture = field(default_factory=FakeFuture)

    def send(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
    ) -> FakeFuture:
        self.sent.append((topic, key, value, list(headers or [])))
        return self.future


def test_dead_letters_keep_the_record_and_describe_the_failure() -> None:
    producer = FakeProducer()
    failed_at = datetime(2026, 3, 5, 6, 0, tzinfo=UTC)
    sink = DeadLetters(producer, "dlq", clock=lambda: failed_at)

    sink.send(record(42, b"payload"), GROUP_ID, "x" * 5000)

    [(topic, key, value, headers)] = producer.sent
    assert (topic, key, value) == ("dlq", b"cell", b"payload")
    assert dict(headers) == {
        "dlq.original.topic": PARTITION.topic.encode(),
        "dlq.original.partition": b"2",
        "dlq.original.offset": b"42",
        "dlq.consumer.group": GROUP_ID.encode(),
        "dlq.error": b"x" * 1000,
        "dlq.failed.at": failed_at.isoformat().encode(),
    }
    assert producer.future.timeouts == [60]


@dataclass
class FakeRecordConsumer:
    """Returns each queued poll result once, then sets ``stop``."""

    stop: StopEvent
    polls: list[dict[TopicPartition, list[ConsumerRecord]]]
    commits: list[Mapping[TopicPartition, OffsetAndMetadata] | None] = field(default_factory=list)
    closed_with_autocommit: list[bool] = field(default_factory=list)

    def poll(self, timeout_ms: int = 0) -> dict[TopicPartition, list[ConsumerRecord]]:
        del timeout_ms
        if len(self.polls) == 1:
            self.stop.set()
        return self.polls.pop(0)

    def commit(
        self,
        offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        del timeout_ms
        self.commits.append(offsets)

    def close(self, *, autocommit: bool = True) -> None:
        self.closed_with_autocommit.append(autocommit)


def test_worker_applies_polled_batches_and_closes_without_autocommit() -> None:
    stop = StopEvent()
    consumer = FakeRecordConsumer(stop, [{PARTITION: [record(1), record(2)]}])
    job = FakeProjection()
    worker = ProjectionWorker(
        job, lambda: consumer, connection, dead_letters=FakeDeadLetters(), stop=stop
    )

    worker.run_until_stopped()

    assert [[item[0].offset for item in batch] for batch in job.applied] == [[1, 2]]
    assert consumer.commits == [{PARTITION: OffsetAndMetadata(3, "", -1)}]
    assert consumer.closed_with_autocommit == [False]


def test_worker_restarts_with_a_fresh_connection_after_a_transient_failure() -> None:
    stop = StopEvent()
    consumer = FakeRecordConsumer(stop, [{}])
    attempts: list[int] = []

    def connect() -> psycopg.Connection:
        attempts.append(len(attempts))
        if len(attempts) == 1:
            raise psycopg.OperationalError("database starting up")
        return connection()

    def open_consumer() -> RecordConsumer:
        return consumer

    worker = ProjectionWorker(
        FakeProjection(), open_consumer, connect, FakeDeadLetters(), stop, restart_delay=0
    )

    worker.run_until_stopped()

    assert len(attempts) == 2
    assert consumer.closed_with_autocommit == [False]
