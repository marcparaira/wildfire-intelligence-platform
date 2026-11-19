from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from typing import cast

import psycopg
import pytest
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import OffsetAndMetadata, TopicPartition
from pydantic import BaseModel

from wildfire_events.codec import InvalidEvent
from wildfire_sink.runner import process_batch

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
