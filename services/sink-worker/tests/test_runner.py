from contextlib import nullcontext
from unittest.mock import MagicMock, Mock

import psycopg
import pytest
from kafka.structs import OffsetAndMetadata, TopicPartition

from wildfire_events.codec import InvalidEvent
from wildfire_sink.runner import DeadLetters, process_batch

PARTITION = TopicPartition("firms.detections", 2)


def record(offset: int, value: bytes | None = b"good") -> Mock:
    return Mock(
        topic="firms.detections", partition=2, offset=offset, key=b"cell", value=value, headers=[]
    )


def projection(apply: Mock | None = None, tombstones: bool = False) -> Mock:
    def decode(value: bytes) -> str:
        if value != b"good":
            raise InvalidEvent("bad record")
        return "event"

    return Mock(
        group_id="sink-worker.test",
        tombstones=tombstones,
        codec=Mock(decode=Mock(side_effect=decode)),
        apply=apply or Mock(),
    )


def connection() -> MagicMock:
    db = MagicMock()
    db.transaction.side_effect = lambda: nullcontext()
    return db


def test_invalid_records_are_dead_lettered_and_offsets_committed() -> None:
    consumer, dead = Mock(), Mock(spec=DeadLetters)
    job = projection()
    records = [record(10), record(11, b"bad"), record(12)]

    assert process_batch(job, connection(), consumer, dead, PARTITION, records) == (2, 1)
    dead.send.assert_called_once_with(records[1], "sink-worker.test", "bad record")
    applied = job.apply.call_args.args[1]
    assert [item[0].offset for item in applied] == [10, 12]
    consumer.commit.assert_called_once_with({PARTITION: OffsetAndMetadata(13, "", -1)})


def test_transient_database_errors_never_commit() -> None:
    consumer, dead = Mock(), Mock(spec=DeadLetters)
    job = projection(Mock(side_effect=psycopg.OperationalError("connection lost")))
    with pytest.raises(psycopg.OperationalError):
        process_batch(job, connection(), consumer, dead, PARTITION, [record(1)])
    consumer.commit.assert_not_called()
    dead.send.assert_not_called()


def test_rows_rejected_by_the_database_are_isolated_and_dead_lettered() -> None:
    def apply(_connection: object, events: list) -> None:
        if len(events) > 1 or events[0][0].offset == 6:
            raise psycopg.errors.CheckViolation("latitude out of range")

    consumer, dead = Mock(), Mock(spec=DeadLetters)
    records = [record(5), record(6), record(7)]
    result = process_batch(projection(Mock(side_effect=apply)), connection(), consumer, dead,
                           PARTITION, records)
    assert result == (2, 1)
    assert dead.send.call_args.args[0] is records[1]
    consumer.commit.assert_called_once_with({PARTITION: OffsetAndMetadata(8, "", -1)})


def test_tombstones_reach_projections_that_accept_them() -> None:
    job = projection(tombstones=True)
    process_batch(job, connection(), Mock(), Mock(spec=DeadLetters), PARTITION,
                  [record(3, None)])
    assert job.apply.call_args.args[1][0][1] is None
