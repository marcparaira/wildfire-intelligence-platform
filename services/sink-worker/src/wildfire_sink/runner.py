"""Consumer loop shared by projection jobs.

Each job has its own consumer group and database connection, so jobs track their
offsets independently. Per poll and partition the loop:

1. decodes every record; records that can never be processed (bad wire format,
   unregistered schema, failed validation) are sent to the dead-letter topic;
2. applies the valid events in one database transaction;
3. commits the partition offset only after the transaction and dead-letter writes
   succeed.

Transient failures (database, Kafka, or schema registry unavailable) restart the job
from its last committed offset; projections are idempotent, so reprocessing is safe.
If the database rejects a batch for a data reason, events are retried one by one and
only the rejected ones are dead-lettered.
"""

import logging
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import Event
from typing import LiteralString, Protocol, cast

import psycopg
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import OffsetAndMetadata, TopicPartition
from pydantic import BaseModel

from wildfire_events.codec import InvalidEvent
from wildfire_events.topics import DeadLetterHeader

LOGGER = logging.getLogger(__name__)
RESTART_DELAY_SECONDS = 5
POLL_TIMEOUT_MS = 1000
MAX_POLL_RECORDS = 1000
DATABASE_CONNECT_TIMEOUT_SECONDS = 10
DEAD_LETTER_SEND_TIMEOUT_SECONDS = 60
# Keeps the dlq.error header bounded; database errors can be very long.
MAX_DEAD_LETTER_REASON_CHARS = 1000

type Batch[E: BaseModel] = list[tuple[ConsumerRecord, E | None]]


def record_key(record: ConsumerRecord) -> bytes | None:
    """The raw key; consumers are created without deserializers."""
    return cast(bytes | None, record.key)


def record_value(record: ConsumerRecord) -> bytes | None:
    """The raw value (``None`` for a tombstone); consumers have no deserializers."""
    return cast(bytes | None, record.value)


def initialize_schema(connection: psycopg.Connection, statements: Iterable[LiteralString]) -> None:
    """Run idempotent DDL statements in one transaction."""
    with connection.transaction():
        for statement in statements:
            _ = connection.execute(statement)


class EventDecoder[E: BaseModel](Protocol):
    """Decodes a record value or raises ``InvalidEvent``; ``EventCodec`` implements it."""

    def decode(self, value: bytes | None) -> E: ...


class Projection[E: BaseModel](Protocol):
    name: str
    topic: str
    group_id: str
    tombstones: bool

    @property
    def codec(self) -> EventDecoder[E]: ...

    def initialize(self, connection: psycopg.Connection) -> None: ...

    def apply(
        self, connection: psycopg.Connection, events: Sequence[tuple[ConsumerRecord, E | None]]
    ) -> None:
        """Apply decoded events; the event is ``None`` for a tombstone."""
        ...


class OffsetCommitter(Protocol):
    def commit(
        self,
        offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
        timeout_ms: int | None = None,
    ) -> None: ...


class RecordConsumer(OffsetCommitter, Protocol):
    """The part of ``KafkaConsumer`` the worker uses."""

    def poll(self, timeout_ms: int = 0) -> Mapping[TopicPartition, Sequence[ConsumerRecord]]: ...

    def close(self, *, autocommit: bool = True) -> None: ...


class SendFuture(Protocol):
    def get(self, timeout: float | None = None) -> object: ...


class RecordSender(Protocol):
    """The part of ``KafkaProducer`` the dead-letter sink uses."""

    def send(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
    ) -> SendFuture: ...


class DeadLetterSink(Protocol):
    def send(self, record: ConsumerRecord, group_id: str, reason: str) -> None: ...


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class DeadLetters:
    """Copies a failed record to the dead-letter topic with ``dlq.*`` diagnostic headers.

    Blocks until the broker acknowledges the write, so the caller may then commit
    the record's offset.
    """

    producer: RecordSender
    topic: str
    clock: Callable[[], datetime] = field(default=_utc_now)

    def send(self, record: ConsumerRecord, group_id: str, reason: str) -> None:
        headers = [
            *(record.headers or []),
            (DeadLetterHeader.ORIGINAL_TOPIC, record.topic.encode()),
            (DeadLetterHeader.ORIGINAL_PARTITION, str(record.partition).encode()),
            (DeadLetterHeader.ORIGINAL_OFFSET, str(record.offset).encode()),
            (DeadLetterHeader.CONSUMER_GROUP, group_id.encode()),
            (DeadLetterHeader.ERROR, reason[:MAX_DEAD_LETTER_REASON_CHARS].encode()),
            (DeadLetterHeader.FAILED_AT, self.clock().isoformat().encode()),
        ]
        future = self.producer.send(
            self.topic, key=record_key(record), value=record_value(record), headers=headers
        )
        _ = future.get(timeout=DEAD_LETTER_SEND_TIMEOUT_SECONDS)
        LOGGER.warning(
            "Dead-lettered %s[%d]@%d: %s", record.topic, record.partition, record.offset, reason
        )


def process_batch[E: BaseModel](
    projection: Projection[E],
    connection: psycopg.Connection,
    consumer: OffsetCommitter,
    dead_letters: DeadLetterSink,
    partition: TopicPartition,
    records: Sequence[ConsumerRecord],
) -> tuple[int, int]:
    """Project one partition's records and commit; returns (applied, dead-lettered).

    Raises ``psycopg.OperationalError`` (without committing) when the database is
    unavailable, so the caller can restart from the last committed offset.
    """
    valid, invalid = _decode(projection, dead_letters, records)
    try:
        with connection.transaction():
            projection.apply(connection, valid)
        applied, rejected = len(valid), 0
    except psycopg.OperationalError:
        raise
    except psycopg.Error as exc:
        LOGGER.warning(
            "%s: batch rejected by the database (%s); retrying singly", projection.name, exc
        )
        applied, rejected = _apply_singly(projection, connection, dead_letters, valid)

    consumer.commit({partition: OffsetAndMetadata(records[-1].offset + 1, "", -1)})
    return applied, invalid + rejected


def _decode[E: BaseModel](
    projection: Projection[E], dead_letters: DeadLetterSink, records: Sequence[ConsumerRecord]
) -> tuple[Batch[E], int]:
    """Decoded events (``None`` for accepted tombstones) and the dead-lettered count."""
    valid: Batch[E] = []
    rejected = 0
    for record in records:
        value = record_value(record)
        if value is None and projection.tombstones:
            valid.append((record, None))
            continue
        try:
            valid.append((record, projection.codec.decode(value)))
        except InvalidEvent as exc:
            dead_letters.send(record, projection.group_id, str(exc))
            rejected += 1
    return valid, rejected


def _apply_singly[E: BaseModel](
    projection: Projection[E],
    connection: psycopg.Connection,
    dead_letters: DeadLetterSink,
    events: Batch[E],
) -> tuple[int, int]:
    """Apply events one transaction each, dead-lettering those the database rejects."""
    applied = rejected = 0
    for item in events:
        try:
            with connection.transaction():
                projection.apply(connection, [item])
        except psycopg.OperationalError:
            raise
        except psycopg.Error as exc:
            dead_letters.send(item[0], projection.group_id, f"database rejected event: {exc}")
            rejected += 1
        else:
            applied += 1
    return applied, rejected


def kafka_consumer[E: BaseModel](
    bootstrap_servers: str, projection: Projection[E]
) -> KafkaConsumer:
    """A manually committing consumer that starts new groups at the earliest offset."""
    return KafkaConsumer(
        projection.topic,
        bootstrap_servers=bootstrap_servers,
        group_id=projection.group_id,
        client_id=f"sink-worker-{projection.name}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=MAX_POLL_RECORDS,
    )


def database_connection(database_url: str) -> psycopg.Connection:
    return psycopg.connect(
        database_url, autocommit=True, connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS
    )


class ProjectionWorker[E: BaseModel]:
    """Runs one projection until ``stop`` is set, restarting after transient failures.

    ``open_consumer`` and ``connect`` are called on every (re)start, so a restart gets
    a fresh consumer and database connection.
    """

    def __init__(
        self,
        projection: Projection[E],
        open_consumer: Callable[[], RecordConsumer],
        connect: Callable[[], psycopg.Connection],
        dead_letters: DeadLetterSink,
        stop: Event,
        restart_delay: float = RESTART_DELAY_SECONDS,
    ) -> None:
        self.projection: Projection[E] = projection
        self.open_consumer: Callable[[], RecordConsumer] = open_consumer
        self.connect: Callable[[], psycopg.Connection] = connect
        self.dead_letters: DeadLetterSink = dead_letters
        self.stop: Event = stop
        self.restart_delay: float = restart_delay

    def run_until_stopped(self) -> None:
        while not self.stop.is_set():
            try:
                self.run()
            # Supervisor boundary: any failure (database, Kafka, registry) is treated
            # as transient; uncommitted records are reprocessed after the restart.
            except Exception:
                LOGGER.exception(
                    "%s: transient error; restarting from the last committed offset in %ss",
                    self.projection.name,
                    self.restart_delay,
                )
                _ = self.stop.wait(self.restart_delay)

    def run(self) -> None:
        with self.connect() as connection:
            self.projection.initialize(connection)
            consumer = self.open_consumer()
            try:
                LOGGER.info(
                    "%s: consuming %s as %s",
                    self.projection.name,
                    self.projection.topic,
                    self.projection.group_id,
                )
                while not self.stop.is_set():
                    self._poll_once(connection, consumer)
            finally:
                consumer.close(autocommit=False)

    def _poll_once(self, connection: psycopg.Connection, consumer: RecordConsumer) -> None:
        started = time.monotonic()
        applied = rejected = 0
        for partition, records in consumer.poll(timeout_ms=POLL_TIMEOUT_MS).items():
            done, failed = process_batch(
                self.projection, connection, consumer, self.dead_letters, partition, records
            )
            applied += done
            rejected += failed
        if applied or rejected:
            LOGGER.info(
                "%s: applied %d events, dead-lettered %d (%.2fs)",
                self.projection.name,
                applied,
                rejected,
                time.monotonic() - started,
            )
