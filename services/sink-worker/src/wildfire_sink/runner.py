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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Event
from typing import Protocol, cast

import psycopg
from kafka import KafkaConsumer, KafkaProducer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import OffsetAndMetadata, TopicPartition
from pydantic import BaseModel

from wildfire_events.codec import InvalidEvent

LOGGER = logging.getLogger(__name__)
RESTART_DELAY_SECONDS = 5


def record_key(record: ConsumerRecord) -> bytes | None:
    """The raw key; consumers are created without deserializers."""
    return cast(bytes | None, record.key)


def record_value(record: ConsumerRecord) -> bytes | None:
    """The raw value (``None`` for a tombstone); consumers have no deserializers."""
    return cast(bytes | None, record.value)


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


class DeadLetterSink(Protocol):
    def send(self, record: ConsumerRecord, group_id: str, reason: str) -> None: ...


@dataclass
class DeadLetters:
    producer: KafkaProducer
    topic: str

    def send(self, record: ConsumerRecord, group_id: str, reason: str) -> None:
        headers = [
            *(record.headers or []),
            ("dlq.original.topic", record.topic.encode()),
            ("dlq.original.partition", str(record.partition).encode()),
            ("dlq.original.offset", str(record.offset).encode()),
            ("dlq.consumer.group", group_id.encode()),
            ("dlq.error", reason[:1000].encode()),
            ("dlq.failed.at", datetime.now(UTC).isoformat().encode()),
        ]
        future = self.producer.send(
            self.topic, key=record_key(record), value=record_value(record), headers=headers
        )
        future.get(timeout=60)  # pyright: ignore[reportUnknownMemberType] -- untyped stub
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
    """Project one partition's records and commit; returns (applied, dead-lettered)."""
    valid: list[tuple[ConsumerRecord, E | None]] = []
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

    try:
        with connection.transaction():
            projection.apply(connection, valid)
    except psycopg.OperationalError:
        raise
    except psycopg.Error as exc:
        LOGGER.warning("%s: batch rejected by the database (%s); retrying singly",
                       projection.name, exc)
        applied: list[tuple[ConsumerRecord, E | None]] = []
        for item in valid:
            try:
                with connection.transaction():
                    projection.apply(connection, [item])
                applied.append(item)
            except psycopg.OperationalError:
                raise
            except psycopg.Error as single:
                reason = f"database rejected event: {single}"
                dead_letters.send(item[0], projection.group_id, reason)
                rejected += 1
        valid = applied

    consumer.commit({partition: OffsetAndMetadata(records[-1].offset + 1, "", -1)})
    return len(valid), rejected


class ProjectionWorker[E: BaseModel]:
    def __init__(
        self,
        projection: Projection[E],
        bootstrap_servers: str,
        database_url: str,
        dead_letters: DeadLetterSink,
        stop: Event,
    ) -> None:
        self.projection: Projection[E] = projection
        self.bootstrap_servers: str = bootstrap_servers
        self.database_url: str = database_url
        self.dead_letters: DeadLetterSink = dead_letters
        self.stop: Event = stop

    def _consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.projection.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.projection.group_id,
            client_id=f"sink-worker-{self.projection.name}",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=1000,
        )

    def run_until_stopped(self) -> None:
        while not self.stop.is_set():
            try:
                self.run()
            except Exception:
                LOGGER.exception(
                    "%s: stopped by a transient error; restarting from the last committed offset "
                    + "in %ds",
                    self.projection.name,
                    RESTART_DELAY_SECONDS,
                )
                _ = self.stop.wait(RESTART_DELAY_SECONDS)

    def run(self) -> None:
        with psycopg.connect(self.database_url, autocommit=True, connect_timeout=10) as connection:
            self.projection.initialize(connection)
            consumer = self._consumer()
            try:
                LOGGER.info("%s: consuming %s as %s", self.projection.name,
                            self.projection.topic, self.projection.group_id)
                while not self.stop.is_set():
                    started = time.monotonic()
                    applied = rejected = 0
                    for partition, records in consumer.poll(timeout_ms=1000).items():
                        done, failed = process_batch(
                            self.projection,
                            connection,
                            consumer,
                            self.dead_letters,
                            partition,
                            records,
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
            finally:
                consumer.close(autocommit=False)
