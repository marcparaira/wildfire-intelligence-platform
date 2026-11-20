"""Adapters between the processor's domain logic and Quix Streams / Kafka."""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, override

from pydantic import BaseModel
from quixstreams import State, message_context
from quixstreams.models.serializers import SerializationContext, Serializer

from wildfire_events.codec import EventCodec, InvalidEvent
from wildfire_events.topics import (
    DAY_MS,
    INCIDENTS_GROUP,
    STREAM_PROCESSOR_DLQ,
    CleanupPolicy,
    DeadLetterHeader,
    TopicSpec,
)

LOGGER = logging.getLogger(__name__)
# Repartitioned records only need to live until the processor reads them back.
REPARTITION_RETENTION_MS = 3 * DAY_MS
DEAD_LETTER_FLUSH_SECONDS = 60.0
# Keeps the dlq.error header bounded.
MAX_DEAD_LETTER_REASON_CHARS = 1000

Headers = Sequence[tuple[str, bytes]]


def repartition_topic(consumer_group: str, source: str, name: str) -> TopicSpec:
    """The internal topic Quix uses for a ``group_by``; created first to cap retention.

    Quix would otherwise copy the source topic's (unlimited) retention.
    """
    return TopicSpec(
        f"repartition__{consumer_group}--{source}--{name}",
        config={
            "cleanup.policy": CleanupPolicy.DELETE,
            "retention.ms": str(REPARTITION_RETENTION_MS),
        },
    )


class EventDecoder[T: BaseModel](Protocol):
    """Decodes a record value or raises ``InvalidEvent``; ``EventCodec`` implements it."""

    def decode(self, value: bytes | None) -> T: ...


class RecordProducer(Protocol):
    """The part of the Quix producer used for dead letters."""

    def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        headers: Headers | None = None,
    ) -> None: ...

    def flush(self, timeout: float | None = None) -> int: ...


@dataclass(frozen=True, slots=True)
class DeadLetters:
    """Writes undecodable records to the dead-letter topic with their origin in headers.

    ``send`` flushes synchronously so the record is durable before the source offset
    commits; it raises ``RuntimeError`` if the flush times out.
    """

    producer: RecordProducer
    topic: str = STREAM_PROCESSOR_DLQ.name
    group_id: str = INCIDENTS_GROUP

    def send(
        self,
        source: tuple[str, int, int],
        key: bytes | None,
        value: bytes | None,
        headers: Headers | None,
        reason: str,
    ) -> None:
        topic, partition, offset = source
        self.producer.produce(
            self.topic,
            value=value,
            key=key,
            headers=[
                *(headers or []),
                (DeadLetterHeader.ORIGINAL_TOPIC, topic.encode()),
                (DeadLetterHeader.ORIGINAL_PARTITION, str(partition).encode()),
                (DeadLetterHeader.ORIGINAL_OFFSET, str(offset).encode()),
                (DeadLetterHeader.CONSUMER_GROUP, self.group_id.encode()),
                (DeadLetterHeader.ERROR, reason[:MAX_DEAD_LETTER_REASON_CHARS].encode()),
                (DeadLetterHeader.FAILED_AT, datetime.now(UTC).isoformat().encode()),
            ],
        )
        if self.producer.flush(timeout=DEAD_LETTER_FLUSH_SECONDS):
            raise RuntimeError(f"timed out writing a dead letter to {self.topic}")
        LOGGER.warning("Dead-lettered %s[%d]@%d: %s", topic, partition, offset, reason)

    def send_current(
        self, key: bytes | None, value: bytes | None, headers: Headers | None, reason: str
    ) -> None:
        """Dead-letter the record Quix is processing now."""
        context = message_context()
        self.send((context.topic, context.partition, context.offset), key, value, headers, reason)


@dataclass(frozen=True, slots=True)
class RecordDecoder[T: BaseModel]:
    """Quix ``apply(metadata=True)`` step: decodes a record, dead-letters invalid ones.

    Returns ``None`` for an invalid record; filter those out after this step.
    """

    codec: EventDecoder[T]
    dead_letters: DeadLetters

    def __call__(
        self, value: bytes | None, key: bytes | None, _timestamp: int, headers: Headers | None
    ) -> T | None:
        try:
            return self.codec.decode(value)
        except InvalidEvent as exc:
            self.dead_letters.send_current(key, value, headers, str(exc))
            return None


def is_present(value: object) -> bool:
    return value is not None


class CodecSerializer[T: BaseModel](Serializer):
    """Encodes event values in the schema-registry wire format."""

    def __init__(self, codec: EventCodec[T]) -> None:
        super().__init__()
        self.codec: EventCodec[T] = codec

    @override
    def __call__(self, value: T, ctx: SerializationContext) -> bytes:
        return self.codec.encode(value)


@dataclass(frozen=True, slots=True)
class QuixState:
    """Adapts Quix Streams' ``State`` to the domain's ``KeyValueState``."""

    state: State[str, object]

    def get(self, key: str, default: object = None) -> object:
        return self.state.get(key, default)

    def set(self, key: str, value: object) -> None:
        self.state.set(key, value)

    def delete(self, key: str) -> None:
        self.state.delete(key)
