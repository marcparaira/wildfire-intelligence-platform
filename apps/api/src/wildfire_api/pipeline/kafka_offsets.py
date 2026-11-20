"""Topic throughput, consumer lag, and dead letters from Kafka offsets."""

import logging
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.structs import OffsetAndTimestamp, TopicPartition

from wildfire_api.pipeline.models import (
    ConsumerStatus,
    DeadLetter,
    DeadLetterStatus,
    KafkaSection,
    PartitionLag,
    TopicStatus,
)
from wildfire_events.topics import (
    ALL_TOPICS,
    CONSUMER_GROUPS,
    DEAD_LETTER_HEADER_PREFIX,
    DEAD_LETTER_TOPICS,
    DeadLetterHeader,
)

LOGGER = logging.getLogger(__name__)

HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
DEAD_LETTER_SAMPLE = 5000
RECENT_DEAD_LETTERS = 10
MAX_ERROR_LENGTH = 300
DEAD_LETTER_READ_SECONDS = 5
DEAD_LETTER_POLL_MS = 500
REQUEST_TIMEOUT_MS = 10_000
API_VERSION_TIMEOUT_MS = 5000
UNKNOWN_GROUP_STATE = "Unknown"
UNKNOWN_TOPIC = "unknown"

type Offsets = dict[TopicPartition, int]
type FoundOffsets = dict[TopicPartition, OffsetAndTimestamp | None]


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
        end[tp] - match.offset for tp, match in found.items() if match is not None and tp in end
    )


def consumer_status(
    group_id: str,
    topic: str,
    *,
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


@dataclass(frozen=True, slots=True)
class GroupState:
    state: str
    members: int


def consumer_statuses(
    groups: Mapping[str, Sequence[str]],
    described: Mapping[str, GroupState],
    committed: Mapping[str, Mapping[TopicPartition, int]],
    assignments: Mapping[str, Sequence[TopicPartition]],
    beginning: Offsets,
    end: Offsets,
) -> list[ConsumerStatus]:
    """One row per consumer group and topic it reads, in ``groups`` order.

    A group that reads several topics (the enrichment job) gets one row per topic.
    Negative committed offsets (Kafka's "none") count as not committed.
    """
    rows: list[ConsumerStatus] = []
    for group_id, topics in groups.items():
        group = described.get(group_id, GroupState(UNKNOWN_GROUP_STATE, 0))
        offsets = committed.get(group_id, {})
        for topic in topics:
            tps = assignments.get(topic, [])
            rows.append(
                consumer_status(
                    group_id,
                    topic,
                    state=group.state,
                    members=group.members,
                    committed={
                        tp.partition: offset
                        for tp, offset in offsets.items()
                        if tp.topic == topic and offset >= 0
                    },
                    beginning={tp.partition: beginning[tp] for tp in tps},
                    end={tp.partition: end[tp] for tp in tps},
                )
            )
    return rows


def _int_or_none(value: str | None) -> int | None:
    try:
        return None if value is None else int(value)
    except ValueError:
        return None


def dead_letter(record: HeaderRecord) -> DeadLetter:
    """Where a dead letter came from and why, from its ``dlq.*`` headers."""
    headers = {
        key: value.decode(errors="replace")
        for key, value in record.headers or []
        if key.startswith(DEAD_LETTER_HEADER_PREFIX) and value is not None
    }
    return DeadLetter(
        failed_at=datetime.fromtimestamp(record.timestamp / 1000, UTC),
        topic=headers.get(DeadLetterHeader.ORIGINAL_TOPIC),
        partition=_int_or_none(headers.get(DeadLetterHeader.ORIGINAL_PARTITION)),
        offset=_int_or_none(headers.get(DeadLetterHeader.ORIGINAL_OFFSET)),
        group_id=headers.get(DeadLetterHeader.CONSUMER_GROUP),
        error=headers.get(DeadLetterHeader.ERROR, "")[:MAX_ERROR_LENGTH],
    )


def summarize_dead_letters(
    records: Sequence[HeaderRecord], retained: int, last_day: int
) -> DeadLetterStatus:
    letters = sorted((dead_letter(record) for record in records), key=lambda item: item.failed_at)
    counts = Counter(letter.topic or UNKNOWN_TOPIC for letter in letters)
    return DeadLetterStatus(
        topics=[spec.name for spec in DEAD_LETTER_TOPICS],
        retained=retained,
        last_day=last_day,
        by_topic=dict(counts),
        sample_truncated=last_day > len(letters),
        recent=list(reversed(letters[-RECENT_DEAD_LETTERS:])),
    )


def _epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


class KafkaInspector:
    """Reads offsets, group state, and dead letters through short-lived clients."""

    def __init__(self, consumer: KafkaConsumer, admin: KafkaAdminClient) -> None:
        self.consumer: KafkaConsumer = consumer
        self.admin: KafkaAdminClient = admin

    @classmethod
    def connect(cls, bootstrap_servers: str) -> "KafkaInspector":
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            client_id="api-pipeline-status",
            group_id=None,
            enable_auto_commit=False,
            request_timeout_ms=REQUEST_TIMEOUT_MS,
            api_version_auto_timeout_ms=API_VERSION_TIMEOUT_MS,
        )
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="api-pipeline-status-admin",
                request_timeout_ms=REQUEST_TIMEOUT_MS,
                api_version_auto_timeout_ms=API_VERSION_TIMEOUT_MS,
            )
        except Exception:
            consumer.close()
            raise
        return cls(consumer, admin)

    def close(self) -> None:
        self.admin.close()
        self.consumer.close(autocommit=False)

    def collect(self, now: datetime) -> KafkaSection:
        assignments = {spec.name: self._partitions(spec.name) for spec in ALL_TOPICS}
        everything = [tp for tps in assignments.values() for tp in tps]
        beginning = self.consumer.beginning_offsets(everything)
        end = self.consumer.end_offsets(everything)
        since_hour = self._offsets_since(everything, now - HOUR)
        since_day = self._offsets_since(everything, now - DAY)

        dlq = [tp for spec in DEAD_LETTER_TOPICS for tp in assignments[spec.name]]
        dead_letters = summarize_dead_letters(
            self._recent_dead_letters(dlq, end, since_day),
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
        return KafkaSection(topics, self._consumers(assignments, beginning, end), dead_letters)

    def _partitions(self, topic: str) -> list[TopicPartition]:
        partitions = cast(set[int] | None, self.consumer.partitions_for_topic(topic)) or set()
        return [TopicPartition(topic, partition) for partition in sorted(partitions)]

    def _offsets_since(self, partitions: list[TopicPartition], since: datetime) -> FoundOffsets:
        return self.consumer.offsets_for_times(dict.fromkeys(partitions, _epoch_ms(since)))

    def _consumers(
        self,
        assignments: Mapping[str, list[TopicPartition]],
        beginning: Offsets,
        end: Offsets,
    ) -> list[ConsumerStatus]:
        described = {
            group.group: GroupState(
                state=str(group.state), members=len(cast(list[object], group.members))
            )
            for group in self.admin.describe_consumer_groups(list(CONSUMER_GROUPS))
        }
        committed = {
            group_id: {
                tp: meta.offset
                for tp, meta in self.admin.list_consumer_group_offsets(group_id).items()
            }
            for group_id in CONSUMER_GROUPS
        }
        return consumer_statuses(CONSUMER_GROUPS, described, committed, assignments, beginning, end)

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
        deadline = time.monotonic() + DEAD_LETTER_READ_SECONDS
        while pending and time.monotonic() < deadline:
            polled = self.consumer.poll(
                timeout_ms=DEAD_LETTER_POLL_MS, max_records=DEAD_LETTER_SAMPLE
            )
            for tp, batch in polled.items():
                records.extend(
                    cast(HeaderRecord, record)
                    for record in batch
                    if record.offset < pending.get(tp, 0)
                )
            pending = {
                tp: stop
                for tp, stop in pending.items()
                if cast(int, self.consumer.position(tp)) < stop
            }
        if pending:
            LOGGER.warning("Timed out reading dead letters; the sample is partial")
        return records[-DEAD_LETTER_SAMPLE:]


class KafkaOffsets:
    """Reads the Kafka section; ``read`` raises whatever the Kafka clients raise."""

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers: str = bootstrap_servers

    def read(self, now: datetime) -> KafkaSection:
        inspector = KafkaInspector.connect(self.bootstrap_servers)
        try:
            return inspector.collect(now)
        finally:
            inspector.close()
