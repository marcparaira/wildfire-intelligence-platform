"""Kafka topic definitions.

All spatially keyed source topics use the same partition count so that events with
the same H3 key land on the same partition number in every topic (co-partitioning),
which later stream-processing joins rely on. Changing PARTITIONS after topics exist
requires recreating them and replaying, because the key-to-partition mapping changes.
"""

from dataclasses import dataclass, field
from enum import StrEnum, auto

PARTITIONS = 6
DAY_MS = 24 * 60 * 60 * 1000
RETAIN_FOREVER = "-1"
DEAD_LETTER_RETENTION_MS = 30 * DAY_MS


class CleanupPolicy(StrEnum):
    """Values of the Kafka ``cleanup.policy`` topic configuration."""

    DELETE = auto()
    COMPACT = auto()


class DeadLetterHeader(StrEnum):
    """Headers on records in the dead-letter topics; values are the header names."""

    ORIGINAL_TOPIC = "dlq.original.topic"
    ORIGINAL_PARTITION = "dlq.original.partition"
    ORIGINAL_OFFSET = "dlq.original.offset"
    CONSUMER_GROUP = "dlq.consumer.group"
    ERROR = "dlq.error"
    FAILED_AT = "dlq.failed.at"


DEAD_LETTER_HEADER_PREFIX = "dlq."


@dataclass(frozen=True, slots=True)
class TopicSpec:
    name: str
    partitions: int = PARTITIONS
    config: dict[str, str] = field(default_factory=dict)


def _retained(retention_ms: str) -> dict[str, str]:
    return {"cleanup.policy": CleanupPolicy.DELETE, "retention.ms": retention_ms}


def _compacted() -> dict[str, str]:
    """Latest value per key. Small segments let the log cleaner compact superseded
    values without waiting for the default one-week segment roll."""
    return {
        "cleanup.policy": CleanupPolicy.COMPACT,
        "segment.ms": str(DAY_MS),
        "min.cleanable.dirty.ratio": "0.1",
    }


FIRMS_DETECTIONS = TopicSpec(
    "firms.detections",
    # The log is the system of record: keep observations indefinitely.
    config=_retained(RETAIN_FOREVER),
)
# Latest forecast per H3 cell.
WEATHER_FORECASTS = TopicSpec("weather.forecasts", config=_compacted())
SINK_WORKER_DLQ = TopicSpec(
    "sink-worker.dlq", partitions=1, config=_retained(str(DEAD_LETTER_RETENTION_MS))
)
# Derived from firms.detections. Keyed by incident ID; compaction keeps the
# latest revision of each incident, which carries its daily growth history.
FIRE_INCIDENTS = TopicSpec("fire.incidents", config=_compacted())
# Derived from firms.detections and weather.forecasts, keyed like the detection.
# Kept indefinitely like its source, so the PostGIS projection can be rebuilt.
DETECTIONS_ENRICHED = TopicSpec("detections.enriched", config=_retained(RETAIN_FOREVER))
# Derived from detections.enriched. Keyed by "<country>|<hour start>"; compaction
# keeps the final count of each country and hour.
STATS_HOURLY = TopicSpec("stats.hourly", config=_compacted())
STREAM_PROCESSOR_DLQ = TopicSpec(
    "stream-processor.dlq", partitions=1, config=_retained(str(DEAD_LETTER_RETENTION_MS))
)

SOURCE_TOPICS = (FIRMS_DETECTIONS, WEATHER_FORECASTS)
DERIVED_TOPICS = (FIRE_INCIDENTS, DETECTIONS_ENRICHED, STATS_HOURLY)
DEAD_LETTER_TOPICS = (SINK_WORKER_DLQ, STREAM_PROCESSOR_DLQ)
ALL_TOPICS = (*SOURCE_TOPICS, *DERIVED_TOPICS, *DEAD_LETTER_TOPICS)

# Consumer groups, with the topics each one reads.
SINK_DETECTIONS_GROUP = "sink-worker.postgis.detections"
SINK_WEATHER_GROUP = "sink-worker.postgis.weather"
SINK_INCIDENTS_GROUP = "sink-worker.postgis.incidents"
SINK_ENRICHMENT_GROUP = "sink-worker.postgis.enrichment"
SINK_STATISTICS_GROUP = "sink-worker.postgis.statistics"
INCIDENTS_GROUP = "stream-processor.incidents"
ENRICHMENT_GROUP = "stream-processor.enrichment"
STATISTICS_GROUP = "stream-processor.statistics"
CONSUMER_GROUPS: dict[str, tuple[str, ...]] = {
    SINK_DETECTIONS_GROUP: (FIRMS_DETECTIONS.name,),
    SINK_WEATHER_GROUP: (WEATHER_FORECASTS.name,),
    SINK_INCIDENTS_GROUP: (FIRE_INCIDENTS.name,),
    SINK_ENRICHMENT_GROUP: (DETECTIONS_ENRICHED.name,),
    SINK_STATISTICS_GROUP: (STATS_HOURLY.name,),
    INCIDENTS_GROUP: (FIRMS_DETECTIONS.name,),
    ENRICHMENT_GROUP: (FIRMS_DETECTIONS.name, WEATHER_FORECASTS.name),
    STATISTICS_GROUP: (DETECTIONS_ENRICHED.name,),
}
SINK_WORKER_GROUPS = (
    SINK_DETECTIONS_GROUP,
    SINK_WEATHER_GROUP,
    SINK_INCIDENTS_GROUP,
    SINK_ENRICHMENT_GROUP,
    SINK_STATISTICS_GROUP,
)
STREAM_PROCESSOR_GROUPS = (INCIDENTS_GROUP, ENRICHMENT_GROUP, STATISTICS_GROUP)


def value_subject(topic: str) -> str:
    """Schema-registry subject for a topic's values (TopicNameStrategy)."""
    return f"{topic}-value"
