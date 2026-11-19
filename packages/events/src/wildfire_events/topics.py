"""Kafka topic definitions.

All spatially keyed source topics use the same partition count so that events with
the same H3 key land on the same partition number in every topic (co-partitioning),
which later stream-processing joins rely on. Changing PARTITIONS after topics exist
requires recreating them and replaying, because the key-to-partition mapping changes.
"""

from dataclasses import dataclass, field

PARTITIONS = 6
DAY_MS = 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int = PARTITIONS
    config: dict[str, str] = field(default_factory=dict)


FIRMS_DETECTIONS = TopicSpec(
    "firms.detections",
    # The log is the system of record: keep observations indefinitely.
    config={"cleanup.policy": "delete", "retention.ms": "-1"},
)
WEATHER_FORECASTS = TopicSpec(
    "weather.forecasts",
    # Latest forecast per H3 cell. Small segments let the log cleaner compact
    # superseded forecasts without waiting for the default one-week roll.
    config={
        "cleanup.policy": "compact",
        "segment.ms": str(DAY_MS),
        "min.cleanable.dirty.ratio": "0.1",
    },
)
SINK_WORKER_DLQ = TopicSpec(
    "sink-worker.dlq",
    partitions=1,
    config={"cleanup.policy": "delete", "retention.ms": str(30 * DAY_MS)},
)

SOURCE_TOPICS = (FIRMS_DETECTIONS, WEATHER_FORECASTS)
ALL_TOPICS = (*SOURCE_TOPICS, SINK_WORKER_DLQ)

# Consumer groups that project topics into PostGIS, with the topic each one reads.
SINK_DETECTIONS_GROUP = "sink-worker.postgis.detections"
SINK_WEATHER_GROUP = "sink-worker.postgis.weather"
CONSUMER_GROUPS: dict[str, str] = {
    SINK_DETECTIONS_GROUP: FIRMS_DETECTIONS.name,
    SINK_WEATHER_GROUP: WEATHER_FORECASTS.name,
}


def value_subject(topic: str) -> str:
    """Schema-registry subject for a topic's values (TopicNameStrategy)."""
    return f"{topic}-value"
