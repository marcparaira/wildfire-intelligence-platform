"""Statistics job: hourly per-country detection and FRP totals from ``detections.enriched``.

Pipeline::

    detections.enriched ─ decode (invalid → stream-processor.dlq) ─ drop no-country
        ─ timestamp := observation time ─ group by country (repartition topic)
        ─ 1-hour tumbling window, 3-day grace ─ every update ─ stats.hourly

See ``wildfire_processor.statistics`` for the window rules.
"""

from dataclasses import dataclass
from typing import cast

from quixstreams import Application
from quixstreams.dataframe.windows import Count, Max, Sum

from wildfire_events.codec import EventCodec
from wildfire_events.kafka import ensure_topics
from wildfire_events.models import EnrichedDetection, HourlyDetectionStats
from wildfire_events.registry import SubjectRegistry
from wildfire_events.topics import (
    DETECTIONS_ENRICHED,
    STATS_HOURLY,
    STREAM_PROCESSOR_DLQ,
    TopicSpec,
    value_subject,
)
from wildfire_processor.adapters import (
    DeadLetters,
    RecordDecoder,
    RecordProducer,
    is_present,
    repartition_topic,
)
from wildfire_processor.quix import application, output_topic
from wildfire_processor.settings import Settings
from wildfire_processor.statistics import GRACE, WINDOW, hourly_stats, stats_input

REPARTITION_NAME = "country"


@dataclass(frozen=True, slots=True)
class StatisticsTopics:
    """Topic definitions; tests substitute their own."""

    enriched: TopicSpec = DETECTIONS_ENRICHED
    stats: TopicSpec = STATS_HOURLY
    dead_letters: TopicSpec = STREAM_PROCESSOR_DLQ


def window_input(enriched: EnrichedDetection) -> dict[str, object] | None:
    """JSON-ready window input (the repartition topic carries it), or ``None``."""
    row = stats_input(enriched)
    return None if row is None else row.model_dump(mode="json")


def observation_time(
    value: dict[str, object], _key: object, _timestamp: int, _headers: object
) -> int:
    return cast(int, value["observed_ms"])


def country_of(value: dict[str, object]) -> str:
    return cast(str, value["country"])


def to_stats(
    window: dict[str, object], key: str, _timestamp: int, _headers: object
) -> HourlyDetectionStats:
    return hourly_stats(key, window)


def stats_key(stats: HourlyDetectionStats) -> str:
    return stats.key


def build_statistics_application(
    settings: Settings, registry: SubjectRegistry, topics: StatisticsTopics | None = None
) -> Application:
    """Create the topics and the Quix application with its pipeline.

    Side effects: creates missing topics (including the repartition topic) and
    registers the ``HourlyDetectionStats`` schema, so an incompatible schema fails here.
    """
    topics = topics or StatisticsTopics()
    ensure_topics(
        settings.kafka,
        [
            topics.enriched,
            topics.stats,
            topics.dead_letters,
            repartition_topic(settings.consumer_group, topics.enriched.name, REPARTITION_NAME),
        ],
    )
    app = application(settings)
    stats = output_topic(
        app,
        topics.stats,
        EventCodec(HourlyDetectionStats, value_subject(topics.stats.name), registry),
    )
    decoder = RecordDecoder(
        EventCodec(EnrichedDetection, value_subject(topics.enriched.name), registry),
        DeadLetters(
            cast(RecordProducer, app.get_producer()),
            topic=topics.dead_letters.name,
            group_id=settings.consumer_group,
        ),
    )
    sdf = app.dataframe(app.topic(topics.enriched.name, value_deserializer="bytes"))
    sdf = sdf.apply(decoder, metadata=True).filter(is_present)  # pyright: ignore[reportUnknownMemberType] -- partially typed in Quix
    sdf = sdf.apply(window_input).filter(is_present)  # pyright: ignore[reportUnknownMemberType] -- partially typed in Quix
    sdf = sdf.set_timestamp(observation_time)
    sdf = sdf.group_by(country_of, name=REPARTITION_NAME)
    windows = sdf.tumbling_window(WINDOW, grace_ms=GRACE).agg(  # pyright: ignore[reportUnknownMemberType] -- partially typed in Quix
        detections=Count(),
        detections_with_frp=Count("frp_mw"),
        total_frp_mw=Sum("frp_mw"),
        max_frp_mw=Max("frp_mw"),
    )
    sdf = windows.current()
    sdf = sdf.apply(to_stats, metadata=True)  # pyright: ignore[reportUnknownMemberType] -- partially typed in Quix
    _ = sdf.to_topic(stats, key=stats_key)
    return app
