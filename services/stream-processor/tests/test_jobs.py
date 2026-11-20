"""End-to-end tests of the enrichment and statistics jobs against Kafka and the registry."""

import os
import time
import uuid
from collections.abc import Iterator, Sequence
from datetime import timedelta
from http.client import HTTPResponse
from pathlib import Path
from typing import cast
from urllib.request import urlopen

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.structs import TopicPartition
from pydantic import BaseModel, TypeAdapter

from wildfire_events.cells import partition_cell
from wildfire_events.codec import EventCodec
from wildfire_events.countries import Country
from wildfire_events.models import (
    EnrichedDetection,
    FirmsDetection,
    HourlyDetectionStats,
    RiskLevel,
    WeatherForecast,
)
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import TopicSpec, value_subject
from wildfire_processor.enrichment_job import EnrichmentTopics, build_enrichment_application
from wildfire_processor.settings import Settings
from wildfire_processor.statistics_job import StatisticsTopics, build_statistics_application

from .test_enrichment import forecast
from .test_incidents import CENTER, T0, detection
from .test_service import delete_subject

KAFKA = os.environ.get("PROCESSOR_TEST_KAFKA", "")
REGISTRY = os.environ.get("PROCESSOR_TEST_SCHEMA_REGISTRY", "")
RUN_TIMEOUT_SECONDS = 90
READ_TIMEOUT_MS = 10_000

pytestmark = pytest.mark.skipif(
    not (KAFKA and REGISTRY),
    reason="set PROCESSOR_TEST_KAFKA and PROCESSOR_TEST_SCHEMA_REGISTRY to run",
)


@pytest.fixture
def suffix() -> Iterator[str]:
    """A unique suffix; topics, groups, and subjects containing it are removed afterwards."""
    value = uuid.uuid4().hex[:8]
    yield value
    admin = KafkaAdminClient(bootstrap_servers=KAFKA)
    try:
        created = [name for name in admin.list_topics() if value in name]
        admin.delete_topics(created)  # pyright: ignore[reportUnknownMemberType] -- untyped in kafka-python
        groups = [group for group, _ in admin.list_consumer_groups() if value in group]
        _ = admin.delete_consumer_groups(groups)
    finally:
        admin.close()
    for subject in registry_subjects():
        if value in subject:
            delete_subject(REGISTRY, subject)


def registry_subjects() -> list[str]:
    with cast(HTTPResponse, urlopen(f"{REGISTRY}/subjects", timeout=10)) as response:
        return TypeAdapter(list[str]).validate_json(response.read())


def settings(suffix: str, tmp_path: Path, job: str) -> Settings:
    return Settings(
        kafka=KAFKA,
        registry_url=REGISTRY,
        state_dir=str(tmp_path / "state"),
        consumer_group=f"processor-test-{job}-{suffix}",
    )


def produce(records: Sequence[tuple[str, bytes | None, bytes | None, int]]) -> None:
    producer = KafkaProducer(bootstrap_servers=KAFKA)
    try:
        for topic, key, value, timestamp_ms in records:
            _ = producer.send(topic, key=key, value=value, timestamp_ms=timestamp_ms)
        producer.flush()
    finally:
        producer.close()


def read[T: BaseModel](topic: str, codec: EventCodec[T], count: int) -> list[tuple[str, T]]:
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA,
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=READ_TIMEOUT_MS,
        isolation_level="read_committed",
    )
    consumer.subscribe([topic])
    records: list[tuple[str, T]] = []
    for record in consumer:
        key = cast(bytes, record.key).decode()
        records.append((key, codec.decode(cast(bytes | None, record.value))))
        if len(records) == count:
            break
    consumer.close()
    return records


def record_count(topic: str) -> int:
    consumer = KafkaConsumer(bootstrap_servers=KAFKA, group_id=None)
    try:
        partitions = [
            TopicPartition(topic, partition)
            for partition in consumer.partitions_for_topic(topic) or set()
        ]
        return sum(consumer.end_offsets(partitions).values())
    finally:
        consumer.close()


def test_enrichment_joins_detections_with_the_latest_forecast_for_their_cell(
    suffix: str, tmp_path: Path
) -> None:
    topics = EnrichmentTopics(
        detections=TopicSpec(f"processor-test-detections-{suffix}"),
        forecasts=TopicSpec(f"processor-test-forecasts-{suffix}"),
        enriched=TopicSpec(f"processor-test-enriched-{suffix}"),
        dead_letters=TopicSpec(f"processor-test-dlq-{suffix}", partitions=1),
    )
    registry = SchemaRegistry(REGISTRY)
    app = build_enrichment_application(settings(suffix, tmp_path, "enrichment"), registry, topics)
    detection_codec = EventCodec(FirmsDetection, value_subject(topics.detections.name), registry)
    forecast_codec = EventCodec(WeatherForecast, value_subject(topics.forecasts.name), registry)

    covered = detection(CENTER, 0, frp=60.0)
    # Another resolution-2 cell, in Thailand.
    uncovered = detection(CENTER, 0.1).model_copy(
        update={
            "latitude": 13.756,
            "longitude": 100.502,
            "h3_cell": partition_cell(13.756, 100.502),
        }
    )
    update = forecast(T0 - timedelta(hours=2), start=T0 - timedelta(hours=30))
    assert update.forecast is not None
    # Bangkok's cell gets a forecast that a tombstone then removes.
    removed = update.forecast.model_copy(update={"cell": uncovered.h3_cell})
    now_ms = int(time.time() * 1000)
    produce(
        [
            (
                topics.forecasts.name,
                update.cell.encode(),
                forecast_codec.encode(update.forecast),
                now_ms - 60_000,
            ),
            (
                topics.forecasts.name,
                uncovered.h3_cell.encode(),
                forecast_codec.encode(removed),
                now_ms - 60_000,
            ),
            (topics.forecasts.name, uncovered.h3_cell.encode(), None, now_ms - 30_000),
            (
                topics.detections.name,
                covered.h3_cell.encode(),
                detection_codec.encode(covered),
                now_ms,
            ),
            (
                topics.detections.name,
                uncovered.h3_cell.encode(),
                detection_codec.encode(uncovered),
                now_ms,
            ),
        ]
    )

    outputs = app.run(count=2, timeout=RUN_TIMEOUT_SECONDS)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] -- partially typed in Quix
    assert len(outputs) == 2  # pyright: ignore[reportUnknownArgumentType] -- see app.run

    codec = EventCodec(EnrichedDetection, value_subject(topics.enriched.name), registry)
    enriched = {
        event.detection.event_id: (key, event)
        for key, event in read(topics.enriched.name, codec, 2)
    }
    key, joined = enriched[covered.event_id]
    assert key == covered.h3_cell
    assert joined.country is Country.IDN
    assert joined.weather is not None
    assert joined.weather.valid_at == T0
    assert joined.risk is not None
    assert joined.risk.level is RiskLevel.HIGH
    key, alone = enriched[uncovered.event_id]
    assert key == uncovered.h3_cell
    assert alone.country is Country.THA
    assert (alone.weather, alone.risk) == (None, None)
    assert record_count(topics.dead_letters.name) == 0  # the tombstone was applied, not rejected


def test_statistics_count_detections_per_country_and_observation_hour(
    suffix: str, tmp_path: Path
) -> None:
    topics = StatisticsTopics(
        enriched=TopicSpec(f"processor-test-enriched-{suffix}"),
        stats=TopicSpec(f"processor-test-stats-{suffix}"),
        dead_letters=TopicSpec(f"processor-test-dlq-{suffix}", partitions=1),
    )
    registry = SchemaRegistry(REGISTRY)
    app = build_statistics_application(settings(suffix, tmp_path, "statistics"), registry, topics)
    codec = EventCodec(EnrichedDetection, value_subject(topics.enriched.name), registry)

    def enriched(hours: float, frp: float | None, country: Country | None) -> EnrichedDetection:
        return EnrichedDetection(
            detection=detection(CENTER, hours, frp=frp), country=country, weather=None, risk=None
        )

    events = [
        enriched(0.1, 10.0, Country.IDN),
        enriched(0.5, None, Country.IDN),
        enriched(0.9, 30.5, Country.IDN),
        enriched(1.2, 5.0, Country.IDN),
        enriched(1.4, 7.0, None),  # no country: not counted
        enriched(1.7, None, Country.IDN),
    ]
    now_ms = int(time.time() * 1000)
    produce(
        [
            (topics.enriched.name, event.detection.h3_cell.encode(), codec.encode(event), now_ms)
            for event in events
        ]
        + [(topics.enriched.name, b"cell", b"junk", now_ms)]
    )

    outputs = app.run(count=5, timeout=RUN_TIMEOUT_SECONDS)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] -- partially typed in Quix
    assert len(outputs) == 5  # pyright: ignore[reportUnknownArgumentType] -- see app.run

    stats_codec = EventCodec(HourlyDetectionStats, value_subject(topics.stats.name), registry)
    records = read(topics.stats.name, stats_codec, 5)
    assert all(key == stats.key for key, stats in records)
    final = {stats.hour_start: stats for _, stats in records}
    first, second = final[T0], final[T0 + timedelta(hours=1)]
    assert (first.detections, first.detections_with_frp) == (3, 2)
    assert (first.total_frp_mw, first.max_frp_mw) == (40.5, 30.5)
    assert (second.detections, second.detections_with_frp) == (2, 1)
    assert (second.total_frp_mw, second.max_frp_mw) == (5.0, 5.0)
    assert {stats.country for stats in final.values()} == {Country.IDN}
