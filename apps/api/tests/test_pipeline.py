import os
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
import psycopg
import pytest
from fastapi.testclient import TestClient
from kafka.structs import OffsetAndTimestamp, TopicPartition
from psycopg import sql
from psycopg.conninfo import make_conninfo

from wildfire_api.main import app
from wildfire_api.pipeline.kafka_offsets import (
    GroupState,
    consumer_status,
    consumer_statuses,
    messages_since,
    summarize_dead_letters,
)
from wildfire_api.pipeline.models import (
    Freshness,
    KafkaSection,
    PipelineStatus,
    SourceKind,
    SourceSpec,
    SourceStatus,
)
from wildfire_api.pipeline.routes import status_cache, status_collector
from wildfire_api.pipeline.sources import (
    DetectionStats,
    ForecastStats,
    PostgisSources,
    source_statuses,
)
from wildfire_api.pipeline.status import (
    KAFKA_UNAVAILABLE,
    POSTGIS_UNAVAILABLE,
    PipelineStatusCollector,
    StatusCache,
)
from wildfire_events.topics import ALL_TOPICS, CONSUMER_GROUPS, ENRICHMENT_GROUP

client: httpx.Client = TestClient(app)
NOW = datetime(2026, 3, 5, 12, 0, tzinfo=UTC)
EMPTY_STATUS = PipelineStatus(
    checked_at=NOW, sources=[], topics=None, consumers=None, dead_letters=None, errors=[]
)


@pytest.fixture(autouse=True)
def isolated_app() -> Iterator[None]:
    status_cache().clear()
    yield
    status_cache().clear()
    app.dependency_overrides.clear()


@pytest.mark.parametrize(
    ("age", "expected"),
    [
        (None, Freshness.NO_DATA),
        (timedelta(hours=1), Freshness.FRESH),
        (timedelta(hours=18), Freshness.FRESH),
        (timedelta(hours=19), Freshness.DELAYED),
        (timedelta(hours=36), Freshness.DELAYED),
        (timedelta(hours=37), Freshness.STALE),
    ],
)
def test_classify(age: timedelta | None, expected: Freshness) -> None:
    spec = SourceSpec(
        "s", "S", SourceKind.OBSERVATION, "t", timedelta(hours=18), timedelta(hours=36)
    )
    latest = None if age is None else NOW - age
    assert spec.classify(latest, NOW) == expected


def test_source_statuses_cover_every_source() -> None:
    detections = [
        DetectionStats(
            product="VIIRS_NOAA20_NRT",
            latest_observed_at=NOW - timedelta(hours=4),
            latest_ingested_at=NOW - timedelta(minutes=20),
            latest_stored_at=NOW - timedelta(minutes=19),
            ingested_last_hour=12,
            ingested_last_day=340,
            stored=9000,
        ),
        DetectionStats(
            product="MODIS_NRT",
            latest_observed_at=NOW - timedelta(days=3),
            latest_ingested_at=NOW - timedelta(days=3),
            latest_stored_at=NOW - timedelta(days=3),
            ingested_last_hour=0,
            ingested_last_day=0,
            stored=50,
        ),
    ]
    forecast = ForecastStats(
        newest=NOW - timedelta(hours=8),
        oldest=NOW - timedelta(hours=9),
        latest_stored_at=NOW - timedelta(hours=8),
        forecast_until=NOW + timedelta(days=7),
        ingested_last_hour=0,
        ingested_last_day=310,
        stored=310,
    )
    statuses = {status.id: status for status in source_statuses(detections, forecast, NOW)}

    assert list(statuses) == [
        "VIIRS_SNPP_NRT",
        "VIIRS_NOAA20_NRT",
        "VIIRS_NOAA21_NRT",
        "MODIS_NRT",
        "open-meteo",
    ]
    assert statuses["VIIRS_NOAA20_NRT"].freshness == "fresh"
    assert statuses["VIIRS_NOAA20_NRT"].ingested_last_day == 340
    assert statuses["VIIRS_SNPP_NRT"].freshness == "no_data"
    assert statuses["VIIRS_SNPP_NRT"].stored == 0
    assert statuses["MODIS_NRT"].freshness is Freshness.STALE
    weather = statuses["open-meteo"]
    assert weather.kind == "forecast"
    assert weather.freshness == "delayed"
    assert weather.oldest_retrieved_at == NOW - timedelta(hours=9)
    assert weather.forecast_until == NOW + timedelta(days=7)


def test_consumer_lag_uses_beginning_when_nothing_is_committed() -> None:
    status = consumer_status(
        "group",
        "topic",
        state="Stable",
        members=1,
        committed={0: 90},
        beginning={0: 0, 1: 40},
        end={0: 100, 1: 50},
    )
    assert [(item.partition, item.committed, item.lag) for item in status.partitions] == [
        (0, 90, 10),
        (1, None, 10),
    ]
    assert status.lag == 20


def test_a_group_reading_several_topics_gets_a_row_per_topic() -> None:
    detections, forecasts = (
        TopicPartition("firms.detections", 0),
        TopicPartition("weather.forecasts", 0),
    )
    rows = consumer_statuses(
        {
            ENRICHMENT_GROUP: CONSUMER_GROUPS[ENRICHMENT_GROUP],
            "missing.group": ("firms.detections",),
        },
        described={ENRICHMENT_GROUP: GroupState("Stable", 1)},
        committed={ENRICHMENT_GROUP: {detections: 7, forecasts: -1}},
        assignments={"firms.detections": [detections], "weather.forecasts": [forecasts]},
        beginning={detections: 0, forecasts: 2},
        end={detections: 10, forecasts: 5},
    )
    assert [(row.group_id, row.topic, row.state, row.members, row.lag) for row in rows] == [
        (ENRICHMENT_GROUP, "firms.detections", "Stable", 1, 3),
        (ENRICHMENT_GROUP, "weather.forecasts", "Stable", 1, 3),
        ("missing.group", "firms.detections", "Unknown", 0, 10),
    ]
    assert rows[1].partitions[0].committed is None


def test_every_consumer_group_reads_known_topics() -> None:
    known = {spec.name for spec in ALL_TOPICS}
    assert all(set(topics) <= known for topics in CONSUMER_GROUPS.values())
    assert CONSUMER_GROUPS[ENRICHMENT_GROUP] == ("firms.detections", "weather.forecasts")


def test_messages_since_ignores_partitions_without_newer_records() -> None:
    a, b = TopicPartition("t", 0), TopicPartition("t", 1)
    found = {a: OffsetAndTimestamp(95, 0, -1), b: None}
    assert messages_since({a: 100, b: 7}, found) == 5


@dataclass
class Letter:
    timestamp: int
    headers: Sequence[tuple[str, bytes | None]] | None


def letter(minutes_ago: int, topic: str, error: str = "bad") -> Letter:
    failed = NOW - timedelta(minutes=minutes_ago)
    return Letter(
        int(failed.timestamp() * 1000),
        [
            ("event_id", b"x"),
            ("dlq.original.topic", topic.encode()),
            ("dlq.original.partition", b"2"),
            ("dlq.original.offset", b"not-a-number"),
            ("dlq.consumer.group", b"sink-worker.postgis.detections"),
            ("dlq.error", error.encode()),
        ],
    )


def test_dead_letter_summary_counts_by_topic_and_lists_newest_first() -> None:
    records = [letter(30, "firms.detections"), letter(5, "weather.forecasts", "late")]
    summary = summarize_dead_letters(records, retained=40, last_day=2)
    assert summary.by_topic == {"firms.detections": 1, "weather.forecasts": 1}
    assert summary.topics == ["sink-worker.dlq", "stream-processor.dlq"]
    assert summary.sample_truncated is False
    assert [item.error for item in summary.recent] == ["late", "bad"]
    assert summary.recent[0].partition == 2
    assert summary.recent[0].offset is None
    assert summarize_dead_letters(records, retained=40, last_day=9).sample_truncated is True


def no_database(_now: datetime) -> list[SourceStatus]:
    raise psycopg.OperationalError("connection refused")


def no_kafka(_now: datetime) -> KafkaSection:
    raise RuntimeError("no brokers")


def test_status_reports_each_unavailable_half() -> None:
    collector = PipelineStatusCollector(no_database, no_kafka, clock=lambda: NOW)
    app.dependency_overrides[status_collector] = lambda: collector
    response = client.get("/pipeline/status")
    assert response.status_code == 200
    status = PipelineStatus.model_validate_json(response.content)
    assert status.checked_at == NOW
    assert {source.freshness for source in status.sources} == {Freshness.UNKNOWN}
    assert status.topics is None
    assert status.consumers is None
    assert status.dead_letters is None
    assert status.errors == [POSTGIS_UNAVAILABLE, KAFKA_UNAVAILABLE]
    # The enums serialize to the same strings the dashboard reads.
    assert b'"freshness":"unknown"' in response.content


def test_unexpected_database_errors_are_not_swallowed() -> None:
    def broken(_now: datetime) -> list[SourceStatus]:
        raise TypeError("bug")

    with pytest.raises(TypeError):
        _ = PipelineStatusCollector(broken, no_kafka).collect()


def test_status_is_cached() -> None:
    calls: list[datetime] = []

    def counted() -> PipelineStatus:
        calls.append(NOW)
        return EMPTY_STATUS

    collector = PipelineStatusCollector(no_database, no_kafka)
    collector.collect = counted
    app.dependency_overrides[status_collector] = lambda: collector
    assert client.get("/pipeline/status").status_code == 200
    assert client.get("/pipeline/status").status_code == 200
    assert len(calls) == 1


def test_cache_expires_after_its_ttl() -> None:
    clock = [100.0]
    cache = StatusCache(ttl_seconds=10, monotonic=lambda: clock[0])
    fetches: list[int] = []

    def fetch() -> PipelineStatus:
        fetches.append(1)
        return EMPTY_STATUS

    _ = cache.get(fetch)
    clock[0] = 109.9
    _ = cache.get(fetch)
    assert len(fetches) == 1
    clock[0] = 110.0
    _ = cache.get(fetch)
    assert len(fetches) == 2


@pytest.mark.skipif(
    not os.environ.get("API_TEST_DATABASE_URL"),
    reason="set API_TEST_DATABASE_URL to run PostGIS integration test",
)
def test_source_freshness_from_postgis() -> None:
    database_url = os.environ["API_TEST_DATABASE_URL"]
    schema = f"api_test_{uuid.uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as db:
        _ = db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            _ = db.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
            _ = db.execute(
                """
                CREATE TABLE fire_detections (
                    product text NOT NULL, observed_at timestamptz NOT NULL,
                    ingested_at timestamptz NOT NULL, stored_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            _ = db.execute(
                """
                INSERT INTO fire_detections (product, observed_at, ingested_at) VALUES
                ('VIIRS_NOAA20_NRT', now() - interval '3 hours', now() - interval '10 minutes'),
                ('VIIRS_NOAA20_NRT', now() - interval '5 hours', now() - interval '2 hours'),
                ('MODIS_NRT', now() - interval '3 days', now() - interval '3 days')
                """
            )
            _ = db.execute(
                """
                CREATE TABLE weather_cells (
                    generated_at timestamptz NOT NULL, stored_at timestamptz NOT NULL
                )
                """
            )
            _ = db.execute("CREATE TABLE weather_hourly (valid_at timestamptz NOT NULL)")
            sources = PostgisSources(
                make_conninfo(database_url, options=f"-csearch_path={schema},public")
            )
            statuses = {status.id: status for status in sources.read(datetime.now(UTC))}
            noaa20 = statuses["VIIRS_NOAA20_NRT"]
            assert noaa20.freshness is Freshness.FRESH
            assert (noaa20.ingested_last_hour, noaa20.ingested_last_day, noaa20.stored) == (1, 2, 2)
            assert statuses["MODIS_NRT"].freshness is Freshness.STALE
            # Empty weather tables: no forecast yet.
            assert statuses["open-meteo"].freshness is Freshness.NO_DATA
        finally:
            _ = db.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)),
            )
