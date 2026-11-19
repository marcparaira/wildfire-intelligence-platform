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

from wildfire_api import pipeline
from wildfire_api.main import app
from wildfire_api.pipeline import (
    KafkaSection,
    PipelineStatus,
    _DetectionStats,  # pyright: ignore[reportPrivateUsage]
    _ForecastStats,  # pyright: ignore[reportPrivateUsage]
    classify,
    consumer_status,
    messages_since,
    source_statuses,
    summarize_dead_letters,
)

client: httpx.Client = TestClient(app)
NOW = datetime(2026, 3, 5, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def fresh_cache() -> Iterator[None]:
    pipeline.clear_cache()
    yield
    pipeline.clear_cache()


@pytest.mark.parametrize(
    ("age", "expected"),
    [
        (None, "no_data"),
        (timedelta(hours=1), "fresh"),
        (timedelta(hours=18), "fresh"),
        (timedelta(hours=19), "delayed"),
        (timedelta(hours=37), "stale"),
    ],
)
def test_classify(age: timedelta | None, expected: str) -> None:
    latest = None if age is None else NOW - age
    assert classify(latest, NOW, timedelta(hours=18), timedelta(hours=36)) == expected


def test_source_statuses_cover_every_source() -> None:
    detections = [
        _DetectionStats(
            product="VIIRS_NOAA20_NRT",
            latest_observed_at=NOW - timedelta(hours=4),
            latest_ingested_at=NOW - timedelta(minutes=20),
            latest_stored_at=NOW - timedelta(minutes=19),
            ingested_last_hour=12,
            ingested_last_day=340,
            stored=9000,
        ),
        _DetectionStats(
            product="MODIS_NRT",
            latest_observed_at=NOW - timedelta(days=3),
            latest_ingested_at=NOW - timedelta(days=3),
            latest_stored_at=NOW - timedelta(days=3),
            ingested_last_hour=0,
            ingested_last_day=0,
            stored=50,
        ),
    ]
    forecast = _ForecastStats(
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
    assert statuses["MODIS_NRT"].freshness == "stale"
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
    assert summary.sample_truncated is False
    assert [item.error for item in summary.recent] == ["late", "bad"]
    assert summary.recent[0].partition == 2
    assert summary.recent[0].offset is None
    assert summarize_dead_letters(records, retained=40, last_day=9).sample_truncated is True


def test_status_reports_each_unavailable_half(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost:1/unavailable")

    def no_kafka(_now: datetime) -> KafkaSection:
        raise RuntimeError("no brokers")

    monkeypatch.setattr(pipeline, "collect_kafka", no_kafka)
    response = client.get("/pipeline/status")
    assert response.status_code == 200
    status = PipelineStatus.model_validate_json(response.content)
    assert {source.freshness for source in status.sources} == {"unknown"}
    assert status.topics is None and status.consumers is None and status.dead_letters is None
    assert len(status.errors) == 2


def test_status_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[datetime] = []

    def counted() -> PipelineStatus:
        calls.append(NOW)
        return PipelineStatus(
            checked_at=NOW, sources=[], topics=None, consumers=None, dead_letters=None, errors=[]
        )

    monkeypatch.setattr(pipeline, "collect_status", counted)
    assert client.get("/pipeline/status").status_code == 200
    assert client.get("/pipeline/status").status_code == 200
    assert len(calls) == 1


@pytest.mark.skipif(
    not os.environ.get("API_TEST_DATABASE_URL"),
    reason="set API_TEST_DATABASE_URL to run PostGIS integration test",
)
def test_source_freshness_from_postgis(monkeypatch: pytest.MonkeyPatch) -> None:
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
            monkeypatch.setenv(
                "DATABASE_URL",
                make_conninfo(database_url, options=f"-csearch_path={schema},public"),
            )
            statuses = {
                status.id: status for status in pipeline.collect_sources(datetime.now(UTC))
            }
            noaa20 = statuses["VIIRS_NOAA20_NRT"]
            assert noaa20.freshness == "fresh"
            assert (noaa20.ingested_last_hour, noaa20.ingested_last_day, noaa20.stored) == (1, 2, 2)
            assert statuses["MODIS_NRT"].freshness == "stale"
            # Empty weather tables: no forecast yet.
            assert statuses["open-meteo"].freshness == "no_data"
        finally:
            _ = db.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)),
            )
