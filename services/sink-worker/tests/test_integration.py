"""PostGIS, Kafka, and schema-registry integration tests.

Skipped unless SINK_TEST_DATABASE_URL is set; the end-to-end test also needs
SINK_TEST_KAFKA and SINK_TEST_SCHEMA_REGISTRY. Each test uses its own database
schema, topics, consumer groups, and registry subjects, and removes them.
"""

import os
import time
import uuid
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from http.client import HTTPResponse
from threading import Event, Thread
from typing import cast
from urllib.request import Request, urlopen

import psycopg
import pytest
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.consumer.fetcher import ConsumerRecord
from psycopg import sql
from psycopg.conninfo import make_conninfo
from pydantic import BaseModel

from wildfire_events.cells import partition_cell
from wildfire_events.countries import Country
from wildfire_events.kafka import idempotent_producer
from wildfire_events.models import (
    EnrichedDetection,
    FireIncident,
    FireRisk,
    FirmsDetection,
    FirmsProduct,
    HourlyDetectionStats,
    HourlyWeather,
    IncidentDay,
    IncidentStatus,
    ObservedWeather,
    RiskLevel,
    WeatherForecast,
    WeatherProvider,
    detection_event_id,
)
from wildfire_events.registry import SchemaRegistry
from wildfire_sink.detections import DetectionProjection
from wildfire_sink.enrichment import EnrichmentProjection
from wildfire_sink.incidents import IncidentProjection
from wildfire_sink.runner import (
    DeadLetters,
    Projection,
    ProjectionWorker,
    database_connection,
    kafka_consumer,
    record_value,
)
from wildfire_sink.statistics import StatisticsProjection
from wildfire_sink.weather import WeatherProjection

DATABASE_URL = os.environ.get("SINK_TEST_DATABASE_URL", "")
KAFKA = os.environ.get("SINK_TEST_KAFKA", "")
REGISTRY = os.environ.get("SINK_TEST_SCHEMA_REGISTRY", "")
T0 = datetime(2026, 3, 5, 5, 30, tzinfo=UTC)

needs_database = pytest.mark.skipif(not DATABASE_URL, reason="set SINK_TEST_DATABASE_URL")
needs_stack = pytest.mark.skipif(
    not (DATABASE_URL and KAFKA and REGISTRY),
    reason="set SINK_TEST_DATABASE_URL, SINK_TEST_KAFKA, and SINK_TEST_SCHEMA_REGISTRY",
)


def detection(
    product: FirmsProduct, satellite: str, minutes: int, latitude: float, longitude: float
) -> FirmsDetection:
    observed_at = T0 + timedelta(minutes=minutes)
    return FirmsDetection(
        event_id=detection_event_id(product, satellite, observed_at, str(latitude), str(longitude)),
        product=product,
        instrument=product.instrument,
        satellite=satellite,
        observed_at=observed_at,
        latitude=latitude,
        longitude=longitude,
        h3_cell=partition_cell(latitude, longitude),
        frp_mw=10.0,
        attributes={"frp": "10.0"},
        ingested_at=T0 + timedelta(hours=3),
    )


# One fire seen by Aqua MODIS and Suomi-NPP VIIRS 4 minutes apart (~150 m apart), by
# NOAA-20 50 minutes earlier (a separate observation), plus an unrelated detection.
AQUA = detection(FirmsProduct.MODIS_NRT, "Aqua", 4, 0.5240, 114.1240)
SNPP = detection(FirmsProduct.VIIRS_SNPP_NRT, "N", 0, 0.5230, 114.1230)
SNPP_NEIGHBOR = detection(FirmsProduct.VIIRS_SNPP_NRT, "N", 0, 0.5260, 114.1230)
NOAA20 = detection(FirmsProduct.VIIRS_NOAA20_NRT, "N20", -50, 0.5234, 114.1234)
OTHER = detection(FirmsProduct.MODIS_NRT, "Terra", 0, 2.0, 112.0)
DETECTIONS = [AQUA, SNPP, SNPP_NEIGHBOR, NOAA20, OTHER]
# Both Suomi-NPP pixels match Aqua with equal rank and time; the event ID breaks the tie.
BEST_FOR_AQUA = min(SNPP.event_id, SNPP_NEIGHBOR.event_id)


@pytest.fixture
def schema_url() -> Iterator[str]:
    schema = f"sink_test_{uuid.uuid4().hex}"
    with psycopg.connect(DATABASE_URL, autocommit=True) as admin:
        _ = admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            yield make_conninfo(DATABASE_URL, options=f"-csearch_path={schema},public")
        finally:
            _ = admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def record(key: bytes | None = None) -> ConsumerRecord:
    return ConsumerRecord(
        topic="test",
        partition=0,
        leader_epoch=None,
        offset=0,
        timestamp=0,
        timestamp_type=0,
        key=key,
        value=None,
        headers=[],
        checksum=None,
        serialized_key_size=-1,
        serialized_value_size=-1,
        serialized_header_size=-1,
    )


def project[E: BaseModel](
    url: str, projection: Projection[E], events: Sequence[tuple[ConsumerRecord, E | None]]
) -> None:
    with psycopg.connect(url, autocommit=True) as connection:
        projection.initialize(connection)
        with connection.transaction():
            projection.apply(connection, events)


def duplicates(url: str) -> dict[uuid.UUID, uuid.UUID | None]:
    with psycopg.connect(url) as connection:
        return dict(connection.execute("SELECT event_id, duplicate_of FROM fire_detections"))


@needs_database
@pytest.mark.parametrize("order", [[0, 1, 2, 3, 4], [4, 3, 2, 1, 0], [2, 0, 4, 1, 3]])
def test_cross_sensor_duplicates_are_marked_independently_of_arrival_order(
    schema_url: str, order: list[int]
) -> None:
    projection = DetectionProjection(SchemaRegistry("http://unused"))
    for index in order:
        project(schema_url, projection, [(record(), DETECTIONS[index])])
    project(schema_url, projection, [(record(), AQUA), (record(), SNPP)])  # redelivery: no-op

    assert duplicates(schema_url) == {
        AQUA.event_id: BEST_FOR_AQUA,
        SNPP.event_id: None,
        SNPP_NEIGHBOR.event_id: None,
        NOAA20.event_id: None,
        OTHER.event_id: None,
    }
    with psycopg.connect(schema_url) as connection:
        row = connection.execute(
            "SELECT count(*), ST_SRID(min(location)::geometry) FROM fire_detections"
        ).fetchone()
    assert row == (5, 4326)


def forecast(generated_hours: int, temperature: float) -> WeatherForecast:
    cell = partition_cell(1, 114)
    generated_at = T0 + timedelta(hours=generated_hours)
    return WeatherForecast(
        cell=cell,
        latitude=1,
        longitude=114,
        provider=WeatherProvider.OPEN_METEO,
        model="best_match",
        generated_at=generated_at,
        hourly=[
            HourlyWeather(
                valid_at=generated_at + timedelta(hours=hour),
                temperature_c=temperature,
                precipitation_mm=0,
                wind_speed_kmh=5,
                wind_direction_degrees=90,
            )
            for hour in range(3)
        ],
    )


@needs_database
def test_weather_keeps_only_the_newest_forecast_per_cell(schema_url: str) -> None:
    projection = WeatherProjection(SchemaRegistry("http://unused"))
    newer, older = forecast(6, 30.0), forecast(0, 20.0)
    project(schema_url, projection, [(record(), newer), (record(), older)])
    with psycopg.connect(schema_url) as connection:
        rows = connection.execute(
            "SELECT h.valid_at, h.temperature_c, ST_NPoints(c.boundary) "
            + "FROM weather_hourly h JOIN weather_cells c USING (cell) ORDER BY h.valid_at"
        ).fetchall()
    assert [row[1] for row in rows] == [30.0, 30.0, 30.0]
    assert rows[0][0] == newer.generated_at
    assert rows[0][2] == 7

    project(schema_url, projection, [(record(key=newer.cell.encode()), None)])
    with psycopg.connect(schema_url) as connection:
        assert connection.execute("SELECT count(*) FROM weather_hourly").fetchone() == (0,)


def incident(revision: int, status: IncidentStatus, detections: int) -> FireIncident:
    return FireIncident(
        incident_id=uuid.UUID("2f0b6c1e-8a51-4a43-9d59-6a3c1a7b9e01"),
        revision=revision,
        status=status,
        previous_status=None if revision == 1 else IncidentStatus.NEW,
        h3_cell=AQUA.h3_cell,
        first_observed_at=T0,
        last_observed_at=T0 + timedelta(hours=revision),
        updated_at=T0 + timedelta(hours=revision),
        detection_count=detections,
        footprint_cells=detections,
        footprint_km2=5.2 * detections,
        latitude=0.524,
        longitude=114.124,
        west=114.12,
        south=0.52,
        east=114.13,
        north=0.53,
        max_frp_mw=10.0,
        total_frp_mw=10.0 * detections,
        products=[FirmsProduct.MODIS_NRT, FirmsProduct.VIIRS_SNPP_NRT],
        daily=[IncidentDay(day=T0.date(), detections=detections, new_cells=detections)],
    )


@needs_database
def test_incidents_keep_the_newest_revision_and_honour_tombstones(schema_url: str) -> None:
    projection = IncidentProjection(SchemaRegistry("http://unused"))
    second = incident(2, IncidentStatus.GROWING, 3)
    project(schema_url, projection, [(record(), incident(1, IncidentStatus.NEW, 1))])
    project(
        schema_url, projection, [(record(), second), (record(), incident(1, IncidentStatus.NEW, 1))]
    )
    with psycopg.connect(schema_url) as connection:
        row = connection.execute(
            "SELECT revision, status, previous_status, detection_count, products, daily, "
            + "ST_X(location), ST_SRID(location) FROM fire_incidents"
        ).fetchone()
    assert row == (
        2,
        "growing",
        "new",
        3,
        ["MODIS_NRT", "VIIRS_SNPP_NRT"],
        [{"day": "2026-03-05", "detections": 3, "new_cells": 3, "max_frp_mw": None}],
        114.124,
        4326,
    )

    key = str(second.incident_id).encode()
    project(schema_url, projection, [(record(key=b"not-a-uuid"), None)])  # ignored
    with psycopg.connect(schema_url) as connection:
        assert connection.execute("SELECT count(*) FROM fire_incidents").fetchone() == (1,)
    project(schema_url, projection, [(record(key=key), None)])
    with psycopg.connect(schema_url) as connection:
        assert connection.execute("SELECT count(*) FROM fire_incidents").fetchone() == (0,)


@needs_database
def test_enrichment_upserts_conditions_per_detection(schema_url: str) -> None:
    projection = EnrichmentProjection(SchemaRegistry("http://unused"))
    weather = ObservedWeather(
        valid_at=T0.replace(minute=0),
        forecast_generated_at=T0 - timedelta(hours=3),
        temperature_c=31.0,
        precipitation_mm=0.0,
        precipitation_24h_mm=0.0,
        wind_speed_kmh=8.0,
        wind_direction_degrees=180.0,
    )
    risk = FireRisk(
        score=5,
        level=RiskLevel.MODERATE,
        wind_points=0,
        temperature_points=2,
        dryness_points=3,
        intensity_points=0,
    )
    bare = EnrichedDetection(detection=AQUA, country=None, weather=None, risk=None)
    enriched = EnrichedDetection(detection=AQUA, country=Country.IDN, weather=weather, risk=risk)
    other = EnrichedDetection(detection=OTHER, country=Country.MYS, weather=None, risk=None)
    project(schema_url, projection, [(record(), bare), (record(), other)])
    project(schema_url, projection, [(record(), enriched)])  # later enrichment replaces
    with psycopg.connect(schema_url) as connection:
        rows = connection.execute(
            "SELECT event_id, country, temperature_c, risk_score, risk_level, dryness_points "
            + "FROM detection_conditions ORDER BY country"
        ).fetchall()
    assert rows == [
        (AQUA.event_id, "IDN", 31.0, 5, "moderate", 3),
        (OTHER.event_id, "MYS", None, None, None, None),
    ]


@needs_database
def test_hourly_statistics_upsert_and_tombstones(schema_url: str) -> None:
    projection = StatisticsProjection(SchemaRegistry("http://unused"))
    hour = T0.replace(minute=0)

    def stats(detections: int, with_frp: int) -> HourlyDetectionStats:
        return HourlyDetectionStats(
            country=Country.IDN,
            hour_start=hour,
            detections=detections,
            detections_with_frp=with_frp,
            total_frp_mw=12.5 * with_frp,
            max_frp_mw=12.5 if with_frp else None,
        )

    project(schema_url, projection, [(record(), stats(1, 0)), (record(), stats(3, 2))])
    with psycopg.connect(schema_url) as connection:
        rows = connection.execute(
            "SELECT country, hour_start, detections, detections_with_frp, total_frp_mw, "
            + "max_frp_mw FROM detection_stats_hourly"
        ).fetchall()
    assert rows == [("IDN", hour, 3, 2, 25.0, 12.5)]

    project(schema_url, projection, [(record(key=stats(3, 2).key.encode()), None)])
    with psycopg.connect(schema_url) as connection:
        assert connection.execute("SELECT count(*) FROM detection_stats_hourly").fetchone() == (0,)


def _delete_subject(subject: str) -> None:
    for suffix in ("", "?permanent=true"):
        try:
            request = Request(f"{REGISTRY}/subjects/{subject}{suffix}", method="DELETE")
            with cast(HTTPResponse, urlopen(request, timeout=10)):
                pass
        except OSError:
            pass


@needs_stack
def test_events_flow_from_kafka_to_postgis_with_dead_letters_and_replay(schema_url: str) -> None:
    suffix = uuid.uuid4().hex[:12]
    topic, dlq = f"sink-test-{suffix}", f"sink-test-{suffix}.dlq"
    admin = KafkaAdminClient(bootstrap_servers=KAFKA)
    admin.create_topics([NewTopic(topic, 6, 1), NewTopic(dlq, 1, 1)])  # pyright: ignore[reportUnknownMemberType]
    registry = SchemaRegistry(REGISTRY)
    producer = idempotent_producer(KAFKA, "sink-test")
    try:
        projection = DetectionProjection(registry, topic=topic, group_id=f"sink-test-{suffix}")
        for event in DETECTIONS:
            _ = producer.send(
                topic, key=event.h3_cell.encode(), value=projection.codec.encode(event)
            )
        bad_key = AQUA.h3_cell.encode()
        _ = producer.send(topic, key=bad_key, value=b"not an event")
        _ = producer.send(topic, key=bad_key, value=b"\x00\x00\x0f\xff\xff{}")
        _ = producer.send(topic, key=bad_key, value=projection.codec.encode(SNPP))  # duplicate
        producer.flush()

        def run(group_id: str) -> None:
            worker_projection = DetectionProjection(registry, topic=topic, group_id=group_id)
            stop = Event()
            worker = ProjectionWorker(
                worker_projection,
                open_consumer=lambda: kafka_consumer(KAFKA, worker_projection),
                connect=lambda: database_connection(schema_url),
                dead_letters=DeadLetters(producer, dlq),
                stop=stop,
            )
            thread = Thread(target=worker.run_until_stopped)
            thread.start()
            try:
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    lag = admin.list_consumer_group_offsets(group_id)
                    if sum(meta.offset for meta in lag.values()) == len(DETECTIONS) + 3:
                        break
                    time.sleep(0.5)
                else:
                    pytest.fail("projection did not consume the topic")
            finally:
                stop.set()
                thread.join(timeout=30)

        run(f"sink-test-{suffix}")
        first = duplicates(schema_url)
        assert len(first) == 5
        assert first[AQUA.event_id] == BEST_FOR_AQUA

        consumer = KafkaConsumer(
            dlq, bootstrap_servers=KAFKA, auto_offset_reset="earliest", consumer_timeout_ms=10000
        )
        dead = [next(consumer), next(consumer)]
        consumer.close()
        headers = [dict(dead_record.headers) for dead_record in dead]
        dead_values = [record_value(dead_record) for dead_record in dead]
        assert dead_values == [b"not an event", b"\x00\x00\x0f\xff\xff{}"]
        assert headers[0]["dlq.original.topic"] == topic.encode()
        assert headers[0]["dlq.consumer.group"] == f"sink-test-{suffix}".encode()
        assert b"too short" in headers[0]["dlq.error"] or b"magic" in headers[0]["dlq.error"]
        assert b"not registered" in headers[1]["dlq.error"]

        # Rebuild: drop the projection and replay the topic with a fresh consumer group.
        with psycopg.connect(schema_url, autocommit=True) as connection:
            _ = connection.execute("DROP TABLE fire_detections")
        run(f"sink-test-{suffix}-replay")
        assert duplicates(schema_url) == first
    finally:
        producer.close()
        admin.delete_topics([topic, dlq])  # pyright: ignore[reportUnknownMemberType]
        admin.close()
        _delete_subject(f"{topic}-value")
