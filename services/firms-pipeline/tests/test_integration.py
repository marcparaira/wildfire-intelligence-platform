import os
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from psycopg import sql

from wildfire_firms.ingest import filter_countries, parse_csv, publish
from wildfire_firms.processor import initialize, process_record

FIXTURE = Path(__file__).parent / "fixtures" / "borneo.csv"


@pytest.mark.skipif(
    not os.environ.get("PIPELINE_TEST_DATABASE_URL"),
    reason="set PIPELINE_TEST_DATABASE_URL to run PostGIS integration test",
)
def test_country_filter_covers_requested_countries_and_excludes_sea_and_neighbors() -> None:
    cities = [
        (16.866, 96.195),   # Myanmar
        (13.756, 100.502),  # Thailand
        (17.975, 102.633),  # Laos
        (11.557, 104.928),  # Cambodia
        (21.028, 105.854),  # Vietnam
        (4.903, 114.939),   # Brunei
        (3.139, 101.687),   # Malaysia
        (1.352, 103.82),    # Singapore
        (14.599, 120.984),  # Philippines
        (-6.208, 106.845),  # Indonesia
        (-9.443, 147.18),   # Papua New Guinea
        (-8.56, 125.573),   # Timor-Leste
        (27.33, 97.42),     # Northern Myanmar
        (-6.22, 155.56),    # Bougainville, Papua New Guinea
    ]
    excluded = [(2, 105), (25, 100), (-13, 135)]
    template = next(parse_csv(FIXTURE.read_text()))
    detections = [
        template.model_copy(update={"latitude": Decimal(str(lat)), "longitude": Decimal(str(lon))})
        for lat, lon in cities + excluded
    ]
    assert filter_countries(detections, os.environ["PIPELINE_TEST_DATABASE_URL"]) == detections[
        : len(cities)
    ]


@pytest.mark.skipif(
    not os.environ.get("PIPELINE_TEST_DATABASE_URL"),
    reason="set PIPELINE_TEST_DATABASE_URL and PIPELINE_TEST_KAFKA to run integration test",
)
def test_firms_kafka_postgis_pipeline_and_replay() -> None:
    database_url = os.environ["PIPELINE_TEST_DATABASE_URL"]
    bootstrap = os.environ["PIPELINE_TEST_KAFKA"]
    suffix = uuid.uuid4().hex
    schema = f"pipeline_test_{suffix}"
    topic = f"pipeline-test-{suffix}"
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
            )
            initialize(connection)
            admin = KafkaAdminClient(bootstrap_servers=bootstrap)
            try:
                admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=bootstrap,
                    group_id=f"pipeline-test-{suffix}",
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                )
                try:
                    records = list(parse_csv(FIXTURE.read_text()))
                    assert publish(records, bootstrap, topic) == 2
                    assert publish(records, bootstrap, topic) == 2
                    results = []
                    deadline = time.monotonic() + 60
                    while len(results) < 4 and time.monotonic() < deadline:
                        for records in consumer.poll(timeout_ms=1000).values():
                            for record in records:
                                results.append(process_record(connection, consumer, record))
                    assert results == [True, True, False, False]
                    rows = connection.execute(
                        """
                        SELECT observed_at, ST_X(location), ST_Y(location), ST_SRID(location),
                               attributes->>'frp'
                        FROM firms_detections ORDER BY observed_at
                        """
                    ).fetchall()
                    assert rows == [
                        (datetime(2026, 3, 5, 0, 7, tzinfo=UTC), 114.1234, 0.5234, 4326, "12.5"),
                        (datetime(2026, 3, 5, 12, 34, tzinfo=UTC), 115.4567, 1.1234, 4326, "24.0"),
                    ]
                finally:
                    consumer.close()
            finally:
                admin.delete_topics([topic])
                admin.close()
        finally:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
