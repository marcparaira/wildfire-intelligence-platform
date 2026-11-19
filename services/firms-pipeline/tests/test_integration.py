import os
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from psycopg import sql

from wildfire_firms.ingest import publish
from wildfire_firms.processor import initialize, process_record

FIXTURE = Path(__file__).parent / "fixtures" / "borneo.csv"


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
                    assert publish(FIXTURE.read_text(), bootstrap, topic) == 2
                    assert publish(FIXTURE.read_text(), bootstrap, topic) == 2
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
