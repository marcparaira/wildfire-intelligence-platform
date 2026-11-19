import logging
import os
import signal
import time

import psycopg
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import NoBrokersAvailable
from kafka.structs import OffsetAndMetadata, TopicPartition
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from wildfire_firms.events import TOPIC, Detection

LOGGER = logging.getLogger(__name__)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS firms_detections (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source text NOT NULL,
    satellite text NOT NULL,
    observed_at timestamptz NOT NULL,
    latitude numeric NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude numeric NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    location geometry(Point, 4326) NOT NULL,
    attributes jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, satellite, observed_at, latitude, longitude)
)
"""
CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS firms_detections_location_idx
ON firms_detections USING gist (location)
"""
INSERT_DETECTION = """
INSERT INTO firms_detections
    (source, satellite, observed_at, latitude, longitude, location, attributes)
VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
ON CONFLICT (source, satellite, observed_at, latitude, longitude) DO NOTHING
"""


def initialize(connection: psycopg.Connection) -> None:
    with connection.transaction():
        connection.execute(CREATE_TABLE)
        connection.execute(CREATE_INDEX)


def persist(connection: psycopg.Connection, value: bytes) -> bool:
    detection = Detection.model_validate_json(value)
    with connection.transaction():
        result = connection.execute(
            INSERT_DETECTION,
            (
                detection.source,
                detection.satellite,
                detection.observed_at,
                detection.latitude,
                detection.longitude,
                detection.longitude,
                detection.latitude,
                Jsonb(detection.attributes),
            ),
        )
        return result.rowcount == 1


def connect_database(url: str) -> psycopg.Connection:
    while True:
        try:
            return psycopg.connect(url, autocommit=True)
        except psycopg.OperationalError as exc:
            LOGGER.warning("Database unavailable; retrying in 5 seconds: %s", exc)
            time.sleep(5)


def connect_consumer(bootstrap_servers: str, topic: str, group: str) -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
        except NoBrokersAvailable:
            LOGGER.warning("Kafka unavailable; retrying in 5 seconds")
            time.sleep(5)


def process_record(
    connection: psycopg.Connection, consumer: KafkaConsumer, record: ConsumerRecord
) -> bool:
    inserted = persist(connection, record.value)
    consumer.commit(
        {
            TopicPartition(record.topic, record.partition): OffsetAndMetadata(
                record.offset + 1, "", -1
            )
        }
    )
    return inserted


def run() -> None:
    database_url = os.environ.get(
        "DATABASE_URL", "postgresql://wildfire:wildfire_dev@localhost:55432/wildfire"
    )
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    with connect_database(database_url) as connection:
        initialize(connection)
        consumer = connect_consumer(bootstrap_servers, TOPIC, "firms-processor-v1")
        try:
            for record in consumer:
                try:
                    inserted = process_record(connection, consumer, record)
                except ValidationError:
                    LOGGER.exception(
                        "Invalid detection at %s partition %s offset %s; stopping without commit",
                        record.topic,
                        record.partition,
                        record.offset,
                    )
                    raise
                LOGGER.info(
                    "%s detection at partition %s offset %s",
                    "Stored" if inserted else "Deduplicated",
                    record.partition,
                    record.offset,
                )
        finally:
            consumer.close(autocommit=False, timeout_ms=5000)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        run()
    except KeyboardInterrupt:
        LOGGER.info("Processor stopped")


if __name__ == "__main__":
    main()
