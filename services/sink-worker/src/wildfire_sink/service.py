"""Sink worker: projects Kafka topics into PostGIS.

``main`` is the composition root: it creates the Kafka, schema-registry, and
database clients and hands them to the projection workers.
"""

import logging
import os
import signal
from dataclasses import dataclass
from threading import Event, Thread
from types import FrameType

from pydantic import BaseModel

from wildfire_events.kafka import ensure_topics, idempotent_producer
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import ALL_TOPICS, SINK_WORKER_DLQ
from wildfire_sink.detections import DetectionProjection
from wildfire_sink.enrichment import EnrichmentProjection
from wildfire_sink.incidents import IncidentProjection
from wildfire_sink.runner import (
    DeadLetters,
    DeadLetterSink,
    Projection,
    ProjectionWorker,
    database_connection,
    kafka_consumer,
)
from wildfire_sink.statistics import StatisticsProjection
from wildfire_sink.weather import WeatherProjection

LOGGER = logging.getLogger(__name__)
DEFAULT_KAFKA = "localhost:19092"
DEFAULT_REGISTRY_URL = "http://localhost:18081"
DEFAULT_DATABASE_URL = "postgresql://wildfire:wildfire_dev@localhost:55432/wildfire"
DEAD_LETTER_CLIENT_ID = "sink-worker-dlq"
PRODUCER_CLOSE_TIMEOUT_SECONDS = 10


@dataclass(frozen=True, slots=True)
class Settings:
    kafka: str
    registry_url: str
    database_url: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA),
            registry_url=os.environ.get("SCHEMA_REGISTRY_URL", DEFAULT_REGISTRY_URL),
            database_url=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
        )


def worker_thread[E: BaseModel](
    projection: Projection[E], settings: Settings, dead_letters: DeadLetterSink, stop: Event
) -> Thread:
    worker = ProjectionWorker(
        projection,
        open_consumer=lambda: kafka_consumer(settings.kafka, projection),
        connect=lambda: database_connection(settings.database_url),
        dead_letters=dead_letters,
        stop=stop,
    )
    return Thread(target=worker.run_until_stopped, name=projection.name)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    settings = Settings.from_env()
    ensure_topics(settings.kafka, ALL_TOPICS)
    registry = SchemaRegistry(settings.registry_url)
    registry.wait_until_ready()
    producer = idempotent_producer(settings.kafka, DEAD_LETTER_CLIENT_ID)
    try:
        dead_letters = DeadLetters(producer, SINK_WORKER_DLQ.name)
        stop = Event()

        def request_stop(_signum: int, _frame: FrameType | None) -> None:
            stop.set()

        _ = signal.signal(signal.SIGTERM, request_stop)
        _ = signal.signal(signal.SIGINT, request_stop)
        workers = [
            worker_thread(DetectionProjection(registry), settings, dead_letters, stop),
            worker_thread(WeatherProjection(registry), settings, dead_letters, stop),
            worker_thread(IncidentProjection(registry), settings, dead_letters, stop),
            worker_thread(EnrichmentProjection(registry), settings, dead_letters, stop),
            worker_thread(StatisticsProjection(registry), settings, dead_letters, stop),
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
    finally:
        producer.close(timeout=PRODUCER_CLOSE_TIMEOUT_SECONDS)
    LOGGER.info("Sink worker stopped")


if __name__ == "__main__":
    main()
