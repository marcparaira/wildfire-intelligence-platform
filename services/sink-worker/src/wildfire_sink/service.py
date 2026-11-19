"""Sink worker: projects Kafka topics into PostGIS."""

import logging
import os
import signal
from dataclasses import dataclass
from threading import Event, Thread

from wildfire_events.kafka import ensure_topics, idempotent_producer
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import ALL_TOPICS, SINK_WORKER_DLQ
from wildfire_sink.detections import DetectionProjection
from wildfire_sink.runner import DeadLetters, Projection, ProjectionWorker
from wildfire_sink.weather import WeatherProjection

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settings:
    kafka: str
    registry_url: str
    database_url: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
            registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081"),
            database_url=os.environ.get(
                "DATABASE_URL", "postgresql://wildfire:wildfire_dev@localhost:55432/wildfire"
            ),
        )


def projections(registry: SchemaRegistry) -> list[Projection]:
    return [DetectionProjection(registry), WeatherProjection(registry)]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    settings = Settings.from_env()
    ensure_topics(settings.kafka, ALL_TOPICS)
    registry = SchemaRegistry(settings.registry_url)
    registry.wait_until_ready()
    producer = idempotent_producer(settings.kafka, "sink-worker-dlq")
    dead_letters = DeadLetters(producer, SINK_WORKER_DLQ.name)
    stop = Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    workers = [
        Thread(
            target=ProjectionWorker(
                projection, settings.kafka, settings.database_url, dead_letters, stop
            ).run_until_stopped,
            name=projection.name,
        )
        for projection in projections(registry)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    producer.close(timeout=10)
    LOGGER.info("Sink worker stopped")


if __name__ == "__main__":
    main()
