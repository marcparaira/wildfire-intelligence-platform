"""Ingestor service: scheduled FIRMS and Open-Meteo producers."""

import argparse
import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Event

from kafka import KafkaProducer

from wildfire_events.codec import EventCodec
from wildfire_events.kafka import ensure_topics, idempotent_producer
from wildfire_events.models import FIRMS_PRODUCTS, FirmsDetection, WeatherForecast
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import FIRMS_DETECTIONS, SOURCE_TOPICS, WEATHER_FORECASTS, value_subject
from wildfire_ingestor.firms import MAX_DAY_RANGE, FirmsProducer, read_map_key
from wildfire_ingestor.log import PublishedEvents, latest_publish_time
from wildfire_ingestor.publisher import Publisher
from wildfire_ingestor.scheduler import ScheduledJob, run_forever
from wildfire_ingestor.weather import WeatherProducer

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settings:
    kafka: str
    registry_url: str
    firms_map_key: str
    firms_products: tuple[str, ...]
    firms_days: int
    firms_interval: timedelta
    weather_interval: timedelta

    @classmethod
    def from_env(cls) -> "Settings":
        products = tuple(
            item.strip()
            for item in os.environ.get("FIRMS_PRODUCTS", ",".join(FIRMS_PRODUCTS)).split(",")
            if item.strip()
        )
        unknown = sorted(set(products) - set(FIRMS_PRODUCTS))
        if unknown:
            raise ValueError(f"Unknown FIRMS products: {', '.join(unknown)}")
        days = int(os.environ.get("FIRMS_DAYS", "2"))
        if not 1 <= days <= MAX_DAY_RANGE:
            raise ValueError(f"FIRMS_DAYS must be between 1 and {MAX_DAY_RANGE}")
        return cls(
            kafka=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
            registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081"),
            firms_map_key=read_map_key(),
            firms_products=products,
            firms_days=days,
            firms_interval=timedelta(minutes=float(os.environ.get("FIRMS_INTERVAL_MINUTES", "30"))),
            weather_interval=timedelta(
                minutes=float(os.environ.get("WEATHER_INTERVAL_MINUTES", "180"))
            ),
        )

    @property
    def dedup_window(self) -> timedelta:
        # Anything a poll can return was published at most firms_days (+ a day of
        # provider latency) ago, so remembering this window prevents republishing.
        return timedelta(days=self.firms_days + 1)


def _registry(settings: Settings) -> SchemaRegistry:
    registry = SchemaRegistry(settings.registry_url)
    registry.wait_until_ready()
    return registry


def detection_publisher(
    settings: Settings,
    published: PublishedEvents | None = None,
    producer: KafkaProducer | None = None,
) -> tuple[Publisher[FirmsDetection], KafkaProducer]:
    """Publisher for firms.detections; loads already-published IDs if none are given."""
    ensure_topics(settings.kafka, SOURCE_TOPICS)
    if published is None:
        published = PublishedEvents(settings.dedup_window)
        published.load(settings.kafka, FIRMS_DETECTIONS.name)
    producer = producer or idempotent_producer(settings.kafka, "wildfire-ingestor")
    codec = EventCodec(FirmsDetection, value_subject(FIRMS_DETECTIONS.name), _registry(settings))
    codec.schema_id  # register now so incompatible schemas fail at startup
    publisher = Publisher(
        producer=producer,
        codec=codec,
        topic=FIRMS_DETECTIONS.name,
        key=lambda event: event.h3_cell,
        event_id=lambda event: event.event_id,
        published=published,
    )
    return publisher, producer


def weather_publisher(
    settings: Settings, producer: KafkaProducer
) -> Publisher[WeatherForecast]:
    codec = EventCodec(WeatherForecast, value_subject(WEATHER_FORECASTS.name), _registry(settings))
    codec.schema_id
    return Publisher(
        producer=producer,
        codec=codec,
        topic=WEATHER_FORECASTS.name,
        key=lambda event: event.cell,
    )


def build_jobs(settings: Settings) -> tuple[list[ScheduledJob], KafkaProducer]:
    published = PublishedEvents(settings.dedup_window)
    ensure_topics(settings.kafka, SOURCE_TOPICS)
    loaded = published.load(settings.kafka, FIRMS_DETECTIONS.name)
    LOGGER.info("Loaded %d recently published detection IDs from %s", loaded, FIRMS_DETECTIONS.name)
    detections, producer = detection_publisher(settings, published)
    jobs = []
    if settings.firms_map_key:
        for product in settings.firms_products:
            firms = FirmsProducer(product, settings.firms_map_key, settings.firms_days, detections)

            def run_firms(firms: FirmsProducer = firms) -> str:
                published.prune()
                return firms.run_once()

            jobs.append(
                ScheduledJob(f"firms:{product}", settings.firms_interval.total_seconds(), run_firms)
            )
    else:
        LOGGER.warning("FIRMS_MAP_KEY is not set; FIRMS producers are disabled")

    weather = WeatherProducer(weather_publisher(settings, producer))

    def weather_delay() -> float:
        latest = latest_publish_time(settings.kafka, WEATHER_FORECASTS.name)
        if latest is None:
            return 0.0
        due = latest + settings.weather_interval
        return max(0.0, (due - datetime.now(UTC)).total_seconds())

    jobs.append(
        ScheduledJob(
            "weather:open-meteo",
            settings.weather_interval.total_seconds(),
            weather.run_once,
            weather_delay,
        )
    )
    return jobs, producer


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the scheduled FIRMS and weather producers")
    parser.add_argument("--once", action="store_true", help="Run every producer once and exit")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    settings = Settings.from_env()
    jobs, producer = build_jobs(settings)
    try:
        if args.once:
            failed = False
            for job in jobs:
                try:
                    LOGGER.info("%s: %s", job.name, job.run())
                except Exception:
                    LOGGER.exception("%s: run failed", job.name)
                    failed = True
            sys.exit(1 if failed else 0)
        stop = Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        LOGGER.info("Running %d scheduled producers", len(jobs))
        run_forever(jobs, stop)
        LOGGER.info("Ingestor stopped")
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
