"""Ingestor settings and the publishers shared by the service and one-off commands."""

import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from kafka import KafkaProducer

from wildfire_events.codec import EventCodec
from wildfire_events.kafka import ensure_topics, idempotent_producer
from wildfire_events.models import FIRMS_PRODUCTS, FirmsDetection, WeatherForecast
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import FIRMS_DETECTIONS, SOURCE_TOPICS, WEATHER_FORECASTS, value_subject
from wildfire_ingestor.log import PublishedEvents
from wildfire_ingestor.publisher import Publisher

MAX_DAY_RANGE = 5


def read_map_key() -> str:
    """FIRMS_MAP_KEY, or the contents of the file named by FIRMS_MAP_KEY_FILE."""
    key = os.environ.get("FIRMS_MAP_KEY", "").strip()
    path = os.environ.get("FIRMS_MAP_KEY_FILE", "")
    if not key and path and Path(path).is_file():
        key = Path(path).read_text(encoding="utf-8").strip()
    return key


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
        _ = published.load(settings.kafka, FIRMS_DETECTIONS.name)
    producer = producer or idempotent_producer(settings.kafka, "wildfire-ingestor")
    codec = EventCodec(FirmsDetection, value_subject(FIRMS_DETECTIONS.name), _registry(settings))
    _ = codec.schema_id  # register now so incompatible schemas fail at startup
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
    _ = codec.schema_id
    return Publisher(
        producer=producer,
        codec=codec,
        topic=WEATHER_FORECASTS.name,
        key=lambda event: event.cell,
    )
