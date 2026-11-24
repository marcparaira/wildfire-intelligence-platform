"""Ingestor settings and the Kafka wiring shared by the service and one-off commands."""

import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from kafka import KafkaProducer

from wildfire_events.codec import EventCodec
from wildfire_events.kafka import ensure_topics
from wildfire_events.models import FirmsDetection, FirmsProduct, WeatherForecast
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import (
    FIRMS_DETECTIONS,
    WEATHER_FORECASTS,
    value_subject,
)
from wildfire_ingestor.sources import MAX_DAY_RANGE
from wildfire_producers.log import load_published_events
from wildfire_producers.publisher import PublishedEvents, Publisher
from wildfire_producers.runtime import env_minutes, kafka_servers, registry_url

LOGGER = logging.getLogger(__name__)
PRODUCER_CLIENT_ID = "wildfire-ingestor"
DEFAULT_FIRMS_DAYS = 2
DEFAULT_FIRMS_INTERVAL_MINUTES = 30
DEFAULT_WEATHER_INTERVAL_MINUTES = 180
# FIRMS publishes NRT detections up to about a day after observation.
PROVIDER_LATENCY = timedelta(days=1)
# The topics this service writes; the imagery worker creates imagery.scenes.
TOPICS = (FIRMS_DETECTIONS, WEATHER_FORECASTS)


def read_map_key() -> str:
    """FIRMS_MAP_KEY, or the contents of the file named by FIRMS_MAP_KEY_FILE."""
    key = os.environ.get("FIRMS_MAP_KEY", "").strip()
    path = os.environ.get("FIRMS_MAP_KEY_FILE", "")
    if not key and path and Path(path).is_file():
        key = Path(path).read_text(encoding="utf-8").strip()
    return key


def parse_products(value: str) -> tuple[FirmsProduct, ...]:
    """A comma-separated list of FIRMS product names; raises ``ValueError`` on unknown names."""
    names = [item.strip() for item in value.split(",") if item.strip()]
    known = set(FirmsProduct)
    unknown = sorted(name for name in names if name not in known)
    if unknown:
        raise ValueError(f"Unknown FIRMS products: {', '.join(unknown)}")
    return tuple(FirmsProduct(name) for name in names)


@dataclass(frozen=True, slots=True)
class Settings:
    kafka: str
    registry_url: str
    firms_map_key: str
    firms_products: tuple[FirmsProduct, ...]
    firms_days: int
    firms_interval: timedelta
    weather_interval: timedelta

    @classmethod
    def from_env(cls) -> "Settings":
        """Read and validate the environment; raises ``ValueError`` on invalid values."""
        days = int(os.environ.get("FIRMS_DAYS", str(DEFAULT_FIRMS_DAYS)))
        if not 1 <= days <= MAX_DAY_RANGE:
            raise ValueError(f"FIRMS_DAYS must be between 1 and {MAX_DAY_RANGE}")
        return cls(
            kafka=kafka_servers(),
            registry_url=registry_url(),
            firms_map_key=read_map_key(),
            firms_products=parse_products(os.environ.get("FIRMS_PRODUCTS", ",".join(FirmsProduct))),
            firms_days=days,
            firms_interval=env_minutes("FIRMS_INTERVAL_MINUTES", DEFAULT_FIRMS_INTERVAL_MINUTES),
            weather_interval=env_minutes(
                "WEATHER_INTERVAL_MINUTES", DEFAULT_WEATHER_INTERVAL_MINUTES
            ),
        )

    @property
    def dedup_window(self) -> timedelta:
        # Anything a poll can return was published at most firms_days (plus the
        # provider's latency) ago, so remembering this window prevents republishing.
        return timedelta(days=self.firms_days) + PROVIDER_LATENCY


def load_published_detections(settings: Settings) -> PublishedEvents:
    """Detection IDs already in firms.detections within the dedup window.

    Creates the service's topics first, so a fresh cluster yields an empty set.
    """
    ensure_topics(settings.kafka, TOPICS)
    published = PublishedEvents(settings.dedup_window)
    loaded = load_published_events(settings.kafka, FIRMS_DETECTIONS.name, published)
    LOGGER.info("Loaded %d recently published detection IDs from %s", loaded, FIRMS_DETECTIONS.name)
    return published


def detection_publisher(
    producer: KafkaProducer, registry: SchemaRegistry, published: PublishedEvents
) -> Publisher[FirmsDetection]:
    """Publisher for firms.detections that skips IDs in ``published``.

    Registers the schema now, so an incompatible model fails at startup.
    """
    codec = EventCodec(FirmsDetection, value_subject(FIRMS_DETECTIONS.name), registry)
    _ = codec.schema_id
    return Publisher(
        producer=producer,
        codec=codec,
        topic=FIRMS_DETECTIONS.name,
        key=lambda event: event.h3_cell,
        event_id=lambda event: event.event_id,
        published=published,
    )


def weather_publisher(
    producer: KafkaProducer, registry: SchemaRegistry
) -> Publisher[WeatherForecast]:
    """Publisher for weather.forecasts; registers the schema now, like detections."""
    codec = EventCodec(WeatherForecast, value_subject(WEATHER_FORECASTS.name), registry)
    _ = codec.schema_id
    return Publisher(
        producer=producer,
        codec=codec,
        topic=WEATHER_FORECASTS.name,
        key=lambda event: event.cell,
    )
