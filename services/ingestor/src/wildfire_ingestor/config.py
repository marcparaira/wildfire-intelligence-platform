"""Ingestor settings and the wiring shared by the service and one-off commands."""

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from kafka import KafkaProducer

from wildfire_events.codec import EventCodec, InvalidEvent
from wildfire_events.kafka import ensure_topics
from wildfire_events.models import (
    FireIncident,
    FirmsDetection,
    FirmsProduct,
    ImageryLayer,
    SatelliteScene,
    WeatherForecast,
)
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import (
    FIRE_INCIDENTS,
    FIRMS_DETECTIONS,
    SATELLITE_SCENES,
    SOURCE_TOPICS,
    WEATHER_FORECASTS,
    value_subject,
)
from wildfire_ingestor.log import latest_values, load_published_events
from wildfire_ingestor.publisher import PublishedEvents, Publisher
from wildfire_ingestor.sources import MAX_DAY_RANGE

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = "%(levelname)s %(name)s %(message)s"
PRODUCER_CLIENT_ID = "wildfire-ingestor"
DEFAULT_KAFKA = "localhost:19092"
DEFAULT_REGISTRY_URL = "http://localhost:18081"
DEFAULT_FIRMS_DAYS = 2
DEFAULT_FIRMS_INTERVAL_MINUTES = 30
DEFAULT_WEATHER_INTERVAL_MINUTES = 180
DEFAULT_IMAGERY_INTERVAL_MINUTES = 60
DEFAULT_IMAGERY_DAYS = 5
MAX_IMAGERY_DAYS = 10
DEFAULT_IMAGERY_MAX_SCENES_PER_RUN = 120
# A Sentinel-2 scene takes one 5-10 s render per granule, so fewer fit in a run.
DEFAULT_SENTINEL_MAX_SCENES_PER_RUN = 40
DEFAULT_IMAGERY_DIR = "data/imagery"
# FIRMS publishes NRT detections up to about a day after observation.
PROVIDER_LATENCY = timedelta(days=1)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    logging.getLogger("kafka").setLevel(logging.WARNING)


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


def parse_layers(value: str) -> tuple[ImageryLayer, ...]:
    """A comma-separated list of imagery layer names (empty disables imagery).

    Raises ``ValueError`` on unknown names.
    """
    names = [item.strip() for item in value.split(",") if item.strip()]
    known = set(ImageryLayer)
    unknown = sorted(name for name in names if name not in known)
    if unknown:
        raise ValueError(f"Unknown imagery layers: {', '.join(unknown)}")
    return tuple(dict.fromkeys(ImageryLayer(name) for name in names))


def _bounded_int(name: str, default: int, lowest: int, highest: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if not lowest <= value <= highest:
        raise ValueError(f"{name} must be between {lowest} and {highest}")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    kafka: str
    registry_url: str
    firms_map_key: str
    firms_products: tuple[FirmsProduct, ...]
    firms_days: int
    firms_interval: timedelta
    weather_interval: timedelta
    imagery_layers: tuple[ImageryLayer, ...]
    imagery_interval: timedelta
    imagery_days: int
    imagery_max_scenes_per_run: int
    sentinel_max_scenes_per_run: int
    imagery_dir: Path

    @classmethod
    def from_env(cls) -> "Settings":
        """Read and validate the environment; raises ``ValueError`` on invalid values."""
        days = int(os.environ.get("FIRMS_DAYS", str(DEFAULT_FIRMS_DAYS)))
        if not 1 <= days <= MAX_DAY_RANGE:
            raise ValueError(f"FIRMS_DAYS must be between 1 and {MAX_DAY_RANGE}")
        return cls(
            kafka=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA),
            registry_url=os.environ.get("SCHEMA_REGISTRY_URL", DEFAULT_REGISTRY_URL),
            firms_map_key=read_map_key(),
            firms_products=parse_products(os.environ.get("FIRMS_PRODUCTS", ",".join(FirmsProduct))),
            firms_days=days,
            firms_interval=_minutes("FIRMS_INTERVAL_MINUTES", DEFAULT_FIRMS_INTERVAL_MINUTES),
            weather_interval=_minutes("WEATHER_INTERVAL_MINUTES", DEFAULT_WEATHER_INTERVAL_MINUTES),
            imagery_layers=parse_layers(os.environ.get("IMAGERY_LAYERS", ",".join(ImageryLayer))),
            imagery_interval=_minutes("IMAGERY_INTERVAL_MINUTES", DEFAULT_IMAGERY_INTERVAL_MINUTES),
            imagery_days=_bounded_int("IMAGERY_DAYS", DEFAULT_IMAGERY_DAYS, 1, MAX_IMAGERY_DAYS),
            imagery_max_scenes_per_run=_bounded_int(
                "IMAGERY_MAX_SCENES_PER_RUN", DEFAULT_IMAGERY_MAX_SCENES_PER_RUN, 1, 10_000
            ),
            sentinel_max_scenes_per_run=_bounded_int(
                "SENTINEL_MAX_SCENES_PER_RUN", DEFAULT_SENTINEL_MAX_SCENES_PER_RUN, 1, 10_000
            ),
            imagery_dir=Path(os.environ.get("IMAGERY_DIR", DEFAULT_IMAGERY_DIR)),
        )

    @property
    def dedup_window(self) -> timedelta:
        # Anything a poll can return was published at most firms_days (plus the
        # provider's latency) ago, so remembering this window prevents republishing.
        return timedelta(days=self.firms_days) + PROVIDER_LATENCY


def _minutes(name: str, default: float) -> timedelta:
    return timedelta(minutes=float(os.environ.get(name, str(default))))


def connect_registry(settings: Settings) -> SchemaRegistry:
    """The schema registry, once it accepts requests."""
    registry = SchemaRegistry(settings.registry_url)
    registry.wait_until_ready()
    return registry


def load_published_detections(settings: Settings) -> PublishedEvents:
    """Detection IDs already in firms.detections within the dedup window.

    Creates the source topics first, so a fresh cluster yields an empty set.
    """
    ensure_topics(settings.kafka, SOURCE_TOPICS)
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


def scene_publisher(producer: KafkaProducer, registry: SchemaRegistry) -> Publisher[SatelliteScene]:
    """Publisher for imagery.scenes; registers the schema now, like detections."""
    codec = EventCodec(SatelliteScene, value_subject(SATELLITE_SCENES.name), registry)
    _ = codec.schema_id
    return Publisher(
        producer=producer,
        codec=codec,
        topic=SATELLITE_SCENES.name,
        key=lambda event: str(event.scene_id),
    )


def load_acquired_scenes(settings: Settings) -> set[uuid.UUID]:
    """IDs of the scenes in imagery.scenes, so restarts do not fetch them again."""
    acquired = {
        uuid.UUID(key.decode())
        for key, value in latest_values(settings.kafka, SATELLITE_SCENES.name).items()
        if value is not None
    }
    LOGGER.info("Loaded %d acquired scene IDs from %s", len(acquired), SATELLITE_SCENES.name)
    return acquired


class IncidentReader:
    """Latest revision of every incident in ``fire.incidents``.

    The whole compacted topic is read (about one record per incident) rather than
    a recent time window: the planner picks the days, counting back from the
    newest activity, so stale or replayed data is still covered. Record times
    cannot anchor a window, because the stream processor writes transactionally
    and a partition's last offset is a commit marker that consumers never see.
    """

    def __init__(self, settings: Settings, registry: SchemaRegistry) -> None:
        self.settings: Settings = settings
        self.codec: EventCodec[FireIncident] = EventCodec(
            FireIncident, value_subject(FIRE_INCIDENTS.name), registry
        )

    def __call__(self) -> list[FireIncident]:
        incidents: list[FireIncident] = []
        for key, value in latest_values(self.settings.kafka, FIRE_INCIDENTS.name).items():
            if value is None:
                continue
            try:
                incidents.append(self.codec.decode(value))
            except InvalidEvent as exc:
                LOGGER.warning("Skipping invalid incident %r: %s", key, exc)
        return incidents
