"""Imagery worker settings and the Kafka wiring of its producers."""

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from kafka import KafkaProducer

from wildfire_events.codec import EventCodec, InvalidEvent
from wildfire_events.models import FireIncident, ImageryLayer, SatelliteScene
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import FIRE_INCIDENTS, SATELLITE_SCENES, value_subject
from wildfire_producers.log import latest_values
from wildfire_producers.publisher import Publisher
from wildfire_producers.runtime import env_bounded_int, env_minutes, kafka_servers, registry_url

LOGGER = logging.getLogger(__name__)
PRODUCER_CLIENT_ID = "wildfire-imagery-worker"
DEFAULT_IMAGERY_INTERVAL_MINUTES = 60
DEFAULT_IMAGERY_DAYS = 5
MAX_IMAGERY_DAYS = 10
DEFAULT_IMAGERY_MAX_SCENES_PER_RUN = 120
# A Sentinel-2 scene takes one 5-10 s render per granule (about 2.5 per scene), so
# 200 scenes take most of an hour; fewer would leave the job idle behind a backlog.
DEFAULT_SENTINEL_MAX_SCENES_PER_RUN = 200
DEFAULT_IMAGERY_DIR = "data/imagery"


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


@dataclass(frozen=True, slots=True)
class Settings:
    kafka: str
    registry_url: str
    imagery_layers: tuple[ImageryLayer, ...]
    imagery_interval: timedelta
    imagery_days: int
    imagery_max_scenes_per_run: int
    sentinel_max_scenes_per_run: int
    imagery_dir: Path

    @classmethod
    def from_env(cls) -> "Settings":
        """Read and validate the environment; raises ``ValueError`` on invalid values."""
        return cls(
            kafka=kafka_servers(),
            registry_url=registry_url(),
            imagery_layers=parse_layers(os.environ.get("IMAGERY_LAYERS", ",".join(ImageryLayer))),
            imagery_interval=env_minutes(
                "IMAGERY_INTERVAL_MINUTES", DEFAULT_IMAGERY_INTERVAL_MINUTES
            ),
            imagery_days=env_bounded_int("IMAGERY_DAYS", DEFAULT_IMAGERY_DAYS, 1, MAX_IMAGERY_DAYS),
            imagery_max_scenes_per_run=env_bounded_int(
                "IMAGERY_MAX_SCENES_PER_RUN", DEFAULT_IMAGERY_MAX_SCENES_PER_RUN, 1, 10_000
            ),
            sentinel_max_scenes_per_run=env_bounded_int(
                "SENTINEL_MAX_SCENES_PER_RUN", DEFAULT_SENTINEL_MAX_SCENES_PER_RUN, 1, 10_000
            ),
            imagery_dir=Path(os.environ.get("IMAGERY_DIR", DEFAULT_IMAGERY_DIR)),
        )


def scene_publisher(producer: KafkaProducer, registry: SchemaRegistry) -> Publisher[SatelliteScene]:
    """Publisher for imagery.scenes; registers the schema now, so an incompatible model
    fails at startup."""
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
