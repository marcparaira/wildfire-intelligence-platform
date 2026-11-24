"""Satellite imagery of fire incidents from NASA GIBS daily composites.

See ``wildfire_ingestor.sentinel`` for the Sentinel-2 passes, which share the tile
grid, the store, and the topic.

Images are fetched per tile of a fixed ``TILE_DEGREES`` grid rather than per incident:
neighbouring incidents share tiles, a growing incident only adds the tiles it newly
reaches, and a tile of one layer on one day is a stable scene that is acquired once.

Each run plans the scenes covering every non-merged incident on its days of
activity, skips scenes already in ``imagery.scenes``, writes each new image to the
imagery store, and then publishes its ``SatelliteScene`` metadata (a claim check:
the image never travels through Kafka).
"""

import hashlib
import logging
import math
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum, auto
from pathlib import Path
from typing import Protocol

from wildfire_events.models import (
    FireIncident,
    ImageryLayer,
    ImageryProvider,
    IncidentStatus,
    SatelliteScene,
    scene_id,
)
from wildfire_ingestor.publisher import Publisher
from wildfire_ingestor.scheduler import RetryLater
from wildfire_ingestor.sources import GIBS_MEDIA_TYPE, FetchedImage

LOGGER = logging.getLogger(__name__)

TILE_DEGREES = 0.5
# About 110 m per pixel at the equator, finer than the 250-750 m VIIRS imagery.
TILE_PIXELS = 512
# Context around the outermost detections of an incident.
INCIDENT_MARGIN_DEGREES = 0.05
# GIBS builds a day's composite as the passes arrive. Over Southeast Asia the NOAA-20
# daytime pass (about 13:30 local time) is processed by about noon UTC.
DAY_READY_AFTER = timedelta(hours=12)
# A day or area without data comes back as a uniform frame, which JPEG compresses to
# about 0.007 bytes per pixel; real imagery, even solid cloud, is several times more.
BLANK_BYTES_PER_PIXEL = 0.01
EMPTY_RETRY_AFTER = timedelta(hours=6)
FAILED_RETRY_AFTER = timedelta(hours=1)
SHORT_HASH_CHARS = 16


@dataclass(frozen=True, slots=True, order=True)
class Tile:
    """A ``TILE_DEGREES`` square; column and row count from longitude 0 and latitude 0."""

    column: int
    row: int

    @property
    def name(self) -> str:
        return f"x{self.column}y{self.row}"

    @property
    def bounds(self) -> tuple[float, float, float, float]:
        """West, south, east, north in degrees."""
        west = self.column * TILE_DEGREES
        south = self.row * TILE_DEGREES
        return west, south, west + TILE_DEGREES, south + TILE_DEGREES


def tiles_covering(
    west: float,
    south: float,
    east: float,
    north: float,
    margin: float = INCIDENT_MARGIN_DEGREES,
) -> list[Tile]:
    """Tiles intersecting the bounds grown by ``margin``, row by row from the south-west."""
    max_column = math.ceil(180 / TILE_DEGREES) - 1
    max_row = math.ceil(90 / TILE_DEGREES) - 1

    def index(value: float, upper: int) -> int:
        return max(-upper - 1, min(upper, math.floor(value / TILE_DEGREES)))

    columns = range(index(west - margin, max_column), index(east + margin, max_column) + 1)
    rows = range(index(south - margin, max_row), index(north + margin, max_row) + 1)
    return [Tile(column, row) for row in rows for column in columns]


@dataclass(frozen=True, slots=True)
class SceneRequest:
    layer: ImageryLayer
    day: date
    tile: Tile

    @property
    def scene_id(self) -> uuid.UUID:
        return scene_id(self.layer, self.day, self.tile.name)


def day_is_ready(day: date, now: datetime) -> bool:
    """Whether the day's composite should be complete over the region."""
    return datetime.combine(day, time.min, tzinfo=UTC) + DAY_READY_AFTER <= now


def plan_scenes(
    incidents: Iterable[FireIncident],
    layers: Sequence[ImageryLayer],
    now: datetime,
    days: int,
) -> list[SceneRequest]:
    """Scenes covering each incident on its active days among the last ``days`` UTC days.

    The days count back from the newest incident activity (or today, if that is
    earlier), not from today: when detections stop arriving, or old detections are
    replayed, the incidents still get imagery for their days of activity.

    Merged incidents are skipped: the incident that absorbed them covers their area.
    Days whose composite is not complete yet are left for a later run. The result
    holds each scene once, most recent day first, then the most recently active
    incident first, then in ``layers`` order.
    """
    active = sorted(
        (incident for incident in incidents if incident.status is not IncidentStatus.MERGED),
        key=lambda incident: incident.last_observed_at,
        reverse=True,
    )
    if not active:
        return []
    today = now.astimezone(UTC).date()
    newest_activity = active[0].last_observed_at.astimezone(UTC).date()
    earliest = min(today, newest_activity) - timedelta(days=days - 1)
    planned: dict[uuid.UUID, tuple[tuple[int, int, int], SceneRequest]] = {}
    for rank, incident in enumerate(active):
        tiles = tiles_covering(incident.west, incident.south, incident.east, incident.north)
        for activity in incident.daily:
            if activity.detections == 0 or activity.day < earliest:
                continue
            if not day_is_ready(activity.day, now):
                continue
            for layer_rank, layer in enumerate(layers):
                for tile in tiles:
                    request = SceneRequest(layer, activity.day, tile)
                    order = (-activity.day.toordinal(), rank, layer_rank)
                    known = planned.get(request.scene_id)
                    if known is None or order < known[0]:
                        planned[request.scene_id] = (order, request)
    return [request for _, request in sorted(planned.values(), key=lambda item: item[0])]


def is_blank(content: bytes, width: int, height: int) -> bool:
    """Whether a JPEG is the uniform frame GIBS returns where it has no data yet."""
    return len(content) < BLANK_BYTES_PER_PIXEL * width * height


def storage_key(request: SceneRequest, sha256: str, extension: str = "jpg") -> str:
    """Store path of an image; the content hash makes every stored file immutable."""
    return (
        f"{request.layer.value}/{request.day.isoformat()}/"
        f"{request.tile.name}-{sha256[:SHORT_HASH_CHARS]}.{extension}"
    )


class SceneStore(Protocol):
    """Where image bytes are kept; ``FileSceneStore`` implements it."""

    def put(self, key: str, content: bytes) -> None: ...


class FileSceneStore:
    """Images as files under a root directory shared with the API (read-only there).

    Files are written to a temporary name and renamed, so a reader never sees a
    partial image. Keys are content-addressed, so an existing file is left alone.
    """

    def __init__(self, root: Path) -> None:
        self.root: Path = root

    def put(self, key: str, content: bytes) -> None:
        path = self.root / key
        if path.exists():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
        _ = partial.write_bytes(content)
        _ = partial.replace(path)


class ImageSource(Protocol):
    """Daily images of a layer; ``GibsClient`` implements it."""

    def fetch_image(
        self,
        layer: ImageryLayer,
        day: date,
        bounds: tuple[float, float, float, float],
        width: int,
        height: int,
    ) -> FetchedImage: ...


class Outcome(StrEnum):
    ACQUIRED = auto()
    EMPTY = auto()
    FAILED = auto()


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class ImageryProducer:
    """Acquires missing scenes for current incidents on each run.

    ``incidents`` returns the latest revision of recently updated incidents.
    ``acquired`` holds the IDs of scenes already in ``imagery.scenes`` and grows as
    scenes are published. Scenes that came back empty or failed are retried after
    a delay, so they do not use up every run's ``max_scenes_per_run``.
    """

    incidents: Callable[[], list[FireIncident]]
    source: ImageSource
    store: SceneStore
    publisher: Publisher[SatelliteScene]
    acquired: set[uuid.UUID]
    layers: tuple[ImageryLayer, ...]
    days: int
    max_scenes_per_run: int
    clock: Callable[[], datetime] = _utc_now
    _not_before: dict[uuid.UUID, datetime] = field(default_factory=dict[uuid.UUID, datetime])

    def run_once(self) -> str:
        now = self.clock()
        self._not_before = {key: due for key, due in self._not_before.items() if due > now}
        due = [
            request
            for request in plan_scenes(self.incidents(), self.layers, now, self.days)
            if request.scene_id not in self.acquired and request.scene_id not in self._not_before
        ]
        batch = due[: self.max_scenes_per_run]
        outcomes = [self._acquire(request, now) for request in batch]
        failed = outcomes.count(Outcome.FAILED)
        if batch and failed == len(batch):
            raise RuntimeError(f"all {failed} imagery requests failed")
        return (
            f"acquired {outcomes.count(Outcome.ACQUIRED)} scenes, "
            f"{outcomes.count(Outcome.EMPTY)} not available yet, {failed} failed, "
            f"{len(due) - len(batch)} deferred to the next run"
        )

    def _acquire(self, request: SceneRequest, now: datetime) -> Outcome:
        """Fetch, store, and publish one scene. ``RetryLater`` ends the run."""
        try:
            image = self.source.fetch_image(
                request.layer, request.day, request.tile.bounds, TILE_PIXELS, TILE_PIXELS
            )
        except RetryLater:
            raise
        except RuntimeError as exc:
            LOGGER.warning("Imagery %s failed: %s", _describe(request), exc)
            self._not_before[request.scene_id] = now + FAILED_RETRY_AFTER
            return Outcome.FAILED
        if is_blank(image.content, TILE_PIXELS, TILE_PIXELS):
            self._not_before[request.scene_id] = now + EMPTY_RETRY_AFTER
            return Outcome.EMPTY
        scene = self._store(request, image)
        _ = self.publisher.publish([scene])
        self.acquired.add(scene.scene_id)
        return Outcome.ACQUIRED

    def _store(self, request: SceneRequest, image: FetchedImage) -> SatelliteScene:
        digest = hashlib.sha256(image.content).hexdigest()
        key = storage_key(request, digest)
        self.store.put(key, image.content)
        west, south, east, north = request.tile.bounds
        return SatelliteScene(
            scene_id=request.scene_id,
            provider=ImageryProvider.NASA_GIBS,
            layer=request.layer,
            instrument=request.layer.instrument,
            platform=request.layer.platform,
            acquisition_date=request.day,
            tile=request.tile.name,
            west=west,
            south=south,
            east=east,
            north=north,
            width=TILE_PIXELS,
            height=TILE_PIXELS,
            media_type=GIBS_MEDIA_TYPE,
            byte_size=len(image.content),
            sha256=digest,
            storage_key=key,
            source_url=image.url,
            retrieved_at=self.clock(),
        )


def _describe(request: SceneRequest) -> str:
    return f"{request.layer} {request.day} {request.tile.name}"
