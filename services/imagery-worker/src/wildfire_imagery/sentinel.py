"""Sentinel-2 short-wave infrared imagery of fire incidents, to confirm them.

VIIRS sees a fire's heat from a 375 m pixel, but its imagery cannot show a fire much
smaller than that. Sentinel-2 images at 20 m in short-wave infrared, where flames
are bright and smoke is thin, every 2-5 days and a few days late, from Microsoft
Planetary Computer.

Scenes use the tile grid of ``wildfire_imagery.tiles``. A scene is one tile on one
pass (UTC day): the pass's granules (110 km MGRS tiles, which overlap) are each
rendered over the tile, composited, and stored as one WebP image whose areas
without data stay transparent. Each run finds the passes over the tiles of current
incidents, from ``SENTINEL_CONTEXT_DAYS`` before their first to as long after their
last day of activity, and acquires those not in ``imagery.scenes`` yet: each tile's
first scene before any tile's second, and for each tile the passes during the
activity first, then the burn scars after it, then the baselines before.
"""

import hashlib
import io
import logging
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import IntEnum
from itertools import groupby
from statistics import fmean
from typing import Protocol

from PIL import Image, UnidentifiedImageError

from wildfire_events.models import (
    SENTINEL_CONTEXT_DAYS,
    FireIncident,
    ImageryLayer,
    IncidentStatus,
    SatelliteScene,
)
from wildfire_imagery.planetary_computer import Ring, SentinelGranule
from wildfire_imagery.scenes import FetchedImage, Outcome, SceneRequest, SceneStore, storage_key
from wildfire_imagery.tiles import TILE_DEGREES, Bounds, Tile, tiles_covering
from wildfire_producers.publisher import Publisher
from wildfire_producers.scheduler import RetryLater

LOGGER = logging.getLogger(__name__)

LAYER = ImageryLayer.SENTINEL2_SWIR
# About 27 m per pixel on the 0.5 degree grid, close to the bands' 20 m.
TILE_PIXELS = 2048
# Granules this cloudy rarely show the ground; the tile itself may still be clearer.
MAX_GRANULE_CLOUD_COVER = 90.0
# Scenes whose data covers less of the tile are not stored. Footprints decide which
# granules are rendered, so this only catches slivers and footprint inaccuracies.
MIN_COVERAGE = 0.01
# Granules whose footprint covers less of a tile are not rendered for it. At the edge
# of a swath, a granule's bounding box covers tiles its data barely reaches.
MIN_FOOTPRINT_OVERLAP = 0.01
MEDIA_TYPE = "image/webp"
WEBP_QUALITY = 85
FAILED_RETRY_AFTER = timedelta(hours=1)


class Timing(IntEnum):
    """When a pass was relative to the tile's fire activity; also the acquisition order."""

    DURING = 0
    AFTER = 1
    BEFORE = 2


@dataclass(frozen=True, slots=True)
class PassRequest:
    """One tile on one pass, with the granules of the pass that reach the tile."""

    scene: SceneRequest
    timing: Timing
    granules: tuple[SentinelGranule, ...]

    @property
    def scene_id(self) -> uuid.UUID:
        return self.scene.scene_id


def tile_activity(
    incidents: Iterable[FireIncident], now: datetime, days: int
) -> dict[Tile, frozenset[date]]:
    """UTC days with detections of each tile of current incidents.

    Like ``plan_scenes``, the window counts back ``days`` from the newest incident
    activity (or today, if earlier), and merged incidents are skipped. Activity up to
    ``SENTINEL_CONTEXT_DAYS`` earlier still counts, so passes after it are acquired.
    Only tiles containing the incident's detection box are taken, without the margin
    of the daily imagery: a 0.5 degree tile already shows the surroundings, and a
    margin tile, which shows none of the incident, would be acquired for every pass.
    """
    active = [incident for incident in incidents if incident.status is not IncidentStatus.MERGED]
    if not active:
        return {}
    today = now.astimezone(UTC).date()
    newest = max(incident.last_observed_at for incident in active).astimezone(UTC).date()
    earliest = min(today, newest) - timedelta(days=days - 1 + SENTINEL_CONTEXT_DAYS)
    activity: dict[Tile, set[date]] = {}
    for incident in active:
        incident_days = {
            entry.day for entry in incident.daily if entry.detections and entry.day >= earliest
        }
        if not incident_days:
            continue
        tiles = tiles_covering(incident.west, incident.south, incident.east, incident.north, 0)
        for tile in tiles:
            activity.setdefault(tile, set()).update(incident_days)
    return {tile: frozenset(days_) for tile, days_ in activity.items()}


def search_window(activity: dict[Tile, frozenset[date]], now: datetime) -> tuple[date, date]:
    """The UTC days whose passes may be needed; ``activity`` must not be empty."""
    every_day = [day for days in activity.values() for day in days]
    context = timedelta(days=SENTINEL_CONTEXT_DAYS)
    return min(every_day) - context, min(max(every_day) + context, now.astimezone(UTC).date())


def search_areas(tiles: Iterable[Tile]) -> list[Bounds]:
    """The tiles as rectangles, with neighbours in a row merged into one strip."""
    areas: list[Bounds] = []
    for row, in_row in groupby(
        sorted(tiles, key=lambda tile: (tile.row, tile.column)), lambda t: t.row
    ):
        columns = [tile.column for tile in in_row]
        start = previous = columns[0]
        for column in [*columns[1:], None]:
            if column is not None and column == previous + 1:
                previous = column
                continue
            areas.append(
                (
                    start * TILE_DEGREES,
                    row * TILE_DEGREES,
                    (previous + 1) * TILE_DEGREES,
                    (row + 1) * TILE_DEGREES,
                )
            )
            if column is not None:
                start = previous = column
    return areas


def timing(day: date, active_days: frozenset[date]) -> Timing | None:
    """Whether a pass on ``day`` is during, after, or before activity, or too far from it."""
    if day in active_days:
        return Timing.DURING
    context = timedelta(days=SENTINEL_CONTEXT_DAYS)
    if any(day - context <= active < day for active in active_days):
        return Timing.AFTER
    if any(day < active <= day + context for active in active_days):
        return Timing.BEFORE
    return None


Point = tuple[float, float]


def _clip(points: list[Point], axis: int, limit: float, *, keep_above: bool) -> list[Point]:
    """One Sutherland-Hodgman step: the polygon on one side of ``x`` or ``y`` = ``limit``."""

    def inside(point: Point) -> bool:
        return point[axis] >= limit if keep_above else point[axis] <= limit

    def crossing(a: Point, b: Point) -> Point:
        share = (limit - a[axis]) / (b[axis] - a[axis])
        other = 1 - axis
        value = a[other] + (b[other] - a[other]) * share
        return (limit, value) if axis == 0 else (value, limit)

    clipped: list[Point] = []
    for index, current in enumerate(points):
        previous = points[index - 1]
        if inside(current):
            if not inside(previous):
                clipped.append(crossing(previous, current))
            clipped.append(current)
        elif inside(previous):
            clipped.append(crossing(previous, current))
    return clipped


def clipped_area(ring: Ring, bounds: Bounds) -> float:
    """Area in square degrees of a polygon ring clipped to a rectangle."""
    west, south, east, north = bounds
    points = list(ring)
    for axis, limit, keep_above in (
        (0, west, True),
        (0, east, False),
        (1, south, True),
        (1, north, False),
    ):
        points = _clip(points, axis, limit, keep_above=keep_above)
        if not points:
            return 0.0
    twice_area = sum(
        x1 * y2 - x2 * y1
        for (x1, y1), (x2, y2) in zip(points, [*points[1:], points[0]], strict=True)
    )
    return abs(twice_area) / 2


def reaches(granule: SentinelGranule, tile: Bounds) -> bool:
    """Whether the granule has data on at least ``MIN_FOOTPRINT_OVERLAP`` of the tile.

    Without a footprint, an overlap of the bounding boxes counts.
    """
    west, south, east, north = granule.bounds
    if not (west < tile[2] and tile[0] < east and south < tile[3] and tile[1] < north):
        return False
    if not granule.footprint:
        return True
    tile_area = (tile[2] - tile[0]) * (tile[3] - tile[1])
    covered = sum(clipped_area(ring, tile) for ring in granule.footprint)
    return covered >= MIN_FOOTPRINT_OVERLAP * tile_area


def plan_passes(
    activity: dict[Tile, frozenset[date]], granules: Iterable[SentinelGranule]
) -> list[PassRequest]:
    """One request per tile and pass day near the tile's activity, in acquisition order.

    Passes during activity come first, then after it, then before it; the most
    recent first within each. A request's granules are ordered cloudiest first, so
    the clearest is drawn last, on top, where they overlap.
    """
    by_scene: dict[tuple[Tile, date, Timing], list[SentinelGranule]] = {}
    for granule in granules:
        day = granule.acquired_at.astimezone(UTC).date()
        for tile, active_days in activity.items():
            when = timing(day, active_days)
            if when is not None and reaches(granule, tile.bounds):
                by_scene.setdefault((tile, day, when), []).append(granule)
    requests: list[PassRequest] = []
    for (tile, day, when), found in by_scene.items():
        ordered = sorted(
            {granule.item_id: granule for granule in found}.values(),
            key=lambda granule: (-granule.cloud_cover, granule.item_id),
        )
        requests.append(PassRequest(SceneRequest(LAYER, day, tile), when, tuple(ordered)))
    return sorted(requests, key=_pass_order)


def _pass_order(request: PassRequest) -> tuple[Timing, int, Tile]:
    return request.timing, -request.scene.day.toordinal(), request.scene.tile


def breadth_first(requests: Iterable[PassRequest], acquired: set[uuid.UUID]) -> list[PassRequest]:
    """The requests not in ``acquired``, each tile's next scene before any tile's later one.

    A tile's scenes keep the ``plan_passes`` order, and its rank counts the scenes it
    already has: every tile gets its first scene before any gets a second, so a
    backlog gives each incident one image first instead of every pass of a few.
    Within a rank, the ``plan_passes`` order holds.
    """
    have: dict[Tile, int] = {}
    due: list[PassRequest] = []
    for request in requests:
        if request.scene_id in acquired:
            have[request.scene.tile] = have.get(request.scene.tile, 0) + 1
        else:
            due.append(request)
    ranked: list[tuple[int, PassRequest]] = []
    for request in sorted(due, key=_pass_order):
        tile = request.scene.tile
        ranked.append((have.get(tile, 0), request))
        have[tile] = have.get(tile, 0) + 1
    ranked.sort(key=lambda item: (item[0], _pass_order(item[1])))
    return [request for _, request in ranked]


def composite(images: Sequence[bytes], width: int, height: int) -> tuple[bytes, float]:
    """The images drawn over each other in order, as WebP, and the share of covered pixels.

    Raises ``RuntimeError`` when an image cannot be decoded or has the wrong size.
    """
    canvas = Image.new("RGBA", (width, height))
    for content in images:
        try:
            with Image.open(io.BytesIO(content)) as image:
                layer = image.convert("RGBA")
        except (UnidentifiedImageError, OSError) as exc:
            raise RuntimeError(f"undecodable Sentinel-2 image: {exc}") from exc
        if layer.size != canvas.size:
            raise RuntimeError(f"Sentinel-2 image is {layer.size}, expected {canvas.size}")
        canvas.alpha_composite(layer)
    transparent = canvas.getchannel("A").histogram()[0]
    coverage = 1 - transparent / (width * height)
    output = io.BytesIO()
    canvas.save(output, format="WEBP", quality=WEBP_QUALITY)
    return output.getvalue(), coverage


class GranuleSource(Protocol):
    """Sentinel-2 granules and their images; ``PlanetaryComputerClient`` implements it."""

    def search(
        self, areas: Sequence[Bounds], start: date, end: date, max_cloud_cover: float
    ) -> list[SentinelGranule]: ...

    def render(self, item_id: str, bounds: Bounds, width: int, height: int) -> FetchedImage: ...


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class SentinelProducer:
    """Acquires missing Sentinel-2 scenes of current incidents on each run.

    ``incidents`` returns the latest incident revisions and
    ``acquired`` the IDs of scenes in ``imagery.scenes``. Failed scenes are retried
    after a delay; scenes the pass's data does not reach are not retried while the
    process runs.
    """

    incidents: Callable[[], list[FireIncident]]
    source: GranuleSource
    store: SceneStore
    publisher: Publisher[SatelliteScene]
    acquired: set[uuid.UUID]
    days: int
    max_scenes_per_run: int
    clock: Callable[[], datetime] = _utc_now
    _not_before: dict[uuid.UUID, datetime] = field(default_factory=dict[uuid.UUID, datetime])
    _no_data: set[uuid.UUID] = field(default_factory=set[uuid.UUID])

    def run_once(self) -> str:
        now = self.clock()
        self._not_before = {key: due for key, due in self._not_before.items() if due > now}
        activity = tile_activity(self.incidents(), now, self.days)
        if not activity:
            return "no incidents need Sentinel-2 imagery"
        start, end = search_window(activity, now)
        granules = self.source.search(search_areas(activity), start, end, MAX_GRANULE_CLOUD_COVER)
        due = [
            request
            for request in breadth_first(plan_passes(activity, granules), self.acquired)
            if request.scene_id not in self._not_before and request.scene_id not in self._no_data
        ]
        batch = due[: self.max_scenes_per_run]
        outcomes = [self._acquire(request, now) for request in batch]
        failed = outcomes.count(Outcome.FAILED)
        if batch and failed == len(batch):
            raise RuntimeError(f"all {failed} Sentinel-2 requests failed")
        return (
            f"found {len(granules)} granules; acquired {outcomes.count(Outcome.ACQUIRED)} scenes, "
            f"{outcomes.count(Outcome.EMPTY)} without data, {failed} failed, "
            f"{len(due) - len(batch)} deferred to the next run"
        )

    def _acquire(self, request: PassRequest, now: datetime) -> Outcome:
        """Render, composite, store, and publish one scene. ``RetryLater`` ends the run."""
        bounds = request.scene.tile.bounds
        try:
            images = [
                self.source.render(granule.item_id, bounds, TILE_PIXELS, TILE_PIXELS)
                for granule in request.granules
            ]
            content, coverage = composite(
                [image.content for image in images], TILE_PIXELS, TILE_PIXELS
            )
        except RetryLater:
            raise
        except RuntimeError as exc:
            LOGGER.warning("Sentinel-2 %s failed: %s", _describe(request), exc)
            self._not_before[request.scene_id] = now + FAILED_RETRY_AFTER
            return Outcome.FAILED
        if coverage < MIN_COVERAGE:
            self._no_data.add(request.scene_id)
            return Outcome.EMPTY
        scene = self._store(request, content, images[-1].url)
        _ = self.publisher.publish([scene])
        self.acquired.add(scene.scene_id)
        return Outcome.ACQUIRED

    def _store(self, request: PassRequest, content: bytes, url: str) -> SatelliteScene:
        digest = hashlib.sha256(content).hexdigest()
        key = storage_key(request.scene, digest)
        self.store.put(key, content)
        west, south, east, north = request.scene.tile.bounds
        clearest = request.granules[-1]
        return SatelliteScene(
            scene_id=request.scene_id,
            provider=LAYER.provider,
            layer=LAYER,
            instrument=LAYER.instrument,
            platform=clearest.platform,
            acquisition_date=request.scene.day,
            tile=request.scene.tile.name,
            west=west,
            south=south,
            east=east,
            north=north,
            width=TILE_PIXELS,
            height=TILE_PIXELS,
            media_type=MEDIA_TYPE,
            byte_size=len(content),
            sha256=digest,
            storage_key=key,
            source_url=url,
            retrieved_at=self.clock(),
            acquired_at=min(granule.acquired_at for granule in request.granules),
            cloud_cover=round(fmean(granule.cloud_cover for granule in request.granules), 1),
            granules=[granule.item_id for granule in request.granules],
        )


def _describe(request: PassRequest) -> str:
    return f"{request.scene.day} {request.scene.tile.name} ({request.timing.name.lower()})"
