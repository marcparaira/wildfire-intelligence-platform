"""Satellite imagery of fire incidents from NASA GIBS daily composites.

VIIRS NOAA-20 true and false color at 250-750 m, one composite per UTC day. Each run
plans the scenes (tiles of ``wildfire_imagery.tiles``) covering every non-merged
incident on its days of activity, skips scenes already in ``imagery.scenes``,
writes each new image to the store, and then publishes its ``SatelliteScene``.
See ``wildfire_imagery.sentinel`` for the Sentinel-2 passes.
"""

import hashlib
import logging
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from http import HTTPStatus
from typing import Final, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

from wildfire_events.models import (
    FireIncident,
    ImageryLayer,
    ImageryProvider,
    IncidentStatus,
    SatelliteScene,
)
from wildfire_imagery.scenes import (
    FetchedImage,
    Outcome,
    SceneRequest,
    SceneStore,
    storage_key,
)
from wildfire_imagery.tiles import Bounds, tiles_covering
from wildfire_producers.http import HttpGet, excerpt, http_get, retry_after_seconds
from wildfire_producers.publisher import Publisher
from wildfire_producers.scheduler import RetryLater

LOGGER = logging.getLogger(__name__)

GIBS_WMS_URL = "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi"
GIBS_TIMEOUT_SECONDS = 60
GIBS_MEDIA_TYPE: Final = "image/jpeg"
JPEG_SIGNATURE = b"\xff\xd8\xff"
# About 110 m per pixel at the equator, finer than the 250-750 m VIIRS imagery.
TILE_PIXELS = 512
# GIBS builds a day's composite as the passes arrive. Over Southeast Asia the NOAA-20
# daytime pass (about 13:30 local time) is processed by about noon UTC.
DAY_READY_AFTER = timedelta(hours=12)
# A day or area without data comes back as a uniform frame, which JPEG compresses to
# about 0.007 bytes per pixel; real imagery, even solid cloud, is several times more.
BLANK_BYTES_PER_PIXEL = 0.01
EMPTY_RETRY_AFTER = timedelta(hours=6)
FAILED_RETRY_AFTER = timedelta(hours=1)


class GibsClient:
    """NASA GIBS (Global Imagery Browse Services) WMS in geographic coordinates.

    GIBS needs no API key. It answers errors such as an unknown layer with HTTP 200
    and an XML service exception, so the body is checked to be a JPEG.
    """

    def __init__(self, get: HttpGet = http_get) -> None:
        self._get: HttpGet = get

    def fetch_image(
        self,
        layer: ImageryLayer,
        day: date,
        bounds: Bounds,
        width: int,
        height: int,
    ) -> FetchedImage:
        """The layer's daily composite over ``bounds`` (west, south, east, north) as JPEG.

        Raises ``RetryLater`` when rate limited and ``RuntimeError`` on other failures.
        """
        west, south, east, north = bounds
        parameters = {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "VERSION": "1.3.0",
            "LAYERS": layer.value,
            "STYLES": "",
            "FORMAT": GIBS_MEDIA_TYPE,
            "CRS": "EPSG:4326",
            # WMS 1.3.0 orders EPSG:4326 axes latitude first.
            "BBOX": f"{south},{west},{north},{east}",
            "WIDTH": str(width),
            "HEIGHT": str(height),
            "TIME": day.isoformat(),
        }
        url = f"{GIBS_WMS_URL}?{urlencode(parameters)}"
        try:
            content = self._get(url, timeout=GIBS_TIMEOUT_SECONDS)
        except HTTPError as exc:
            if exc.code == HTTPStatus.TOO_MANY_REQUESTS:
                seconds = retry_after_seconds(exc.headers.get("Retry-After"))
                raise RetryLater("GIBS rate limit reached", seconds) from exc
            raise RuntimeError(f"GIBS request failed with HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise RuntimeError(f"GIBS request failed: {type(exc).__name__}") from exc
        if not content.startswith(JPEG_SIGNATURE):
            raise RuntimeError(f"GIBS returned no image: {excerpt(content)}")
        return FetchedImage(url, content)


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


class ImageSource(Protocol):
    """Daily images of a layer; ``GibsClient`` implements it."""

    def fetch_image(
        self,
        layer: ImageryLayer,
        day: date,
        bounds: Bounds,
        width: int,
        height: int,
    ) -> FetchedImage: ...


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class GibsProducer:
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
