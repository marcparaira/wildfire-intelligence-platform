"""Satellite imagery of fire incidents.

Scene metadata comes from the ``satellite_scenes`` projection of ``imagery.scenes``;
the images themselves are read from the imagery store the ingestor writes to
(mounted read-only into the API container).
"""

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Annotated
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from pydantic import BaseModel

from wildfire_api.db import connect, fetch_all
from wildfire_api.settings import SettingsDep
from wildfire_events.models import (
    SENTINEL_CONTEXT_DAYS,
    ImageryLayer,
    ImageryProvider,
    Instrument,
)

LOGGER = logging.getLogger(__name__)
router = APIRouter(tags=["imagery"])

# Scenes over the incident's bounding box grown by this margin, so the report shows
# the tiles around the outermost detections like the imagery producer fetches them.
SCENE_MARGIN_DEGREES = 0.05
# Stored images never change (their store keys contain the content hash).
IMMUTABLE_CACHE = "public, max-age=31536000, immutable"


class SceneSummary(BaseModel):
    scene_id: UUID
    provider: ImageryProvider
    layer: ImageryLayer
    instrument: Instrument
    platform: str
    acquisition_date: date
    retrieved_at: datetime
    bbox: tuple[float, float, float, float]  # west, south, east, north
    width: int
    height: int
    acquired_at: datetime | None = None  # sensing time of a single pass (Sentinel-2)
    cloud_cover: float | None = None  # of the source granules, percent


class IncidentImagery(BaseModel):
    incident_id: UUID
    first_day: date
    last_day: date
    scenes: list[SceneSummary]


class _IncidentWindow(BaseModel):
    first_day: date
    last_day: date
    west: float
    south: float
    east: float
    north: float


class _SceneRow(BaseModel):
    scene_id: UUID
    provider: ImageryProvider
    layer: ImageryLayer
    instrument: Instrument
    platform: str
    acquisition_date: date
    retrieved_at: datetime
    west: float
    south: float
    east: float
    north: float
    width: int
    height: int
    acquired_at: datetime | None
    cloud_cover: float | None

    def summary(self) -> SceneSummary:
        return SceneSummary(
            scene_id=self.scene_id,
            provider=self.provider,
            layer=self.layer,
            instrument=self.instrument,
            platform=self.platform,
            acquisition_date=self.acquisition_date,
            retrieved_at=self.retrieved_at,
            bbox=(self.west, self.south, self.east, self.north),
            width=self.width,
            height=self.height,
            acquired_at=self.acquired_at,
            cloud_cover=self.cloud_cover,
        )


class _TableExists(BaseModel):
    exists: bool


class _StoredImage(BaseModel):
    storage_key: str
    media_type: str


class SceneNotFound(LookupError):
    """No stored scene (or image) with this ID."""


class IncidentNotFound(LookupError):
    """No incident with this ID has been stored."""


def find_incident_imagery(connection: psycopg.Connection, incident_id: UUID) -> IncidentImagery:
    """Scenes over the incident on its UTC days of activity, oldest day first.

    Sentinel-2 passes are included from ``SENTINEL_CONTEXT_DAYS`` before the first to
    as long after the last day, as the imagery producer acquires them: a pass rarely
    falls on a day of activity, and those around it show the ground before the fire
    and its burn scar. Raises ``IncidentNotFound``.
    """
    windows = fetch_all(
        connection,
        _IncidentWindow,
        """
        SELECT (first_observed_at AT TIME ZONE 'UTC')::date AS first_day,
               (last_observed_at AT TIME ZONE 'UTC')::date AS last_day,
               west, south, east, north
        FROM fire_incidents WHERE incident_id = %s
        """,
        [incident_id],
    )
    if not windows:
        raise IncidentNotFound(incident_id)
    window = windows[0]
    [table] = fetch_all(
        connection,
        _TableExists,
        "SELECT to_regclass('satellite_scenes') IS NOT NULL AS exists",
    )
    # Until the sink worker creates the projection, there is simply no imagery yet.
    rows = (
        []
        if not table.exists
        else fetch_all(
            connection,
            _SceneRow,
            """
        SELECT scene_id, provider, layer, instrument, platform, acquisition_date,
               retrieved_at, west, south, east, north, width, height, acquired_at,
               cloud_cover
        FROM satellite_scenes
        WHERE acquisition_date BETWEEN %s - CASE WHEN provider = %s THEN %s ELSE 0 END
                                   AND %s + CASE WHEN provider = %s THEN %s ELSE 0 END
          AND ST_Intersects(footprint, ST_MakeEnvelope(%s, %s, %s, %s, 4326))
        ORDER BY acquisition_date, layer, south, west
        """,
            [
                window.first_day,
                ImageryProvider.PLANETARY_COMPUTER,
                SENTINEL_CONTEXT_DAYS,
                window.last_day,
                ImageryProvider.PLANETARY_COMPUTER,
                SENTINEL_CONTEXT_DAYS,
                window.west - SCENE_MARGIN_DEGREES,
                window.south - SCENE_MARGIN_DEGREES,
                window.east + SCENE_MARGIN_DEGREES,
                window.north + SCENE_MARGIN_DEGREES,
            ],
        )
    )
    return IncidentImagery(
        incident_id=incident_id,
        first_day=window.first_day,
        last_day=window.last_day,
        scenes=[row.summary() for row in rows],
    )


class ImageStore:
    """Read access to the imagery store directory."""

    def __init__(self, root: Path) -> None:
        self.root: Path = root.resolve()

    def path(self, storage_key: str) -> Path:
        """The file for a key; raises ``SceneNotFound`` if it is missing or outside the store."""
        path = (self.root / storage_key).resolve()
        if not path.is_relative_to(self.root) or not path.is_file():
            raise SceneNotFound(storage_key)
        return path


def get_image_store(settings: SettingsDep) -> ImageStore:
    return ImageStore(settings.imagery_dir)


def locate_image(
    connection: psycopg.Connection, store: ImageStore, scene_id: UUID
) -> tuple[Path, str]:
    """The image file and media type of a scene; raises ``SceneNotFound``."""
    rows = fetch_all(
        connection,
        _StoredImage,
        "SELECT storage_key, media_type FROM satellite_scenes WHERE scene_id = %s",
        [scene_id],
    )
    if not rows:
        raise SceneNotFound(scene_id)
    return store.path(rows[0].storage_key), rows[0].media_type


def _unavailable() -> HTTPException:
    return HTTPException(
        status.HTTP_503_SERVICE_UNAVAILABLE, "Imagery data is currently unavailable"
    )


@router.get("/incidents/{incident_id}/imagery", response_model=IncidentImagery)
def incident_imagery(settings: SettingsDep, incident_id: UUID) -> IncidentImagery:
    """Satellite scenes over an incident, from its first to its last day of activity (UTC).

    Each scene is one tile of a daily NASA GIBS composite or of a Sentinel-2 pass (also
    shown for a few days around the activity); its image is served by
    ``GET /imagery/{scene_id}``. The list is empty until the ingestor has acquired
    imagery for the incident's area.
    """
    try:
        with connect(settings.database_url) as connection:
            return find_incident_imagery(connection, incident_id)
    except IncidentNotFound as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Incident not found") from exc
    except psycopg.Error as exc:
        LOGGER.exception("Unable to query incident imagery")
        raise _unavailable() from exc


@router.get(
    "/imagery/{scene_id}",
    response_class=FileResponse,
    responses={200: {"content": {"image/jpeg": {}, "image/webp": {}}}, 404: {}},
)
def scene_image(
    settings: SettingsDep,
    store: Annotated[ImageStore, Depends(get_image_store)],
    scene_id: UUID,
) -> FileResponse:
    """The image of one scene. Images never change, so they may be cached indefinitely."""
    try:
        with connect(settings.database_url) as connection:
            path, media_type = locate_image(connection, store, scene_id)
    except SceneNotFound as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Scene not found") from exc
    except psycopg.errors.UndefinedTable as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Scene not found") from exc
    except psycopg.Error as exc:
        LOGGER.exception("Unable to look up a satellite scene")
        raise _unavailable() from exc
    return FileResponse(path, media_type=media_type, headers={"Cache-Control": IMMUTABLE_CACHE})
