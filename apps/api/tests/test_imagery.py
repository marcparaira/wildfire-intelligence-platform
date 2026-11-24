import uuid
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import httpx
import psycopg
import pytest
from fastapi.testclient import TestClient

from wildfire_api.imagery import ImageStore, IncidentImagery, SceneNotFound
from wildfire_api.main import app
from wildfire_events.cells import partition_cell
from wildfire_events.models import SENTINEL_CONTEXT_DAYS

client: httpx.Client = TestClient(app)
INCIDENT = uuid.UUID("00000000-0000-0000-0000-0000000000a1")
TRUE_COLOR = "VIIRS_NOAA20_CorrectedReflectance_TrueColor"
FALSE_COLOR = "VIIRS_NOAA20_CorrectedReflectance_BandsM11-I2-I1"
SWIR = "Sentinel-2_L2A_SWIR_B12-B8A-B04"
JPEG = b"\xff\xd8\xff\xe0 not really a jpeg"


def test_image_store_only_serves_files_inside_its_root(tmp_path: Path) -> None:
    root = tmp_path / "imagery"
    (root / "layer").mkdir(parents=True)
    _ = (root / "layer" / "a.jpg").write_bytes(JPEG)
    _ = (tmp_path / "secret.txt").write_text("secret")
    store = ImageStore(root)
    assert store.path("layer/a.jpg").read_bytes() == JPEG
    for key in ("layer/missing.jpg", "../secret.txt", "layer"):
        with pytest.raises(SceneNotFound):
            _ = store.path(key)


@pytest.mark.parametrize("path", ["/imagery/not-a-uuid", "/incidents/not-a-uuid/imagery"])
def test_invalid_ids_are_rejected(path: str) -> None:
    assert client.get(path).status_code == 422


def insert_incident(db: psycopg.Connection, last_observed_at: datetime) -> None:
    """An incident from 2026-03-05 01:00 UTC around (1.2, 114.2), 0.02 degrees across."""
    _ = db.execute(
        """
        INSERT INTO fire_incidents (
            incident_id, revision, status, h3_cell, first_observed_at, last_observed_at,
            updated_at, detection_count, footprint_cells, footprint_km2, latitude, longitude,
            location, west, south, east, north, total_frp_mw, products, daily
        ) VALUES (
            %(id)s, 1, 'growing', %(cell)s, %(first)s, %(last)s, %(last)s, 3, 1, 5.0,
            1.2, 114.2, ST_SetSRID(ST_MakePoint(114.2, 1.2), 4326),
            114.19, 1.19, 114.21, 1.21, 0, ARRAY[]::text[], '[]'
        )
        """,
        {
            "id": INCIDENT,
            "cell": partition_cell(1.2, 114.2),
            "first": datetime(2026, 3, 5, 1, tzinfo=UTC),
            "last": last_observed_at,
        },
    )


def insert_scene(
    db: psycopg.Connection,
    layer: str,
    day: date,
    bounds: tuple[float, float, float, float],
    storage_key: str = "layer/scene.jpg",
) -> uuid.UUID:
    scene_id = uuid.uuid4()
    west, south, east, north = bounds
    sentinel = layer == SWIR
    _ = db.execute(
        """
        INSERT INTO satellite_scenes (
            scene_id, provider, layer, instrument, platform, acquisition_date, tile,
            west, south, east, north, footprint, width, height, media_type, byte_size,
            sha256, storage_key, source_url, retrieved_at, acquired_at, cloud_cover
        ) VALUES (
            %(id)s, %(provider)s, %(layer)s, %(instrument)s, %(platform)s, %(day)s, 'x0y0',
            %(w)s, %(s)s, %(e)s, %(n)s, ST_MakeEnvelope(%(w)s, %(s)s, %(e)s, %(n)s, 4326),
            512, 512, 'image/jpeg', 10, %(sha)s, %(key)s, 'https://gibs.example', %(at)s,
            %(acquired)s, %(cloud)s
        )
        """,
        {
            "id": scene_id,
            "provider": "planetary-computer" if sentinel else "nasa-gibs",
            "instrument": "MSI" if sentinel else "VIIRS",
            "platform": "Sentinel-2B" if sentinel else "NOAA-20",
            "acquired": datetime.combine(day, time(2, 13), tzinfo=UTC) if sentinel else None,
            "cloud": 12.5 if sentinel else None,
            "layer": layer,
            "day": day,
            "w": west,
            "s": south,
            "e": east,
            "n": north,
            "sha": "0" * 64,
            "key": storage_key,
            "at": datetime(2026, 3, 7, tzinfo=UTC),
        },
    )
    return scene_id


def test_incident_imagery_lists_scenes_over_its_area_and_days(
    database: psycopg.Connection,
) -> None:
    insert_incident(database, datetime(2026, 3, 6, 8, tzinfo=UTC))
    tile = (114.0, 1.0, 114.5, 1.5)
    second_day = insert_scene(database, FALSE_COLOR, date(2026, 3, 6), tile)
    first_day = insert_scene(database, TRUE_COLOR, date(2026, 3, 5), tile)
    _ = insert_scene(database, TRUE_COLOR, date(2026, 3, 4), tile)  # before the incident
    _ = insert_scene(database, TRUE_COLOR, date(2026, 3, 5), (120.0, 1.0, 120.5, 1.5))  # far

    response = client.get(f"/incidents/{INCIDENT}/imagery")
    assert response.status_code == 200
    imagery = IncidentImagery.model_validate_json(response.content)
    assert (imagery.first_day, imagery.last_day) == (date(2026, 3, 5), date(2026, 3, 6))
    assert [scene.scene_id for scene in imagery.scenes] == [first_day, second_day]
    assert imagery.scenes[0].bbox == tile
    assert imagery.scenes[0].platform == "NOAA-20"

    assert client.get(f"/incidents/{uuid.uuid4()}/imagery").status_code == 404


def test_incident_imagery_includes_sentinel_passes_around_the_activity(
    database: psycopg.Connection,
) -> None:
    insert_incident(database, datetime(2026, 3, 6, 8, tzinfo=UTC))
    tile = (114.0, 1.0, 114.5, 1.5)
    context = timedelta(days=SENTINEL_CONTEXT_DAYS)
    before = insert_scene(database, SWIR, date(2026, 3, 5) - context, tile)
    after = insert_scene(database, SWIR, date(2026, 3, 6) + context, tile)
    _ = insert_scene(database, SWIR, date(2026, 3, 6) + context + timedelta(days=1), tile)
    _ = insert_scene(database, TRUE_COLOR, date(2026, 3, 4), tile)  # daily: activity only

    response = client.get(f"/incidents/{INCIDENT}/imagery")
    imagery = IncidentImagery.model_validate_json(response.content)
    assert [scene.scene_id for scene in imagery.scenes] == [before, after]
    assert imagery.scenes[0].acquired_at == datetime(2026, 2, 27, 2, 13, tzinfo=UTC)
    assert imagery.scenes[0].cloud_cover == 12.5
    assert imagery.scenes[0].platform == "Sentinel-2B"


def test_incident_imagery_is_empty_before_the_projection_exists(
    database: psycopg.Connection,
) -> None:
    insert_incident(database, datetime(2026, 3, 6, tzinfo=UTC))
    _ = database.execute("DROP TABLE satellite_scenes")
    response = client.get(f"/incidents/{INCIDENT}/imagery")
    assert response.status_code == 200
    assert IncidentImagery.model_validate_json(response.content).scenes == []


def test_scene_image_is_served_from_the_store(
    database: psycopg.Connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("IMAGERY_DIR", str(tmp_path))
    (tmp_path / "layer").mkdir()
    _ = (tmp_path / "layer" / "scene.jpg").write_bytes(JPEG)
    stored = insert_scene(database, TRUE_COLOR, date(2026, 3, 5), (114.0, 1.0, 114.5, 1.5))
    missing_file = insert_scene(
        database, TRUE_COLOR, date(2026, 3, 6), (114.0, 1.0, 114.5, 1.5), "layer/gone.jpg"
    )

    response = client.get(f"/imagery/{stored}")
    assert response.status_code == 200
    assert response.content == JPEG
    assert response.headers["content-type"] == "image/jpeg"
    assert "immutable" in response.headers["cache-control"]
    assert client.get(f"/imagery/{missing_file}").status_code == 404
    assert client.get(f"/imagery/{uuid.uuid4()}").status_code == 404
