import os
import uuid
from datetime import UTC, datetime

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo

from wildfire_api.main import app

client = TestClient(app)


@pytest.mark.parametrize(
    "query",
    [
        "?west=108",
        "?west=120&south=-5&east=108&north=8",
        "?west=108&south=8&east=120&north=-5",
        "?west=-181&south=-5&east=120&north=8",
        "?date_from=2026-03-06&date_to=2026-03-05",
        "?date_from=not-a-date",
        "?limit=0",
        "?product=LANDSAT",
    ],
)
def test_invalid_filters_are_rejected(query: str) -> None:
    assert client.get(f"/hotspots{query}").status_code == 422


def test_database_failure_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost:1/unavailable")
    response = client.get("/hotspots")
    assert response.status_code == 503
    assert response.json()["detail"] == "Hotspot data is currently unavailable"


@pytest.mark.skipif(
    not os.environ.get("API_TEST_DATABASE_URL"),
    reason="set API_TEST_DATABASE_URL to run PostGIS integration test",
)
def test_hotspot_filters_dates_geometry_and_truncation(monkeypatch: pytest.MonkeyPatch) -> None:
    database_url = os.environ["API_TEST_DATABASE_URL"]
    schema = f"api_test_{uuid.uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as db:
        db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            db.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
            db.execute(
                """
                CREATE TABLE fire_detections (
                    id bigint PRIMARY KEY, event_id uuid NOT NULL, product text NOT NULL,
                    instrument text NOT NULL, satellite text NOT NULL,
                    observed_at timestamptz NOT NULL, latitude double precision NOT NULL,
                    longitude double precision NOT NULL, location geometry(Point, 4326) NOT NULL,
                    attributes jsonb NOT NULL, duplicate_of uuid
                )
                """
            )
            db.execute(
                """
                INSERT INTO fire_detections VALUES
                (1, gen_random_uuid(), 'VIIRS_NOAA20_NRT', 'VIIRS', 'N20',
                 '2026-03-05 00:00:00+00', 1, 114,
                 ST_SetSRID(ST_MakePoint(114, 1), 4326), '{"frp":"12.5"}', NULL),
                (2, gen_random_uuid(), 'VIIRS_NOAA20_NRT', 'VIIRS', 'N20',
                 '2026-03-05 23:59:59+00', 2, 115,
                 ST_SetSRID(ST_MakePoint(115, 2), 4326), '{"confidence":"nominal"}', NULL),
                (3, gen_random_uuid(), 'VIIRS_NOAA20_NRT', 'VIIRS', 'N20',
                 '2026-03-06 00:00:00+00', 3, 116,
                 ST_SetSRID(ST_MakePoint(116, 3), 4326), '{}', NULL),
                (4, gen_random_uuid(), 'VIIRS_NOAA20_NRT', 'VIIRS', 'N20',
                 '2026-03-05 12:00:00+00', 10, 100,
                 ST_SetSRID(ST_MakePoint(100, 10), 4326), '{}', NULL),
                (5, gen_random_uuid(), 'MODIS_NRT', 'MODIS', 'Aqua',
                 '2026-03-05 00:04:00+00', 1.001, 114.001,
                 ST_SetSRID(ST_MakePoint(114.001, 1.001), 4326), '{}',
                 '00000000-0000-0000-0000-000000000001')
                """
            )
            monkeypatch.setenv(
                "DATABASE_URL",
                make_conninfo(database_url, options=f"-csearch_path={schema},public"),
            )
            params = {
                "west": 108,
                "south": -5,
                "east": 120,
                "north": 8,
                "date_from": "2026-03-05",
                "date_to": "2026-03-05",
            }
            response = client.get("/hotspots", params=params)
            assert response.status_code == 200
            result = response.json()
            assert [item["id"] for item in result["items"]] == [2, 1]
            assert result["truncated"] is False
            assert result["items"][1]["attributes"]["frp"] == "12.5"
            assert datetime.fromisoformat(result["items"][0]["observed_at"]) == datetime(
                2026, 3, 5, 23, 59, 59, tzinfo=UTC
            )
            limited = client.get("/hotspots", params={**params, "limit": 1}).json()
            assert len(limited["items"]) == 1
            assert limited["truncated"] is True
            with_duplicates = client.get(
                "/hotspots", params={**params, "include_duplicates": "true"}
            ).json()
            assert [item["id"] for item in with_duplicates["items"]] == [2, 5, 1]
            assert with_duplicates["items"][1]["duplicate_of"] is not None
            modis = client.get(
                "/hotspots",
                params={**params, "include_duplicates": "true", "product": "MODIS_NRT"},
            ).json()
            assert [item["id"] for item in modis["items"]] == [5]
            assert client.get("/hotspots", params={"date_to": "2026-03-04"}).json() == {
                "items": [],
                "truncated": False,
            }
        finally:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
