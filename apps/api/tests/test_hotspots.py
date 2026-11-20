from datetime import UTC, datetime

import httpx
import psycopg
import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel

from wildfire_api.hotspots import HotspotResults
from wildfire_api.main import app
from wildfire_events.models import RiskLevel

client: httpx.Client = TestClient(app)


class ErrorBody(BaseModel):
    detail: str


def hotspots(params: dict[str, str | int]) -> HotspotResults:
    response = client.get("/hotspots", params=params)
    assert response.status_code == 200
    return HotspotResults.model_validate_json(response.content)


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
    assert ErrorBody.model_validate_json(response.content).detail == (
        "Hotspot data is currently unavailable"
    )


def test_hotspot_filters_dates_geometry_and_truncation(database: psycopg.Connection) -> None:
    db = database
    _ = db.execute(
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
    _ = db.execute(
        """
        INSERT INTO detection_conditions (
            event_id, h3_cell, observed_at, country, weather_valid_at, forecast_generated_at,
            temperature_c, precipitation_mm, precipitation_24h_mm, wind_speed_kmh,
            wind_direction_degrees, risk_score, risk_level, wind_points, temperature_points,
            dryness_points, intensity_points
        )
        SELECT event_id, '826527fffffffff', observed_at, 'MYS', '2026-03-05 00:00:00+00',
               '2026-03-04 21:00:00+00', 31.5, 0, 0.1, 12, 270, 7, 'high', 1, 2, 3, 1
        FROM fire_detections WHERE id = 1
        """
    )
    _ = db.execute(
        """
        INSERT INTO detection_conditions (event_id, h3_cell, observed_at, country)
        SELECT event_id, '826527fffffffff', observed_at, 'MYS' FROM fire_detections WHERE id = 2
        """
    )
    params: dict[str, str | int] = {
        "west": 108,
        "south": -5,
        "east": 120,
        "north": 8,
        "date_from": "2026-03-05",
        "date_to": "2026-03-05",
    }
    result = hotspots(params)
    assert [item.id for item in result.items] == [2, 1]
    assert result.truncated is False
    assert result.items[1].attributes["frp"] == "12.5"
    assert result.items[0].observed_at == datetime(2026, 3, 5, 23, 59, 59, tzinfo=UTC)
    limited = hotspots({**params, "limit": 1})
    assert len(limited.items) == 1
    assert limited.truncated is True
    with_duplicates = hotspots({**params, "include_duplicates": "true"})
    assert [item.id for item in with_duplicates.items] == [2, 5, 1]
    assert with_duplicates.items[1].duplicate_of is not None
    modis = hotspots({**params, "include_duplicates": "true", "product": "MODIS_NRT"})
    assert [item.id for item in modis.items] == [5]
    assert hotspots({"date_to": "2026-03-04"}) == HotspotResults(items=[], truncated=False)
    enriched, unenriched = result.items[1], result.items[0]
    assert enriched.country == "MYS"
    assert enriched.risk is not None
    assert enriched.risk.level is RiskLevel.HIGH
    assert (enriched.risk.score, enriched.risk.dryness_points) == (7, 3)
    assert enriched.weather is not None
    assert enriched.weather.valid_at == datetime(2026, 3, 5, tzinfo=UTC)
    assert enriched.weather.precipitation_24h_mm == 0.1
    # Enriched without weather: the country is known, risk and weather are not.
    assert (unenriched.country, unenriched.risk, unenriched.weather) == ("MYS", None, None)
    # Not enriched at all.
    assert modis.items[0].country is None
