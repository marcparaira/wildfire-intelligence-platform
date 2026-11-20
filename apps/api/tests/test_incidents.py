import json
import math
import uuid
from collections.abc import Mapping
from datetime import UTC, date, datetime

import httpx
import psycopg
import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel

from wildfire_api.incidents import (
    IncidentDetail,
    IncidentResults,
    RecordedDay,
    growth,
    load_incident,
    weather_window,
)
from wildfire_api.main import app
from wildfire_events.cells import partition_cell
from wildfire_events.models import IncidentStatus, RiskLevel

client: httpx.Client = TestClient(app)
NOW = datetime(2026, 3, 6, 10, 25, tzinfo=UTC)
GROWING = uuid.UUID("00000000-0000-0000-0000-00000000000a")
NEW_FAR = uuid.UUID("00000000-0000-0000-0000-00000000000b")
STABLE = uuid.UUID("00000000-0000-0000-0000-00000000000c")
INACTIVE = uuid.UUID("00000000-0000-0000-0000-00000000000d")
BORNEO = {"west": 108, "south": -5, "east": 120, "north": 8}


class ErrorBody(BaseModel):
    detail: str


def test_growth_accumulates_the_footprint_in_day_order() -> None:
    days = [
        RecordedDay(day=date(2026, 3, 6), detections=4, new_cells=1),
        RecordedDay(day=date(2026, 3, 5), detections=3, new_cells=2, max_frp_mw=10.0),
    ]
    result = growth(days, footprint_cells=3, footprint_km2=15.6)
    assert [(day.day, day.footprint_cells) for day in result] == [
        (date(2026, 3, 5), 2),
        (date(2026, 3, 6), 3),
    ]
    assert math.isclose(result[0].footprint_km2, 10.4)
    assert math.isclose(result[-1].footprint_km2, 15.6)
    assert result[0].max_frp_mw == 10.0
    assert growth([], footprint_cells=1, footprint_km2=5.2) == []


def test_weather_window_starts_at_the_current_hour_and_spans_a_day() -> None:
    start, end = weather_window(NOW)
    assert start == datetime(2026, 3, 6, 10, tzinfo=UTC)
    assert end == datetime(2026, 3, 7, 10, tzinfo=UTC)
    assert weather_window(start) == (start, end)


@pytest.mark.parametrize(
    "path",
    [
        "/incidents?status=burning",
        "/incidents?limit=0",
        "/incidents?limit=2001",
        "/incidents?west=108",
        "/incidents?west=120&south=-5&east=108&north=8",
        "/incidents/not-a-uuid",
    ],
)
def test_invalid_requests_are_rejected(path: str) -> None:
    assert client.get(path).status_code == 422


@pytest.mark.parametrize("path", ["/incidents", f"/incidents/{GROWING}"])
def test_database_failure_is_explicit(monkeypatch: pytest.MonkeyPatch, path: str) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost:1/unavailable")
    response = client.get(path)
    assert response.status_code == 503
    assert ErrorBody.model_validate_json(response.content).detail == (
        "Incident data is currently unavailable"
    )


def insert_incident(
    db: psycopg.Connection,
    incident_id: uuid.UUID,
    status: IncidentStatus,
    last_observed_at: datetime,
    position: tuple[float, float],
    daily: list[dict[str, object]] | None = None,
) -> None:
    latitude, longitude = position
    _ = db.execute(
        """
        INSERT INTO fire_incidents (
            incident_id, revision, status, previous_status, merged_into, h3_cell,
            first_observed_at, last_observed_at, updated_at, detection_count, footprint_cells,
            footprint_km2, latitude, longitude, location, west, south, east, north, max_frp_mw,
            total_frp_mw, products, daily
        ) VALUES (
            %(id)s, 2, %(status)s, 'new', NULL, %(cell)s, %(first)s, %(last)s, %(last)s, 7, 3,
            15.6, %(lat)s, %(lon)s, ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326),
            %(lon)s - 0.01, %(lat)s - 0.01, %(lon)s + 0.01, %(lat)s + 0.01, 42.0, 80.5,
            ARRAY['VIIRS_NOAA20_NRT', 'MODIS_NRT'], %(daily)s
        )
        """,
        {
            "id": incident_id,
            "status": str(status),
            "cell": partition_cell(latitude, longitude),
            "first": datetime(2026, 3, 5, 1, tzinfo=UTC),
            "last": last_observed_at,
            "lat": latitude,
            "lon": longitude,
            "daily": json.dumps(daily or []),
        },
    )


def insert_detection(
    db: psycopg.Connection,
    row_id: int,
    position: tuple[float, float],
    observed_at: datetime,
    risk: tuple[int, str] | None,
) -> None:
    latitude, longitude = position
    event_id = uuid.uuid4()
    _ = db.execute(
        """
        INSERT INTO fire_detections VALUES (
            %s, %s, 'VIIRS_NOAA20_NRT', 'VIIRS', 'N20', %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326), '{}', NULL
        )
        """,
        (row_id, event_id, observed_at, latitude, longitude, longitude, latitude),
    )
    score, level = risk if risk is not None else (None, None)
    _ = db.execute(
        """
        INSERT INTO detection_conditions (event_id, h3_cell, observed_at, country, risk_score,
                                          risk_level)
        VALUES (%s, %s, %s, 'IDN', %s, %s)
        """,
        (event_id, partition_cell(latitude, longitude), observed_at, score, level),
    )


def seed(db: psycopg.Connection) -> None:
    insert_incident(
        db,
        GROWING,
        IncidentStatus.GROWING,
        datetime(2026, 3, 6, 6, tzinfo=UTC),
        (1.0, 114.0),
        daily=[
            {"day": "2026-03-06", "detections": 4, "new_cells": 1, "max_frp_mw": None},
            {"day": "2026-03-05", "detections": 3, "new_cells": 2, "max_frp_mw": 42.0},
        ],
    )
    insert_incident(
        db, NEW_FAR, IncidentStatus.NEW, datetime(2026, 3, 6, 6, tzinfo=UTC), (20.0, 100.0)
    )
    insert_incident(
        db, STABLE, IncidentStatus.STABLE, datetime(2026, 3, 5, 12, tzinfo=UTC), (2.0, 115.0)
    )
    insert_incident(
        db, INACTIVE, IncidentStatus.INACTIVE, datetime(2026, 3, 6, 8, tzinfo=UTC), (1.5, 114.5)
    )
    # Peak-risk candidates around GROWING (bbox 113.99-114.01, 0.99-1.01):
    insert_detection(db, 1, (1.0, 114.0), datetime(2026, 3, 6, 5, tzinfo=UTC), (5, "moderate"))
    # within the 0.01° margin: the peak
    insert_detection(db, 2, (1.0, 114.015), datetime(2026, 3, 6, 3, tzinfo=UTC), (8, "high"))
    # before the incident's last 24 hours
    insert_detection(db, 3, (1.0, 114.0), datetime(2026, 3, 5, 5, tzinfo=UTC), (11, "extreme"))
    # outside the box and margin
    insert_detection(db, 4, (1.0, 114.03), datetime(2026, 3, 6, 4, tzinfo=UTC), (10, "extreme"))
    # enriched without a risk
    insert_detection(db, 5, (1.0, 114.0), datetime(2026, 3, 6, 5, tzinfo=UTC), None)
    cell = partition_cell(1.0, 114.0)
    _ = db.execute(
        """
        INSERT INTO weather_cells VALUES (
            %s, 1, 114, ST_GeomFromText('POLYGON((113 0,115 0,115 2,113 2,113 0))', 4326),
            'open-meteo', 'best_match', '2026-03-06 06:00:00+00'
        )
        """,
        (cell,),
    )
    for valid_at in [
        "2026-03-06 09:00:00+00",  # before the current hour
        "2026-03-06 10:00:00+00",
        "2026-03-06 22:00:00+00",
        "2026-03-07 10:00:00+00",  # 24 hours later: included
        "2026-03-07 11:00:00+00",
    ]:
        _ = db.execute(
            "INSERT INTO weather_hourly VALUES (%s, %s, 31.0, 0.0, 14.0, 90.0)", (cell, valid_at)
        )


def incidents(params: Mapping[str, str | int | list[str]]) -> IncidentResults:
    response = client.get("/incidents", params=params)
    assert response.status_code == 200
    return IncidentResults.model_validate_json(response.content)


def test_incident_list_filters_orders_and_reports_peak_risk(database: psycopg.Connection) -> None:
    seed(database)
    active = incidents({})
    assert [item.incident_id for item in active.items] == [GROWING, NEW_FAR, STABLE]
    assert active.truncated is False
    growing = active.items[0]
    assert growing.peak_risk is not None
    assert (growing.peak_risk.score, growing.peak_risk.level) == (8, RiskLevel.HIGH)
    assert [round(edge, 6) for edge in growing.bbox] == [113.99, 0.99, 114.01, 1.01]
    assert [str(product) for product in growing.products] == ["VIIRS_NOAA20_NRT", "MODIS_NRT"]
    assert active.items[2].peak_risk is None

    assert [item.incident_id for item in incidents(BORNEO).items] == [GROWING, STABLE]
    limited = incidents({"limit": 1})
    assert [item.incident_id for item in limited.items] == [GROWING]
    assert limited.truncated is True
    final = incidents({"status": ["inactive", "stable"]})
    assert [item.incident_id for item in final.items] == [INACTIVE, STABLE]


def test_incident_detail_has_growth_and_weather_context(database: psycopg.Connection) -> None:
    seed(database)
    detail = load_incident(database, GROWING, NOW)
    assert [(day.day, day.footprint_cells) for day in detail.daily] == [
        (date(2026, 3, 5), 2),
        (date(2026, 3, 6), 3),
    ]
    assert detail.weather is not None
    assert detail.weather.cell == partition_cell(1.0, 114.0)
    assert detail.weather.generated_at == datetime(2026, 3, 6, 6, tzinfo=UTC)
    assert [hour.valid_at for hour in detail.weather.hours] == [
        datetime(2026, 3, 6, 10, tzinfo=UTC),
        datetime(2026, 3, 6, 22, tzinfo=UTC),
        datetime(2026, 3, 7, 10, tzinfo=UTC),
    ]
    assert load_incident(database, NEW_FAR, NOW).weather is None

    response = client.get(f"/incidents/{INACTIVE}")
    assert response.status_code == 200
    body = IncidentDetail.model_validate_json(response.content)
    assert body.status is IncidentStatus.INACTIVE
    assert body.daily == []

    missing = client.get(f"/incidents/{uuid.uuid4()}")
    assert missing.status_code == 404
    assert ErrorBody.model_validate_json(missing.content).detail == "Incident not found"
