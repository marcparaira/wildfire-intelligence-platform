"""Weather forecasts from the PostGIS projection of the ``weather.forecasts`` topic.

The ingestor publishes Open-Meteo forecasts on a schedule and the sink worker keeps
the latest forecast per H3 cell in ``weather_cells`` and ``weather_hourly``; the API
only reads them and never calls the provider.
"""

import json
import logging
import os
from datetime import UTC, datetime
from typing import Annotated

import psycopg
from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row
from pydantic import BaseModel

LOGGER = logging.getLogger(__name__)
router = APIRouter(tags=["weather"])


class WeatherPoint(BaseModel):
    cell: str
    latitude: float
    longitude: float
    boundary: list[tuple[float, float]]
    generated_at: datetime
    temperature_c: float | None
    precipitation_mm: float | None
    wind_speed_kmh: float | None
    wind_direction_degrees: float | None


class WeatherForecast(BaseModel):
    generated_at: datetime
    oldest_generated_at: datetime
    valid_at: datetime
    times: list[datetime]
    items: list[WeatherPoint]


@router.get("/weather/forecast", response_model=WeatherForecast)
def weather_forecast(
    west: Annotated[float | None, Query(ge=-180, le=180)] = None,
    south: Annotated[float | None, Query(ge=-90, le=90)] = None,
    east: Annotated[float | None, Query(ge=-180, le=180)] = None,
    north: Annotated[float | None, Query(ge=-90, le=90)] = None,
    at: datetime | None = None,
) -> WeatherForecast:
    """Hourly forecast for one valid time over the H3 forecast grid.

    ``generated_at`` is when the newest cell forecast was retrieved from Open-Meteo,
    and ``oldest_generated_at`` the oldest, so clients can show forecast freshness.
    """
    bounds = (west, south, east, north)
    if any(value is not None for value in bounds):
        if west is None or south is None or east is None or north is None:
            raise HTTPException(422, "All four geographic bounds are required")
        if west >= east or south >= north:
            raise HTTPException(422, "West must be less than east and south less than north")
    if at is not None and at.tzinfo is None:
        raise HTTPException(422, "Forecast time must include a timezone")

    database_url = os.environ.get(
        "DATABASE_URL", "postgresql://wildfire:wildfire_dev@localhost:55432/wildfire"
    )
    try:
        with psycopg.connect(database_url, row_factory=dict_row, connect_timeout=5) as connection:
            times = [
                row["valid_at"]
                for row in connection.execute(
                    "SELECT DISTINCT valid_at FROM weather_hourly ORDER BY valid_at"
                ).fetchall()
            ]
            if not times:
                raise HTTPException(503, "No weather forecast has been ingested yet")

            now = datetime.now(UTC)
            selected_at = at.astimezone(UTC) if at is not None else next(
                (value for value in times if value >= now), times[-1]
            )
            if selected_at not in times:
                raise HTTPException(422, "Forecast time is not available")

            clauses = ["h.valid_at = %s"]
            parameters: list[object] = [selected_at]
            if west is not None and south is not None and east is not None and north is not None:
                clauses.append("c.boundary && ST_MakeEnvelope(%s, %s, %s, %s, 4326)")
                parameters.extend((west, south, east, north))
            rows = connection.execute(
                """
                SELECT c.cell, c.latitude, c.longitude, ST_AsGeoJSON(c.boundary) AS boundary,
                       c.generated_at, h.temperature_c, h.precipitation_mm, h.wind_speed_kmh,
                       h.wind_direction_degrees
                FROM weather_hourly AS h JOIN weather_cells AS c USING (cell)
                WHERE """
                + " AND ".join(clauses)
                + " ORDER BY c.latitude, c.longitude",
                parameters,
            ).fetchall()
            freshness = connection.execute(
                "SELECT max(generated_at) AS newest, min(generated_at) AS oldest "
                "FROM weather_cells"
            ).fetchone()
    except HTTPException:
        raise
    except psycopg.Error as exc:
        LOGGER.exception("Unable to query stored weather forecasts")
        raise HTTPException(503, "Weather data is currently unavailable") from exc

    items = []
    for row in rows:
        row["boundary"] = [tuple(point) for point in json.loads(row["boundary"])["coordinates"][0]]
        items.append(WeatherPoint.model_validate(row))
    return WeatherForecast(
        generated_at=freshness["newest"],
        oldest_generated_at=freshness["oldest"],
        valid_at=selected_at,
        times=times,
        items=items,
    )
