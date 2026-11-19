"""Weather forecasts from the PostGIS projection of the ``weather.forecasts`` topic.

The ingestor publishes Open-Meteo forecasts on a schedule and the sink worker keeps
the latest forecast per H3 cell in ``weather_cells`` and ``weather_hourly``; the API
only reads them and never calls the provider.
"""

import logging
from datetime import UTC, datetime
from typing import Annotated, LiteralString

import psycopg
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from wildfire_api.db import connect, fetch_all

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


class _ValidTime(BaseModel):
    valid_at: datetime


class _Polygon(BaseModel):
    coordinates: list[list[tuple[float, float]]]


class _CellRow(BaseModel):
    cell: str
    latitude: float
    longitude: float
    boundary: str  # GeoJSON polygon
    generated_at: datetime
    temperature_c: float | None
    precipitation_mm: float | None
    wind_speed_kmh: float | None
    wind_direction_degrees: float | None

    def point(self) -> WeatherPoint:
        return WeatherPoint(
            cell=self.cell,
            latitude=self.latitude,
            longitude=self.longitude,
            boundary=_Polygon.model_validate_json(self.boundary).coordinates[0],
            generated_at=self.generated_at,
            temperature_c=self.temperature_c,
            precipitation_mm=self.precipitation_mm,
            wind_speed_kmh=self.wind_speed_kmh,
            wind_direction_degrees=self.wind_direction_degrees,
        )


class _Freshness(BaseModel):
    newest: datetime
    oldest: datetime


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

    try:
        with connect() as connection:
            times = [
                row.valid_at
                for row in fetch_all(
                    connection,
                    _ValidTime,
                    "SELECT DISTINCT valid_at FROM weather_hourly ORDER BY valid_at",
                )
            ]
            if not times:
                raise HTTPException(503, "No weather forecast has been ingested yet")

            now = datetime.now(UTC)
            selected_at = (
                at.astimezone(UTC)
                if at is not None
                else next((value for value in times if value >= now), times[-1])
            )
            if selected_at not in times:
                raise HTTPException(422, "Forecast time is not available")

            clauses: list[LiteralString] = ["h.valid_at = %s"]
            parameters: list[object] = [selected_at]
            if west is not None and south is not None and east is not None and north is not None:
                clauses.append("c.boundary && ST_MakeEnvelope(%s, %s, %s, %s, 4326)")
                parameters.extend((west, south, east, north))
            rows = fetch_all(
                connection,
                _CellRow,
                f"""
                SELECT c.cell, c.latitude, c.longitude, ST_AsGeoJSON(c.boundary) AS boundary,
                       c.generated_at, h.temperature_c, h.precipitation_mm, h.wind_speed_kmh,
                       h.wind_direction_degrees
                FROM weather_hourly AS h JOIN weather_cells AS c USING (cell)
                WHERE {" AND ".join(clauses)}
                ORDER BY c.latitude, c.longitude
                """,
                parameters,
            )
            # weather_hourly references weather_cells, so a forecast time implies cells.
            [freshness] = fetch_all(
                connection,
                _Freshness,
                "SELECT max(generated_at) AS newest, min(generated_at) AS oldest "
                + "FROM weather_cells",
            )
    except HTTPException:
        raise
    except psycopg.Error as exc:
        LOGGER.exception("Unable to query stored weather forecasts")
        raise HTTPException(503, "Weather data is currently unavailable") from exc

    return WeatherForecast(
        generated_at=freshness.newest,
        oldest_generated_at=freshness.oldest,
        valid_at=selected_at,
        times=times,
        items=[row.point() for row in rows],
    )
