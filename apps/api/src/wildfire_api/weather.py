"""Weather forecasts from the PostGIS projection of the ``weather.forecasts`` topic.

The ingestor publishes Open-Meteo forecasts on a schedule and the sink worker keeps
the latest forecast per H3 cell in ``weather_cells`` and ``weather_hourly``; the API
only reads them and never calls the provider.
"""

import logging
from datetime import UTC, datetime
from typing import LiteralString

import psycopg
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from wildfire_api.bounds import Bounds, BoundsDep
from wildfire_api.db import connect, fetch_all
from wildfire_api.settings import SettingsDep

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


class ForecastUnavailable(LookupError):
    """No forecast has been ingested yet."""


class ForecastTimeUnavailable(LookupError):
    """The requested valid time is not in the stored forecast."""


def select_forecast_time(times: list[datetime], at: datetime | None, now: datetime) -> datetime:
    """The requested time, or by default the first forecast hour not in the past."""
    if at is None:
        return next((value for value in times if value >= now), times[-1])
    selected = at.astimezone(UTC)
    if selected not in times:
        raise ForecastTimeUnavailable(selected)
    return selected


def load_forecast(
    connection: psycopg.Connection, bounds: Bounds | None, at: datetime | None, now: datetime
) -> WeatherForecast:
    """One forecast hour over the cells that intersect ``bounds``.

    Raises ``ForecastUnavailable`` before the first forecast is stored and
    ``ForecastTimeUnavailable`` when ``at`` is not a stored valid time.
    """
    times = [
        row.valid_at
        for row in fetch_all(
            connection,
            _ValidTime,
            "SELECT DISTINCT valid_at FROM weather_hourly ORDER BY valid_at",
        )
    ]
    if not times:
        raise ForecastUnavailable
    selected_at = select_forecast_time(times, at, now)

    clauses: list[LiteralString] = ["h.valid_at = %s"]
    parameters: list[object] = [selected_at]
    if bounds is not None:
        clauses.append("c.boundary && ST_MakeEnvelope(%s, %s, %s, %s, 4326)")
        parameters.extend((bounds.west, bounds.south, bounds.east, bounds.north))
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
        "SELECT max(generated_at) AS newest, min(generated_at) AS oldest FROM weather_cells",
    )
    return WeatherForecast(
        generated_at=freshness.newest,
        oldest_generated_at=freshness.oldest,
        valid_at=selected_at,
        times=times,
        items=[row.point() for row in rows],
    )


@router.get("/weather/forecast", response_model=WeatherForecast)
def weather_forecast(
    settings: SettingsDep, bounds: BoundsDep, at: datetime | None = None
) -> WeatherForecast:
    """Hourly forecast for one valid time over the H3 forecast grid.

    ``generated_at`` is when the newest cell forecast was retrieved from Open-Meteo,
    and ``oldest_generated_at`` the oldest, so clients can show forecast freshness.
    """
    if at is not None and at.tzinfo is None:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "Forecast time must include a timezone"
        )
    try:
        with connect(settings.database_url) as connection:
            return load_forecast(connection, bounds, at, datetime.now(UTC))
    except ForecastUnavailable as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "No weather forecast has been ingested yet"
        ) from exc
    except ForecastTimeUnavailable as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "Forecast time is not available"
        ) from exc
    except psycopg.Error as exc:
        LOGGER.exception("Unable to query stored weather forecasts")
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "Weather data is currently unavailable"
        ) from exc
