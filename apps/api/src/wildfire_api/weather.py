import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from threading import Lock
from time import monotonic
from typing import Annotated
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import psycopg
from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row
from pydantic import BaseModel

LOGGER = logging.getLogger(__name__)
router = APIRouter(tags=["weather"])
_RATE_LIMIT_LOCK = Lock()
_RATE_LIMITED_UNTIL = 0.0

REGION_BOUNDS = (92.0, -12.0, 156.0, 29.0)
GRID_CELL_SIZE = 3.0
GRID_ROWS_PER_REQUEST = 100
FORECAST_DAYS = 7
REFRESH_INTERVAL = timedelta(hours=6)
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = (
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
)

CREATE_WEATHER_TABLE = """
CREATE TABLE IF NOT EXISTS weather_forecasts (
    generated_at timestamptz NOT NULL,
    valid_at timestamptz NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    west double precision NOT NULL,
    south double precision NOT NULL,
    east double precision NOT NULL,
    north double precision NOT NULL,
    temperature_c double precision,
    precipitation_mm double precision,
    wind_speed_kmh double precision,
    wind_direction_degrees double precision,
    PRIMARY KEY (valid_at, latitude, longitude)
)
"""
CREATE_WEATHER_INDEX = """
CREATE INDEX IF NOT EXISTS weather_forecasts_generated_at_idx
ON weather_forecasts (generated_at)
"""
INSERT_WEATHER = """
INSERT INTO weather_forecasts (
    generated_at, valid_at, latitude, longitude, west, south, east, north,
    temperature_c, precipitation_mm, wind_speed_kmh, wind_direction_degrees
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""

ForecastRow = tuple[
    datetime,
    datetime,
    float,
    float,
    float,
    float,
    float,
    float,
    float | None,
    float | None,
    float | None,
    float | None,
]


@dataclass(frozen=True)
class GridCell:
    latitude: float
    longitude: float
    west: float
    south: float
    east: float
    north: float


class WeatherProviderError(RuntimeError):
    def __init__(self, message: str, retry_after: int | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class WeatherPoint(BaseModel):
    latitude: float
    longitude: float
    west: float
    south: float
    east: float
    north: float
    temperature_c: float | None
    precipitation_mm: float | None
    wind_speed_kmh: float | None
    wind_direction_degrees: float | None


class WeatherForecast(BaseModel):
    generated_at: datetime
    valid_at: datetime
    times: list[datetime]
    items: list[WeatherPoint]


def grid_cells() -> list[GridCell]:
    west, south, east, north = REGION_BOUNDS
    columns = math.ceil((east - west) / GRID_CELL_SIZE)
    rows = math.ceil((north - south) / GRID_CELL_SIZE)
    cells = []
    for row in range(rows):
        cell_south = south + row * GRID_CELL_SIZE
        cell_north = min(cell_south + GRID_CELL_SIZE, north)
        for column in range(columns):
            cell_west = west + column * GRID_CELL_SIZE
            cell_east = min(cell_west + GRID_CELL_SIZE, east)
            cells.append(
                GridCell(
                    latitude=round((cell_south + cell_north) / 2, 4),
                    longitude=round((cell_west + cell_east) / 2, 4),
                    west=cell_west,
                    south=cell_south,
                    east=cell_east,
                    north=cell_north,
                )
            )
    return cells


def _utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _number(value: object) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("Forecast values must be finite numbers")
    return result


def _rate_limit_remaining() -> int:
    with _RATE_LIMIT_LOCK:
        return max(0, math.ceil(_RATE_LIMITED_UNTIL - monotonic()))


def _record_rate_limit(retry_after: int) -> None:
    global _RATE_LIMITED_UNTIL
    with _RATE_LIMIT_LOCK:
        _RATE_LIMITED_UNTIL = max(_RATE_LIMITED_UNTIL, monotonic() + retry_after)


def _retry_after_seconds(value: str | None) -> int:
    if value is None:
        return 60
    try:
        return max(1, int(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            return max(1, math.ceil((retry_at - datetime.now(UTC)).total_seconds()))
        except (TypeError, ValueError, OverflowError):
            return 60


def fetch_chunk(
    cells: list[GridCell], generated_at: datetime
) -> tuple[list[datetime], list[ForecastRow]]:
    retry_after = _rate_limit_remaining()
    if retry_after:
        raise WeatherProviderError("Open-Meteo rate limit reached", retry_after)

    parameters = {
        "latitude": ",".join(str(cell.latitude) for cell in cells),
        "longitude": ",".join(str(cell.longitude) for cell in cells),
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_days": str(FORECAST_DAYS),
        "timezone": "GMT",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    }
    url = f"{OPEN_METEO_URL}?{urlencode(parameters)}"
    try:
        with urlopen(url, timeout=30) as response:
            payload = json.loads(response.read())
    except HTTPError as exc:
        if exc.code == 429:
            retry_after = _retry_after_seconds(exc.headers.get("Retry-After"))
            _record_rate_limit(retry_after)
            LOGGER.warning("Open-Meteo rate limit reached; retry in %d seconds", retry_after)
            raise WeatherProviderError("Open-Meteo rate limit reached", retry_after) from exc
        LOGGER.warning("Open-Meteo request failed with HTTP %d", exc.code)
        raise WeatherProviderError("Open-Meteo request failed") from exc
    except (URLError, TimeoutError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        LOGGER.warning("Open-Meteo request failed: %s", type(exc).__name__)
        raise WeatherProviderError("Open-Meteo request failed") from exc

    locations = payload if isinstance(payload, list) else [payload]
    if len(locations) != len(cells):
        raise WeatherProviderError("Open-Meteo returned an unexpected number of locations")

    times: list[datetime] | None = None
    rows: list[ForecastRow] = []
    try:
        for cell, location in zip(cells, locations, strict=True):
            hourly = location["hourly"]
            location_times = [_utc_datetime(value) for value in hourly["time"]]
            if not location_times:
                raise ValueError("Forecast contains no hourly times")
            if times is None:
                times = location_times
            elif times != location_times:
                raise ValueError("Forecast locations have different timelines")

            values = [hourly[name] for name in HOURLY_VARIABLES]
            if any(len(series) != len(location_times) for series in values):
                raise ValueError("Forecast values do not match the hourly timeline")

            for index, valid_at in enumerate(location_times):
                rows.append(
                    (
                        generated_at,
                        valid_at,
                        cell.latitude,
                        cell.longitude,
                        cell.west,
                        cell.south,
                        cell.east,
                        cell.north,
                        *(_number(series[index]) for series in values),
                    )
                )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        LOGGER.warning("Open-Meteo returned invalid forecast data")
        raise WeatherProviderError("Open-Meteo returned invalid forecast data") from exc

    if times is None:
        raise WeatherProviderError("Open-Meteo returned no forecast data")
    return times, rows


def fetch_forecast(generated_at: datetime) -> tuple[list[datetime], list[ForecastRow]]:
    cells = grid_cells()
    all_times: list[datetime] | None = None
    all_rows: list[ForecastRow] = []
    for offset in range(0, len(cells), GRID_ROWS_PER_REQUEST):
        times, rows = fetch_chunk(
            cells[offset : offset + GRID_ROWS_PER_REQUEST], generated_at
        )
        if all_times is None:
            all_times = times
        elif all_times != times:
            raise WeatherProviderError("Open-Meteo returned inconsistent forecast timelines")
        all_rows.extend(rows)
    if all_times is None:
        raise WeatherProviderError("No weather grid cells are configured")
    return all_times, all_rows


@router.post("/weather/forecast", response_model=WeatherForecast)
def weather_forecast(
    west: Annotated[float | None, Query(ge=-180, le=180)] = None,
    south: Annotated[float | None, Query(ge=-90, le=90)] = None,
    east: Annotated[float | None, Query(ge=-180, le=180)] = None,
    north: Annotated[float | None, Query(ge=-90, le=90)] = None,
    at: datetime | None = None,
    refresh: bool = False,
) -> WeatherForecast:
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
            connection.execute(CREATE_WEATHER_TABLE)
            connection.execute(CREATE_WEATHER_INDEX)
            latest = connection.execute(
                "SELECT max(generated_at) AS generated_at FROM weather_forecasts"
            ).fetchone()["generated_at"]
            connection.commit()

            now = datetime.now(UTC)
            if refresh or latest is None or latest < now - REFRESH_INTERVAL:
                generated_at = now
                _, records = fetch_forecast(generated_at)
                with connection.transaction():
                    connection.execute("DELETE FROM weather_forecasts")
                    with connection.cursor() as cursor:
                        cursor.executemany(INSERT_WEATHER, records)

            times = [
                row["valid_at"]
                for row in connection.execute(
                    "SELECT DISTINCT valid_at FROM weather_forecasts ORDER BY valid_at"
                ).fetchall()
            ]
            if not times:
                raise HTTPException(503, "Weather forecast is currently unavailable")

            selected_at = at.astimezone(UTC) if at is not None else next(
                (value for value in times if value >= now), times[-1]
            )
            if selected_at not in times:
                raise HTTPException(422, "Forecast time is not available")

            clauses = ["valid_at = %s"]
            parameters: list[object] = [selected_at]
            if west is not None and south is not None and east is not None and north is not None:
                clauses.extend(["east > %s", "west < %s", "north > %s", "south < %s"])
                parameters.extend((west, east, south, north))
            items = connection.execute(
                """
                SELECT latitude, longitude, west, south, east, north, temperature_c,
                       precipitation_mm, wind_speed_kmh, wind_direction_degrees
                FROM weather_forecasts
                WHERE """
                + " AND ".join(clauses)
                + " ORDER BY latitude, longitude",
                parameters,
            ).fetchall()
            generated_at = connection.execute(
                "SELECT max(generated_at) AS generated_at FROM weather_forecasts"
            ).fetchone()["generated_at"]
    except HTTPException:
        raise
    except WeatherProviderError as exc:
        LOGGER.warning("Unable to load Open-Meteo forecast: %s", exc)
        if exc.retry_after is not None:
            detail = f"Open-Meteo rate limit reached; retry in {exc.retry_after} seconds."
            raise HTTPException(503, detail, headers={"Retry-After": str(exc.retry_after)}) from exc
        raise HTTPException(503, "Weather forecast is currently unavailable") from exc
    except psycopg.Error as exc:
        LOGGER.exception("Unable to query stored weather forecasts")
        raise HTTPException(503, "Weather data is currently unavailable") from exc

    return WeatherForecast(
        generated_at=generated_at,
        valid_at=selected_at,
        times=times,
        items=[WeatherPoint.model_validate(item) for item in items],
    )
