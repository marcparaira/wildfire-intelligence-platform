"""Open-Meteo hourly forecast producer for the H3 forecast grid."""

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from functools import cache
from http.client import HTTPResponse
from typing import cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import h3
from pydantic import BaseModel, TypeAdapter, ValidationError

from wildfire_events.cells import cells_covering
from wildfire_events.models import HourlyWeather, WeatherForecast
from wildfire_ingestor.countries import REGION_BOUNDS
from wildfire_ingestor.publisher import Publisher
from wildfire_ingestor.scheduler import RetryLater

LOGGER = logging.getLogger(__name__)
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
LOCATIONS_PER_REQUEST = 100
FORECAST_DAYS = 7
MODEL = "best_match"
HOURLY_VARIABLES = ("temperature_2m", "precipitation", "wind_speed_10m", "wind_direction_10m")


@dataclass(frozen=True)
class ForecastCell:
    cell: str
    latitude: float
    longitude: float


class OpenMeteoHourly(BaseModel):
    time: list[str]
    temperature_2m: list[float | None]
    precipitation: list[float | None]
    wind_speed_10m: list[float | None]
    wind_direction_10m: list[float | None]


class OpenMeteoLocation(BaseModel):
    hourly: OpenMeteoHourly


# One location is returned as an object, several as a list of objects.
_PAYLOAD = TypeAdapter(list[object] | dict[str, object])


@cache
def forecast_cells() -> tuple[ForecastCell, ...]:
    cells: list[ForecastCell] = []
    for cell in cells_covering(*REGION_BOUNDS):
        latitude, longitude = h3.cell_to_latlng(cell)
        cells.append(ForecastCell(cell, round(latitude, 4), round(longitude, 4)))
    return tuple(cells)


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


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def parse_location(
    cell: ForecastCell, location: object, generated_at: datetime
) -> WeatherForecast:
    hourly = OpenMeteoLocation.model_validate(location).hourly
    times = hourly.time
    series = [
        hourly.temperature_2m,
        hourly.precipitation,
        hourly.wind_speed_10m,
        hourly.wind_direction_10m,
    ]
    if any(len(values) != len(times) for values in series):
        raise ValueError("forecast values do not match the hourly timeline")
    return WeatherForecast(
        cell=cell.cell,
        latitude=cell.latitude,
        longitude=cell.longitude,
        provider="open-meteo",
        model=MODEL,
        generated_at=generated_at,
        hourly=[
            HourlyWeather(
                valid_at=_utc(time),
                temperature_c=series[0][index],
                precipitation_mm=series[1][index],
                wind_speed_kmh=series[2][index],
                wind_direction_degrees=series[3][index],
            )
            for index, time in enumerate(times)
        ],
    )


def fetch_chunk(cells: list[ForecastCell], generated_at: datetime) -> list[WeatherForecast]:
    parameters = {
        "latitude": ",".join(str(cell.latitude) for cell in cells),
        "longitude": ",".join(str(cell.longitude) for cell in cells),
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_days": str(FORECAST_DAYS),
        "models": MODEL,
        "timezone": "GMT",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    }
    try:
        url = f"{OPEN_METEO_URL}?{urlencode(parameters)}"
        with cast(HTTPResponse, urlopen(url, timeout=60)) as response:
            payload = _PAYLOAD.validate_json(response.read())
    except HTTPError as exc:
        if exc.code == 429:
            seconds = _retry_after_seconds(exc.headers.get("Retry-After"))
            raise RetryLater("Open-Meteo rate limit reached", seconds) from exc
        raise RuntimeError(f"Open-Meteo request failed with HTTP {exc.code}") from exc
    except (URLError, TimeoutError, OSError, UnicodeDecodeError, ValidationError) as exc:
        raise RuntimeError(f"Open-Meteo request failed: {type(exc).__name__}") from exc

    locations: list[object] = payload if isinstance(payload, list) else [payload]
    if len(locations) != len(cells):
        raise RuntimeError("Open-Meteo returned an unexpected number of locations")
    forecasts: list[WeatherForecast] = []
    for cell, location in zip(cells, locations, strict=True):
        try:
            forecasts.append(parse_location(cell, location, generated_at))
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            LOGGER.warning("Skipping invalid Open-Meteo forecast for %s: %s", cell.cell, exc)
    return forecasts


@dataclass
class WeatherProducer:
    publisher: Publisher[WeatherForecast]

    def run_once(self) -> str:
        cells = forecast_cells()
        generated_at = datetime.now(UTC)
        published = 0
        for offset in range(0, len(cells), LOCATIONS_PER_REQUEST):
            chunk = list(cells[offset : offset + LOCATIONS_PER_REQUEST])
            count, _ = self.publisher.publish(fetch_chunk(chunk, generated_at))
            published += count
        return f"published forecasts for {published} of {len(cells)} cells"
