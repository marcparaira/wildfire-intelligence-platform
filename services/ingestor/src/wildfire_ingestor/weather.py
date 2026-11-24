"""Open-Meteo hourly forecasts for the H3 forecast grid."""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cache
from typing import Protocol

import h3
from pydantic import BaseModel, ValidationError

from wildfire_events.cells import cells_covering
from wildfire_events.countries import REGION_BOUNDS
from wildfire_events.models import HourlyWeather, WeatherForecast, WeatherProvider
from wildfire_producers.publisher import Publisher

LOGGER = logging.getLogger(__name__)
LOCATIONS_PER_REQUEST = 100
FORECAST_DAYS = 7
MODEL = "best_match"
HOURLY_VARIABLES = ("temperature_2m", "precipitation", "wind_speed_10m", "wind_direction_10m")
COORDINATE_DECIMALS = 4


@dataclass(frozen=True, slots=True)
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


class ForecastSource(Protocol):
    """Raw per-location forecasts; ``OpenMeteoClient`` implements it."""

    def fetch_locations(self, cells: Sequence[ForecastCell]) -> list[object]: ...


@cache
def forecast_cells() -> tuple[ForecastCell, ...]:
    """The partition cells over the region, each forecast at its rounded center."""
    cells: list[ForecastCell] = []
    for cell in cells_covering(*REGION_BOUNDS):
        latitude, longitude = h3.cell_to_latlng(cell)
        cells.append(
            ForecastCell(
                cell, round(latitude, COORDINATE_DECIMALS), round(longitude, COORDINATE_DECIMALS)
            )
        )
    return tuple(cells)


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def parse_location(cell: ForecastCell, location: object, generated_at: datetime) -> WeatherForecast:
    """Validate one Open-Meteo location payload; raises ``ValueError`` if it is malformed."""
    hourly = OpenMeteoLocation.model_validate(location).hourly
    series = (
        hourly.temperature_2m,
        hourly.precipitation,
        hourly.wind_speed_10m,
        hourly.wind_direction_10m,
    )
    if any(len(values) != len(hourly.time) for values in series):
        raise ValueError("forecast values do not match the hourly timeline")
    return WeatherForecast(
        cell=cell.cell,
        latitude=cell.latitude,
        longitude=cell.longitude,
        provider=WeatherProvider.OPEN_METEO,
        model=MODEL,
        generated_at=generated_at,
        hourly=[
            HourlyWeather(
                valid_at=_utc(time),
                temperature_c=temperature,
                precipitation_mm=precipitation,
                wind_speed_kmh=wind_speed,
                wind_direction_degrees=wind_direction,
            )
            for time, temperature, precipitation, wind_speed, wind_direction in zip(
                hourly.time, *series, strict=True
            )
        ],
    )


def fetch_forecasts(
    source: ForecastSource, cells: Sequence[ForecastCell], generated_at: datetime
) -> list[WeatherForecast]:
    """Forecasts for ``cells``; invalid locations are logged and skipped."""
    locations = source.fetch_locations(cells)
    if len(locations) != len(cells):
        raise RuntimeError("Open-Meteo returned an unexpected number of locations")
    forecasts: list[WeatherForecast] = []
    for cell, location in zip(cells, locations, strict=True):
        try:
            forecasts.append(parse_location(cell, location, generated_at))
        except (ValueError, ValidationError) as exc:
            LOGGER.warning("Skipping invalid Open-Meteo forecast for %s: %s", cell.cell, exc)
    return forecasts


@dataclass(frozen=True, slots=True)
class WeatherProducer:
    """Publishes a fresh forecast for every grid cell on each run."""

    source: ForecastSource
    publisher: Publisher[WeatherForecast]

    def run_once(self) -> str:
        cells = forecast_cells()
        generated_at = datetime.now(UTC)
        published = 0
        for offset in range(0, len(cells), LOCATIONS_PER_REQUEST):
            chunk = cells[offset : offset + LOCATIONS_PER_REQUEST]
            count, _ = self.publisher.publish(fetch_forecasts(self.source, chunk, generated_at))
            published += count
        return f"published forecasts for {published} of {len(cells)} cells"
