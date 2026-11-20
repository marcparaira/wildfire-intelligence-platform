"""Enrich detections with their country, the weather at observation time, and a risk.

Detections and forecasts share the Kafka key (the resolution-2 H3 cell), so one
state per cell sees both. Forecasts are merged into an hourly weather history for
the cell; a detection takes the hour nearest its observation time from it.

Weather history
    A newer forecast (by ``generated_at``) replaces the hours it covers; an older one
    only fills hours the history does not have. Each hour remembers the retrieval
    time of the forecast that supplied it. Hours more than ``HISTORY_WINDOW`` before
    the first hour of the newest forecast are dropped, which keeps the past few days
    that Open-Meteo no longer returns (a forecast starts at 00:00 UTC of its day).
    A forecast tombstone clears the cell.

Observation weather
    The forecast hour nearest the observation (half an hour rounds up) supplies the
    values. The 24-hour precipitation sums that hour and the 23 before it, and is
    null when any of them is missing. Without the hour the weather is null.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from pydantic import BaseModel

from wildfire_events.countries import Country
from wildfire_events.models import (
    EnrichedDetection,
    FirmsDetection,
    ObservedWeather,
    WeatherForecast,
)
from wildfire_processor.risk import compute_risk
from wildfire_processor.state import KeyValueState

HISTORY_WINDOW = timedelta(hours=72)
PRECIPITATION_WINDOW_HOURS = 24
PRECIPITATION_DECIMALS = 3
HOUR = timedelta(hours=1)
HOUR_MS = 3_600_000
_MS = 1000

# State store key; the stored layout is persisted in the Quix changelog, so keep it stable.
_WEATHER_KEY = "weather"

# temperature_c, precipitation_mm, wind_speed_kmh, wind_direction_degrees, generated_ms
_StoredHour = tuple[float | None, float | None, float | None, float | None, int]


def _ms(value: datetime) -> int:
    return int(value.timestamp() * _MS)


def _datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / _MS, UTC)


def nearest_hour(moment: datetime) -> datetime:
    """The whole UTC hour nearest ``moment``; exactly half past rounds up."""
    shifted = moment.astimezone(UTC) + HOUR / 2
    return shifted.replace(minute=0, second=0, microsecond=0)


class CountryLocator(Protocol):
    """``wildfire_events.countries.CountryLocator`` implements it."""

    def locate(self, points: Sequence[tuple[float, float]]) -> list[Country | None]: ...


@dataclass(frozen=True, slots=True)
class ForecastUpdate:
    """A record of ``weather.forecasts``: a forecast, or a tombstone (``None``)."""

    cell: str
    forecast: WeatherForecast | None


class _CellWeather(BaseModel):
    """Stored weather history of one cell; JSON-serializable for the state store."""

    newest_generated_ms: int
    newest_first_hour_ms: int
    hours: dict[str, _StoredHour]


class WeatherHistory:
    """The hourly weather history of the cell whose state this is."""

    def __init__(self, state: KeyValueState) -> None:
        self.state: KeyValueState = state

    def _load(self) -> _CellWeather | None:
        stored = self.state.get(_WEATHER_KEY)
        return None if stored is None else _CellWeather.model_validate(stored)

    def merge(self, forecast: WeatherForecast) -> None:
        generated_ms = _ms(forecast.generated_at)
        first_hour_ms = _ms(forecast.hourly[0].valid_at)
        history = self._load() or _CellWeather(
            newest_generated_ms=generated_ms, newest_first_hour_ms=first_hour_ms, hours={}
        )
        for hour in forecast.hourly:
            key = str(_ms(hour.valid_at))
            stored = history.hours.get(key)
            if stored is None or stored[4] <= generated_ms:
                history.hours[key] = (
                    hour.temperature_c,
                    hour.precipitation_mm,
                    hour.wind_speed_kmh,
                    hour.wind_direction_degrees,
                    generated_ms,
                )
        if generated_ms >= history.newest_generated_ms:
            history.newest_generated_ms = generated_ms
            history.newest_first_hour_ms = first_hour_ms
        self._prune(history)
        self.state.set(_WEATHER_KEY, history.model_dump(mode="json"))

    @staticmethod
    def _prune(history: _CellWeather) -> None:
        cutoff_ms = history.newest_first_hour_ms - int(HISTORY_WINDOW.total_seconds() * _MS)
        history.hours = {key: hour for key, hour in history.hours.items() if int(key) >= cutoff_ms}

    def clear(self) -> None:
        self.state.delete(_WEATHER_KEY)

    def observed(self, observed_at: datetime) -> ObservedWeather | None:
        """The weather at the forecast hour nearest ``observed_at``, if known."""
        history = self._load()
        if history is None:
            return None
        valid_at = nearest_hour(observed_at)
        hour = history.hours.get(str(_ms(valid_at)))
        if hour is None:
            return None
        temperature, precipitation, wind_speed, wind_direction, generated_ms = hour
        return ObservedWeather(
            valid_at=valid_at,
            forecast_generated_at=_datetime(generated_ms),
            temperature_c=temperature,
            precipitation_mm=precipitation,
            precipitation_24h_mm=self._precipitation_24h(history, _ms(valid_at)),
            wind_speed_kmh=wind_speed,
            wind_direction_degrees=wind_direction,
        )

    @staticmethod
    def _precipitation_24h(history: _CellWeather, valid_ms: int) -> float | None:
        total = 0.0
        for offset in range(PRECIPITATION_WINDOW_HOURS):
            hour = history.hours.get(str(valid_ms - offset * HOUR_MS))
            if hour is None or hour[1] is None:
                return None
            total += hour[1]
        return round(total, PRECIPITATION_DECIMALS)


@dataclass(frozen=True, slots=True)
class Enricher:
    """Applies one ``weather.forecasts`` or ``firms.detections`` record to a cell's state."""

    countries: CountryLocator

    def process(
        self, record: ForecastUpdate | FirmsDetection, state: KeyValueState
    ) -> list[EnrichedDetection]:
        """Forecasts update the history and emit nothing; a detection emits its enrichment."""
        history = WeatherHistory(state)
        if isinstance(record, FirmsDetection):
            return [self.enrich(record, history)]
        if record.forecast is None:
            history.clear()
        else:
            history.merge(record.forecast)
        return []

    def enrich(self, detection: FirmsDetection, history: WeatherHistory) -> EnrichedDetection:
        weather = history.observed(detection.observed_at)
        [country] = self.countries.locate([(detection.latitude, detection.longitude)])
        return EnrichedDetection(
            detection=detection,
            country=country,
            weather=weather,
            risk=compute_risk(weather, detection.frp_mw),
        )
