import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pytest

from wildfire_events.cells import partition_cell
from wildfire_events.countries import Country
from wildfire_events.models import (
    EnrichedDetection,
    HourlyWeather,
    RiskLevel,
    WeatherForecast,
    WeatherProvider,
)
from wildfire_processor.enrichment import (
    HISTORY_WINDOW,
    Enricher,
    ForecastUpdate,
    nearest_hour,
)

from .test_incidents import CENTER, T0, DictState, detection

CELL = partition_cell(1.0, 114.0)
DAY_START = T0.replace(hour=0)  # forecasts start at 00:00 UTC, like Open-Meteo's


@dataclass
class FakeLocator:
    country: Country | None = Country.IDN
    asked: list[tuple[float, float]] = field(default_factory=list)

    def locate(self, points: Sequence[tuple[float, float]]) -> list[Country | None]:
        self.asked.extend(points)
        return [self.country for _ in points]


def forecast(
    generated_at: datetime,
    start: datetime = DAY_START,
    hours: int = 48,
    *,
    temperature: float = 31.0,
    precipitation: float | None = 0.0,
    wind: float = 12.0,
) -> ForecastUpdate:
    return ForecastUpdate(
        cell=CELL,
        forecast=WeatherForecast(
            cell=CELL,
            latitude=1.0,
            longitude=114.0,
            provider=WeatherProvider.OPEN_METEO,
            model="best_match",
            generated_at=generated_at,
            hourly=[
                HourlyWeather(
                    valid_at=start + timedelta(hours=offset),
                    temperature_c=temperature,
                    precipitation_mm=precipitation,
                    wind_speed_kmh=wind,
                    wind_direction_degrees=270.0,
                )
                for offset in range(hours)
            ],
        ),
    )


def enrich(state: DictState, hours: float, locator: FakeLocator | None = None) -> EnrichedDetection:
    enricher = Enricher(locator or FakeLocator())
    [enriched] = enricher.process(detection(CENTER, hours, frp=60.0), state)
    return enriched


def apply(state: DictState, *updates: ForecastUpdate) -> None:
    enricher = Enricher(FakeLocator())
    for update in updates:
        assert enricher.process(update, state) == []


@pytest.mark.parametrize(
    ("minutes", "expected_hour"),
    [(0, 6), (29, 6), (30, 7), (31, 7), (59, 7)],
)
def test_nearest_hour_rounds_half_up(minutes: int, expected_hour: int) -> None:
    moment = T0 + timedelta(minutes=minutes, seconds=0)
    assert nearest_hour(moment) == T0.replace(hour=expected_hour)


def test_detection_gets_the_nearest_hours_weather_country_and_risk() -> None:
    state = DictState()
    generated = T0 - timedelta(hours=2)
    apply(state, forecast(generated, start=T0 - timedelta(hours=30)))
    locator = FakeLocator()

    enriched = enrich(state, 0.6, locator)  # 06:36 → 07:00

    weather = enriched.weather
    assert weather is not None
    assert weather.valid_at == T0 + timedelta(hours=1)
    assert weather.forecast_generated_at == generated
    assert (weather.temperature_c, weather.wind_speed_kmh) == (31.0, 12.0)
    assert weather.precipitation_24h_mm == 0.0
    assert enriched.country is Country.IDN
    assert locator.asked == [(enriched.detection.latitude, enriched.detection.longitude)]
    risk = enriched.risk
    assert risk is not None
    # wind 1 + temperature 2 + dryness 3 + FRP 60 MW 2
    assert (risk.score, risk.level) == (8, RiskLevel.HIGH)


def test_detection_without_a_forecast_hour_has_no_weather_or_risk() -> None:
    state = DictState()
    assert enrich(state, 0).weather is None  # no forecast at all

    apply(state, forecast(T0, start=T0 + timedelta(hours=5)))
    enriched = enrich(state, 0, FakeLocator(country=None))
    assert enriched.weather is None
    assert enriched.risk is None
    assert enriched.country is None


def test_newer_forecast_overrides_and_older_only_fills_gaps() -> None:
    state = DictState()
    newer, older = T0, T0 - timedelta(hours=6)
    apply(
        state,
        forecast(newer, start=T0, hours=24, temperature=33.0),
        forecast(older, start=T0 - timedelta(hours=12), hours=36, temperature=20.0),
    )
    current = enrich(state, 1).weather  # 07:00 is in both: the newer one wins
    assert current is not None
    assert (current.temperature_c, current.forecast_generated_at) == (33.0, newer)
    earlier = enrich(state, -3).weather  # 03:00 only in the older forecast
    assert earlier is not None
    assert (earlier.temperature_c, earlier.forecast_generated_at) == (20.0, older)

    apply(state, forecast(T0 + timedelta(hours=3), start=T0, hours=24, temperature=35.0))
    updated = enrich(state, 1).weather
    assert updated is not None
    assert updated.temperature_c == 35.0


def test_old_hours_are_pruned_relative_to_the_newest_forecast() -> None:
    state = DictState()
    apply(state, forecast(T0 - timedelta(days=5), start=DAY_START - timedelta(days=5), hours=24))
    assert enrich(state, -24 * 5).weather is not None

    apply(state, forecast(T0, start=DAY_START, hours=24))
    cutoff = DAY_START - HISTORY_WINDOW
    assert nearest_hour(T0 - timedelta(days=5)) < cutoff
    assert enrich(state, -24 * 5).weather is None
    assert enrich(state, 0).weather is not None


def test_tombstone_clears_the_cell() -> None:
    state = DictState()
    apply(state, forecast(T0), ForecastUpdate(cell=CELL, forecast=None))
    assert enrich(state, 0).weather is None
    assert state.data == {}


def test_precipitation_24h_needs_all_24_hours() -> None:
    state = DictState()
    # Hours 00:00-23:00 with 0.1 mm each: at 06:00 only 7 hours are known.
    apply(state, forecast(T0, start=DAY_START, hours=24, precipitation=0.1))
    partial = enrich(state, 0).weather
    assert partial is not None
    assert partial.precipitation_24h_mm is None
    assert enrich(state, 0).risk is None

    # The previous day completes the window: 24 hours of 0.1 mm.
    apply(
        state,
        forecast(
            T0 - timedelta(days=1), start=DAY_START - timedelta(days=1), hours=24, precipitation=0.1
        ),
    )
    complete = enrich(state, 0).weather
    assert complete is not None
    assert complete.precipitation_24h_mm == 2.4  # rounded, so float noise cannot leak


def test_precipitation_24h_is_null_when_an_hour_has_no_value() -> None:
    state = DictState()
    apply(
        state,
        forecast(T0 - timedelta(days=1), start=DAY_START - timedelta(days=1), hours=24),
        forecast(T0, start=DAY_START, hours=24, precipitation=None),
    )
    weather = enrich(state, 0).weather
    assert weather is not None
    assert weather.precipitation_mm is None
    assert weather.precipitation_24h_mm is None


def test_stored_history_is_json_serializable() -> None:
    state = DictState()
    apply(state, forecast(T0, hours=2))
    stored = state.data["weather"]
    assert json.loads(json.dumps(stored)) == stored
