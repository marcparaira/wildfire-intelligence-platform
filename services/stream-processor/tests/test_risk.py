from datetime import UTC, datetime

import pytest

from wildfire_events.models import ObservedWeather, RiskLevel
from wildfire_processor.risk import compute_risk, risk_level

HOUR = datetime(2026, 3, 5, 6, tzinfo=UTC)


def weather(
    *,
    temperature: float | None = 20.0,
    wind: float | None = 5.0,
    precipitation_24h: float | None = 20.0,
) -> ObservedWeather:
    return ObservedWeather(
        valid_at=HOUR,
        forecast_generated_at=HOUR,
        temperature_c=temperature,
        precipitation_mm=0.0,
        precipitation_24h_mm=precipitation_24h,
        wind_speed_kmh=wind,
        wind_direction_degrees=90.0,
    )


@pytest.mark.parametrize(
    ("wind", "points"),
    [(0.0, 0), (9.99, 0), (10.0, 1), (19.99, 1), (20.0, 2), (29.99, 2), (30.0, 3), (80.0, 3)],
)
def test_wind_points(wind: float, points: int) -> None:
    risk = compute_risk(weather(wind=wind), None)
    assert risk is not None
    assert risk.wind_points == points


@pytest.mark.parametrize(
    ("temperature", "points"),
    [(-5.0, 0), (24.99, 0), (25.0, 1), (29.99, 1), (30.0, 2), (34.99, 2), (35.0, 3)],
)
def test_temperature_points(temperature: float, points: int) -> None:
    risk = compute_risk(weather(temperature=temperature), None)
    assert risk is not None
    assert risk.temperature_points == points


@pytest.mark.parametrize(
    ("precipitation", "points"),
    [(50.0, 0), (10.0, 0), (9.99, 1), (2.0, 1), (1.99, 2), (0.2, 2), (0.19, 3), (0.0, 3)],
)
def test_dryness_points(precipitation: float, points: int) -> None:
    risk = compute_risk(weather(precipitation_24h=precipitation), None)
    assert risk is not None
    assert risk.dryness_points == points


@pytest.mark.parametrize(
    ("frp", "points"),
    [(None, 0), (0.0, 0), (9.99, 0), (10.0, 1), (49.99, 1), (50.0, 2), (99.99, 2), (100.0, 3)],
)
def test_intensity_points(frp: float | None, points: int) -> None:
    risk = compute_risk(weather(), frp)
    assert risk is not None
    assert risk.intensity_points == points


@pytest.mark.parametrize(
    ("score", "level"),
    [
        (0, RiskLevel.LOW),
        (3, RiskLevel.LOW),
        (4, RiskLevel.MODERATE),
        (6, RiskLevel.MODERATE),
        (7, RiskLevel.HIGH),
        (9, RiskLevel.HIGH),
        (10, RiskLevel.EXTREME),
        (12, RiskLevel.EXTREME),
    ],
)
def test_levels(score: int, level: RiskLevel) -> None:
    assert risk_level(score) is level


def test_score_sums_the_factors() -> None:
    risk = compute_risk(weather(temperature=36.0, wind=25.0, precipitation_24h=0.0), 60.0)
    assert risk is not None
    assert (risk.score, risk.level) == (3 + 2 + 3 + 2, RiskLevel.EXTREME)


@pytest.mark.parametrize(
    "missing",
    [
        weather(temperature=None),
        weather(wind=None),
        weather(precipitation_24h=None),
        None,
    ],
)
def test_no_risk_without_the_weather_it_needs(missing: ObservedWeather | None) -> None:
    assert compute_risk(missing, 120.0) is None
