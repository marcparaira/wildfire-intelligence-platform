"""A simple, documented fire risk indicator for one detection.

Four factors each score 0-3 points; the score is their sum (0-12):

=============  ===============================  ====  =====  =====  ====
Factor         Input                            0     1      2      3
=============  ===============================  ====  =====  =====  ====
Wind           wind speed at 10 m (km/h)        <10   10-20  20-30  >=30
Temperature    air temperature at 2 m (°C)      <25   25-30  30-35  >=35
Dryness        precipitation, past 24 h (mm)    >=10  2-10   0.2-2  <0.2
Intensity      fire radiative power (MW)        <10   10-50  50-100 >=100
=============  ===============================  ====  =====  =====  ====

Lower bounds are inclusive. Unknown FRP scores 0. Levels: 0-3 low, 4-6 moderate,
7-9 high, 10-12 extreme. Without temperature, wind speed, or 24-hour precipitation
there is no risk value. This is a screening heuristic for ranking detections, not
a calibrated fire danger rating such as the Canadian FWI.
"""

from bisect import bisect_right

from wildfire_events.models import FireRisk, ObservedWeather, RiskLevel

# Ascending thresholds: the points are the number of thresholds at or below the value.
WIND_THRESHOLDS_KMH = (10.0, 20.0, 30.0)
TEMPERATURE_THRESHOLDS_C = (25.0, 30.0, 35.0)
INTENSITY_THRESHOLDS_MW = (10.0, 50.0, 100.0)
# Descending wetness: less precipitation means more points.
DRYNESS_THRESHOLDS_MM = (10.0, 2.0, 0.2)
# Lowest score of each level above LOW.
LEVEL_FLOORS = ((10, RiskLevel.EXTREME), (7, RiskLevel.HIGH), (4, RiskLevel.MODERATE))


def _points(value: float, thresholds: tuple[float, ...]) -> int:
    return bisect_right(thresholds, value)


def _dryness_points(precipitation_mm: float) -> int:
    return sum(precipitation_mm < threshold for threshold in DRYNESS_THRESHOLDS_MM)


def risk_level(score: int) -> RiskLevel:
    for floor, level in LEVEL_FLOORS:
        if score >= floor:
            return level
    return RiskLevel.LOW


def compute_risk(weather: ObservedWeather | None, frp_mw: float | None) -> FireRisk | None:
    """The detection's risk, or ``None`` when the weather it needs is missing."""
    if weather is None:
        return None
    temperature, wind = weather.temperature_c, weather.wind_speed_kmh
    precipitation = weather.precipitation_24h_mm
    if temperature is None or wind is None or precipitation is None:
        return None
    wind_points = _points(wind, WIND_THRESHOLDS_KMH)
    temperature_points = _points(temperature, TEMPERATURE_THRESHOLDS_C)
    dryness_points = _dryness_points(precipitation)
    intensity_points = 0 if frp_mw is None else _points(frp_mw, INTENSITY_THRESHOLDS_MW)
    score = wind_points + temperature_points + dryness_points + intensity_points
    return FireRisk(
        score=score,
        level=risk_level(score),
        wind_points=wind_points,
        temperature_points=temperature_points,
        dryness_points=dryness_points,
        intensity_points=intensity_points,
    )
