"""Fire risk and weather at observation time, from the ``detection_conditions`` projection.

The stream processor enriches each detection (``detections.enriched``) and the sink
worker stores the result in ``detection_conditions``, keyed by the detection's
``event_id``. The rules behind the risk points are documented in
docs/operations/event-pipeline.md.
"""

from datetime import datetime
from typing import LiteralString

from pydantic import BaseModel

from wildfire_events.models import RiskLevel

# Columns read from ``detection_conditions`` (aliased ``c``) for one detection.
CONDITION_COLUMNS: LiteralString = """
    c.country, c.weather_valid_at, c.forecast_generated_at, c.temperature_c,
    c.precipitation_mm, c.precipitation_24h_mm, c.wind_speed_kmh, c.wind_direction_degrees,
    c.risk_score, c.risk_level, c.wind_points, c.temperature_points, c.dryness_points,
    c.intensity_points
"""


class RiskSummary(BaseModel):
    score: int
    level: RiskLevel


class DetectionRisk(RiskSummary):
    """Risk score (0-12) and the points (0-3) each factor contributed."""

    wind_points: int
    temperature_points: int
    dryness_points: int
    intensity_points: int


class DetectionWeather(BaseModel):
    """Forecast weather for the detection's cell at the hour nearest its observation."""

    valid_at: datetime
    temperature_c: float | None
    precipitation_mm: float | None
    precipitation_24h_mm: float | None
    wind_speed_kmh: float | None
    wind_direction_degrees: float | None


class ConditionColumns(BaseModel):
    """The ``CONDITION_COLUMNS`` of a row; all null when the detection is not enriched yet."""

    country: str | None = None
    weather_valid_at: datetime | None = None
    forecast_generated_at: datetime | None = None
    temperature_c: float | None = None
    precipitation_mm: float | None = None
    precipitation_24h_mm: float | None = None
    wind_speed_kmh: float | None = None
    wind_direction_degrees: float | None = None
    risk_score: int | None = None
    risk_level: RiskLevel | None = None
    wind_points: int | None = None
    temperature_points: int | None = None
    dryness_points: int | None = None
    intensity_points: int | None = None

    def risk(self) -> DetectionRisk | None:
        if (
            self.risk_score is None
            or self.risk_level is None
            or self.wind_points is None
            or self.temperature_points is None
            or self.dryness_points is None
            or self.intensity_points is None
        ):
            return None
        return DetectionRisk(
            score=self.risk_score,
            level=self.risk_level,
            wind_points=self.wind_points,
            temperature_points=self.temperature_points,
            dryness_points=self.dryness_points,
            intensity_points=self.intensity_points,
        )

    def weather(self) -> DetectionWeather | None:
        if self.weather_valid_at is None:
            return None
        return DetectionWeather(
            valid_at=self.weather_valid_at,
            temperature_c=self.temperature_c,
            precipitation_mm=self.precipitation_mm,
            precipitation_24h_mm=self.precipitation_24h_mm,
            wind_speed_kmh=self.wind_speed_kmh,
            wind_direction_degrees=self.wind_direction_degrees,
        )
