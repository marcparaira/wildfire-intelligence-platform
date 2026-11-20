"""Hourly per-country detection and FRP statistics.

Enriched detections with a country are counted in one-hour tumbling windows of
observation time per country. Every update of a window emits the running totals,
and compaction of ``stats.hourly`` keeps the latest. A detection observed more than
``GRACE`` before the newest observation already counted for its country is too
late: its window is closed, and Quix logs and drops it. Cross-sensor observations
of the same fire each count.
"""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel

from wildfire_events.countries import Country
from wildfire_events.models import EnrichedDetection, HourlyDetectionStats

WINDOW = timedelta(hours=1)
GRACE = timedelta(days=3)
FRP_DECIMALS = 3
_MS = 1000


class StatsInput(BaseModel):
    """What the window needs from one detection; JSON-serializable for repartitioning."""

    country: Country
    observed_ms: int
    frp_mw: float | None


class WindowResult(BaseModel):
    """A Quix tumbling-window result with the aggregates ``statistics_job`` defines."""

    start: int
    detections: int
    detections_with_frp: int
    total_frp_mw: float
    max_frp_mw: float | None


def stats_input(enriched: EnrichedDetection) -> StatsInput | None:
    """The window input for a detection, or ``None`` when it has no country."""
    if enriched.country is None:
        return None
    detection = enriched.detection
    return StatsInput(
        country=enriched.country,
        observed_ms=int(detection.observed_at.timestamp() * _MS),
        frp_mw=detection.frp_mw,
    )


def hourly_stats(country: str, window: Mapping[str, object]) -> HourlyDetectionStats:
    result = WindowResult.model_validate(window)
    return HourlyDetectionStats(
        country=Country(country),
        hour_start=datetime.fromtimestamp(result.start / _MS, UTC),
        detections=result.detections,
        detections_with_frp=result.detections_with_frp,
        total_frp_mw=round(result.total_frp_mw, FRP_DECIMALS),
        max_frp_mw=result.max_frp_mw,
    )
