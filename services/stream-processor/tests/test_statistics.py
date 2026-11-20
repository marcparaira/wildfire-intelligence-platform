from datetime import UTC, datetime

import pytest

from wildfire_events.countries import Country
from wildfire_events.models import EnrichedDetection
from wildfire_processor.statistics import hourly_stats, stats_input

from .test_incidents import CENTER, T0, detection

HOUR_MS = int(datetime(2026, 3, 5, 6, tzinfo=UTC).timestamp() * 1000)


def test_detections_without_a_country_are_not_counted() -> None:
    enriched = EnrichedDetection(
        detection=detection(CENTER, 0), country=None, weather=None, risk=None
    )
    assert stats_input(enriched) is None


def test_window_input_carries_country_observation_time_and_frp() -> None:
    enriched = EnrichedDetection(
        detection=detection(CENTER, 0.5, frp=None), country=Country.MYS, weather=None, risk=None
    )
    row = stats_input(enriched)
    assert row is not None
    assert row.model_dump(mode="json") == {
        "country": "MYS",
        "observed_ms": int(T0.timestamp() * 1000) + 30 * 60 * 1000,
        "frp_mw": None,
    }


def test_window_result_becomes_hourly_stats() -> None:
    stats = hourly_stats(
        "IDN",
        {
            "start": HOUR_MS,
            "end": HOUR_MS + 3_600_000,
            "detections": 3,
            "detections_with_frp": 2,
            "total_frp_mw": 0.1 + 0.2,
            "max_frp_mw": 0.2,
        },
    )
    assert stats.key == "IDN|2026-03-05T06:00:00+00:00"
    assert (stats.detections, stats.detections_with_frp) == (3, 2)
    assert stats.total_frp_mw == 0.3
    assert stats.max_frp_mw == 0.2


def test_window_without_frp_has_no_maximum() -> None:
    stats = hourly_stats(
        "THA",
        {
            "start": HOUR_MS,
            "end": HOUR_MS + 3_600_000,
            "detections": 1,
            "detections_with_frp": 0,
            "total_frp_mw": 0,
            "max_frp_mw": None,
        },
    )
    assert (stats.total_frp_mw, stats.max_frp_mw) == (0.0, None)


def test_unknown_country_is_rejected() -> None:
    with pytest.raises(ValueError, match="AUS"):
        _ = hourly_stats(
            "AUS",
            {
                "start": HOUR_MS,
                "detections": 1,
                "detections_with_frp": 0,
                "total_frp_mw": 0,
                "max_frp_mw": None,
            },
        )
