"""Unit tests for the Phase 8 projections' key parsing and row mapping."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from wildfire_events.cells import partition_cell
from wildfire_events.countries import Country
from wildfire_events.models import (
    EnrichedDetection,
    FireRisk,
    FirmsDetection,
    FirmsProduct,
    HourlyDetectionStats,
    ObservedWeather,
    RiskLevel,
    detection_event_id,
)
from wildfire_sink.enrichment import COLUMNS
from wildfire_sink.enrichment import row_parameters as condition_row
from wildfire_sink.incidents import incident_key
from wildfire_sink.statistics import parse_stats_key

OBSERVED = datetime(2026, 3, 5, 5, 40, tzinfo=UTC)


def detection() -> FirmsDetection:
    product = FirmsProduct.VIIRS_NOAA20_NRT
    return FirmsDetection(
        event_id=detection_event_id(product, "N20", OBSERVED, "0.5", "114.1"),
        product=product,
        instrument=product.instrument,
        satellite="N20",
        observed_at=OBSERVED,
        latitude=0.5,
        longitude=114.1,
        h3_cell=partition_cell(0.5, 114.1),
        frp_mw=55.0,
        attributes={},
        ingested_at=OBSERVED + timedelta(hours=3),
    )


def test_stats_key_round_trips_from_the_model_key() -> None:
    hour = datetime(2026, 3, 5, 7, tzinfo=UTC)
    stats = HourlyDetectionStats(
        country=Country.IDN,
        hour_start=hour,
        detections=1,
        detections_with_frp=0,
        total_frp_mw=0,
    )
    assert parse_stats_key(stats.key.encode()) == (Country.IDN, hour)


@pytest.mark.parametrize(
    "key",
    [
        None,
        b"",
        b"IDN",
        b"IDN|not-a-time",
        b"IDN|2026-03-05T07:00:00",  # naive
        b"FRA|2026-03-05T07:00:00+00:00",  # not a selected country
        b"\xff\xfe",
    ],
)
def test_malformed_stats_keys_are_rejected(key: bytes | None) -> None:
    assert parse_stats_key(key) is None


def test_incident_key_parses_uuids_only() -> None:
    incident_id = uuid.uuid4()
    assert incident_key(str(incident_id).encode()) == incident_id
    assert incident_key(b"not-a-uuid") is None
    assert incident_key(None) is None


def test_enriched_detection_maps_weather_and_risk_columns() -> None:
    weather = ObservedWeather(
        valid_at=OBSERVED.replace(minute=0) + timedelta(hours=1),
        forecast_generated_at=OBSERVED - timedelta(hours=2),
        temperature_c=33.0,
        precipitation_mm=0,
        precipitation_24h_mm=0.1,
        wind_speed_kmh=12,
        wind_direction_degrees=270,
    )
    risk = FireRisk(
        score=8,
        level=RiskLevel.HIGH,
        wind_points=1,
        temperature_points=2,
        dryness_points=3,
        intensity_points=2,
    )
    event = EnrichedDetection(
        detection=detection(), country=Country.IDN, weather=weather, risk=risk
    )
    row = condition_row(event)
    assert set(row) == set(COLUMNS)
    assert row["event_id"] == event.detection.event_id
    assert row["country"] == "IDN"
    assert row["weather_valid_at"] == weather.valid_at
    assert row["precipitation_24h_mm"] == 0.1
    assert row["risk_score"] == 8
    assert row["risk_level"] == "high"
    assert row["intensity_points"] == 2


def test_enriched_detection_without_weather_leaves_conditions_null() -> None:
    row = condition_row(
        EnrichedDetection(detection=detection(), country=None, weather=None, risk=None)
    )
    nullable = set(COLUMNS) - {"event_id", "h3_cell", "observed_at"}
    assert all(row[column] is None for column in nullable)
    assert row["observed_at"] == OBSERVED
