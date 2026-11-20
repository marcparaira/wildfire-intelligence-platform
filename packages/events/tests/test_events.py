import struct
from datetime import UTC, datetime, timedelta, timezone
from enum import StrEnum
from typing import get_args

import pytest
from pydantic import ValidationError

from wildfire_events import schemas
from wildfire_events.cells import cell_boundary, cells_covering, partition_cell
from wildfire_events.codec import EventCodec, InvalidEvent
from wildfire_events.countries import Country, CountryLiteral, selected_countries
from wildfire_events.models import (
    FireRisk,
    FirmsDetection,
    FirmsProduct,
    FirmsProductLiteral,
    HourlyDetectionStats,
    HourlyWeather,
    IncidentStatus,
    IncidentStatusLiteral,
    Instrument,
    InstrumentLiteral,
    RiskLevel,
    RiskLevelLiteral,
    WeatherForecast,
    WeatherProvider,
    WeatherProviderLiteral,
    detection_event_id,
)

OBSERVED = datetime(2026, 3, 5, 0, 7, tzinfo=UTC)


def detection(**changes: object) -> FirmsDetection:
    values: dict[str, object] = {
        "event_id": detection_event_id(
            FirmsProduct.VIIRS_NOAA20_NRT, "N20", OBSERVED, "0.5234", "114.1234"
        ),
        "product": "VIIRS_NOAA20_NRT",
        "instrument": "VIIRS",
        "satellite": "N20",
        "observed_at": OBSERVED,
        "latitude": 0.5234,
        "longitude": 114.1234,
        "h3_cell": partition_cell(0.5234, 114.1234),
        "frp_mw": 12.5,
        "attributes": {"frp": "12.5"},
        "ingested_at": OBSERVED + timedelta(hours=3),
    }
    values.update(changes)
    return FirmsDetection.model_validate(values)


class FakeRegistry:
    def __init__(self) -> None:
        self.subjects: dict[int, str] = {}

    def register(self, subject: str, schema: str) -> int:
        assert schema == schemas.load(schemas.SUBJECT_MODELS[subject])
        schema_id = len(self.subjects) + 1
        self.subjects[schema_id] = subject
        return schema_id

    def subjects_for(self, schema_id: int) -> frozenset[str]:
        return frozenset({self.subjects[schema_id]}) if schema_id in self.subjects else frozenset()


def test_checked_in_schemas_match_models() -> None:
    for model in schemas.SUBJECT_MODELS.values():
        assert schemas.load(model) == schemas.generate(model), (
            f"{model.__name__} schema is stale; run python -m wildfire_events.schemas"
        )


@pytest.mark.parametrize(
    ("literal", "enum"),
    [
        (InstrumentLiteral, Instrument),
        (FirmsProductLiteral, FirmsProduct),
        (WeatherProviderLiteral, WeatherProvider),
        (IncidentStatusLiteral, IncidentStatus),
        (RiskLevelLiteral, RiskLevel),
        (CountryLiteral, Country),
    ],
)
def test_wire_literals_cover_their_enums(literal: object, enum: type[StrEnum]) -> None:
    assert set(get_args(literal)) == set(enum)


def test_products_belong_to_their_instrument() -> None:
    assert FirmsProduct.MODIS_NRT.instrument is Instrument.MODIS
    assert {product.instrument for product in FirmsProduct} == set(Instrument)


def test_event_id_is_stable_across_coordinate_formatting_and_time_zones() -> None:
    plus_seven = OBSERVED.astimezone(timezone(timedelta(hours=7)))
    first = detection_event_id(FirmsProduct.MODIS_NRT, "Aqua", OBSERVED, "0.5234", "114.1234")
    assert first == detection_event_id(
        FirmsProduct.MODIS_NRT, " Aqua", plus_seven, "0.52340", "114.12340"
    )
    assert first != detection_event_id(
        FirmsProduct.MODIS_NRT, "Terra", OBSERVED, "0.5234", "114.1234"
    )
    with pytest.raises(ValueError, match="not a finite number"):
        _ = detection_event_id(FirmsProduct.MODIS_NRT, "Aqua", OBSERVED, "nan", "114")


@pytest.mark.parametrize(
    "changes",
    [
        {"instrument": "MODIS"},
        {"h3_cell": partition_cell(10, 100)},
        {"h3_cell": "not-a-cell"},
        {"observed_at": datetime(2026, 3, 5)},
        {"satellite": " "},
        {"unexpected": "field"},
    ],
)
def test_detection_rejects_inconsistent_events(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _ = detection(**changes)


def test_weather_forecast_requires_partition_cell_and_ordered_hours() -> None:
    hour = HourlyWeather(
        valid_at=OBSERVED,
        temperature_c=28.0,
        precipitation_mm=0.0,
        wind_speed_kmh=10.0,
        wind_direction_degrees=90.0,
    )
    values = {
        "cell": partition_cell(1, 114),
        "latitude": 1,
        "longitude": 114,
        "provider": "open-meteo",
        "model": "best_match",
        "generated_at": OBSERVED,
        "hourly": [hour, hour.model_copy(update={"valid_at": OBSERVED + timedelta(hours=1)})],
    }
    assert WeatherForecast.model_validate(values).cell == values["cell"]
    with pytest.raises(ValidationError):
        _ = WeatherForecast.model_validate({**values, "hourly": [hour, hour]})
    with pytest.raises(ValidationError):
        _ = WeatherForecast.model_validate({**values, "cell": "8928308280fffff"})


def test_codec_round_trip_uses_confluent_wire_format() -> None:
    codec = EventCodec(FirmsDetection, "firms.detections-value", FakeRegistry())
    event = detection()
    encoded = codec.encode(event)
    assert encoded[:5] == struct.pack(">bI", 0, 1)
    assert codec.decode(encoded) == event


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (None, "no value"),
        (b"{}", "too short"),
        (b"\x01\x00\x00\x00\x01{}", "magic byte"),
        (b"\x00\x00\x00\x00\x09{}", "not registered"),
        (b"\x00\x00\x00\x00\x02{}", "not registered"),
        (b'\x00\x00\x00\x00\x01{"event_id": 1}', "does not match"),
        (b"\x00\x00\x00\x00\x01not json", "does not match"),
    ],
)
def test_codec_rejects_invalid_records(value: bytes | None, message: str) -> None:
    registry = FakeRegistry()
    codec = EventCodec(FirmsDetection, "firms.detections-value", registry)
    assert codec.schema_id == 1
    _ = registry.register("weather.forecasts-value", schemas.load(WeatherForecast))
    with pytest.raises(InvalidEvent, match=message):
        _ = codec.decode(value)


def test_region_cells_have_closed_boundaries() -> None:
    cells = cells_covering(92, -12, 156, 29)
    assert len(cells) == 385
    ring = cell_boundary(cells[0])
    assert ring[0] == ring[-1]
    assert len(ring) == 7


def test_locates_countries_and_leaves_sea_and_other_countries_unassigned() -> None:
    bangkok, jakarta, port_moresby = (13.756, 100.502), (-6.2, 106.85), (-9.44, 147.18)
    south_china_sea, sydney = (12.0, 113.0), (-33.87, 151.21)
    locator = selected_countries()
    assert locator.locate([bangkok, south_china_sea, jakarta, sydney, port_moresby]) == [
        Country.THA,
        None,
        Country.IDN,
        None,
        Country.PNG,
    ]
    assert locator.covers([bangkok, sydney]) == [True, False]
    assert locator.locate([]) == []


def test_fire_risk_score_is_the_sum_of_its_factors() -> None:
    risk = FireRisk(
        score=7,
        level=RiskLevel.HIGH,
        wind_points=1,
        temperature_points=2,
        dryness_points=3,
        intensity_points=1,
    )
    assert risk.level is RiskLevel.HIGH
    with pytest.raises(ValidationError, match="sum of the factor points"):
        _ = FireRisk.model_validate({**risk.model_dump(), "score": 8})


def test_hourly_stats_invariants_and_key() -> None:
    hour = datetime(2026, 3, 5, 7, tzinfo=UTC)
    stats = HourlyDetectionStats(
        country=Country.IDN,
        hour_start=hour.astimezone(timezone(timedelta(hours=7))),
        detections=3,
        detections_with_frp=2,
        total_frp_mw=30.5,
        max_frp_mw=20.0,
    )
    assert stats.key == "IDN|2026-03-05T07:00:00+00:00"
    values = stats.model_dump()
    with pytest.raises(ValidationError, match="on the hour"):
        _ = HourlyDetectionStats.model_validate({**values, "hour_start": hour.replace(minute=5)})
    with pytest.raises(ValidationError, match="exceeds detections"):
        _ = HourlyDetectionStats.model_validate({**values, "detections_with_frp": 4})
    with pytest.raises(ValidationError, match="max_frp_mw"):
        _ = HourlyDetectionStats.model_validate(
            {**values, "detections_with_frp": 0, "total_frp_mw": 0}
        )
