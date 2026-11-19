"""Event payload models.

The pydantic models are the source of the JSON Schemas registered in the schema
registry (see ``wildfire_events.schemas``). Evolve them backward-compatibly: add
optional fields with defaults, never remove, rename, or retype fields.
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from wildfire_events.cells import is_partition_cell, partition_cell

FirmsProduct = Literal["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT", "MODIS_NRT"]
Instrument = Literal["VIIRS", "MODIS"]

FIRMS_PRODUCTS: dict[str, Instrument] = {
    "VIIRS_SNPP_NRT": "VIIRS",
    "VIIRS_NOAA20_NRT": "VIIRS",
    "VIIRS_NOAA21_NRT": "VIIRS",
    "MODIS_NRT": "MODIS",
}

_EVENT_NAMESPACE = uuid.UUID("6f1d4a52-3c1e-4d55-9a7e-1f0c6b8f2a10")


def _utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


def _canonical_number(value: str) -> str:
    try:
        number = Decimal(value.strip())
    except InvalidOperation as exc:
        raise ValueError(f"not a number: {value!r}") from exc
    if not number.is_finite():
        raise ValueError(f"not a finite number: {value!r}")
    return format(number.normalize(), "f")


def detection_event_id(
    product: str, satellite: str, observed_at: datetime, latitude: str, longitude: str
) -> uuid.UUID:
    """Deterministic identity of one FIRMS detection.

    Re-polling an overlapping FIRMS window returns the same rows; they get the same
    ID, which producers and projections use for idempotent deduplication. Coordinates
    are taken as the provider's decimal strings so float formatting cannot change IDs.
    """
    key = "|".join(
        (
            product,
            satellite.strip(),
            _utc(observed_at).isoformat(),
            _canonical_number(latitude),
            _canonical_number(longitude),
        )
    )
    return uuid.uuid5(_EVENT_NAMESPACE, key)


class FirmsDetection(BaseModel):
    """One satellite active-fire pixel from a NASA FIRMS NRT product."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, title="FirmsDetection")

    event_id: uuid.UUID
    product: FirmsProduct
    instrument: Instrument
    satellite: str = Field(min_length=1, max_length=32)
    observed_at: AwareDatetime
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    h3_cell: str = Field(description="Resolution-2 H3 cell; also the Kafka message key")
    frp_mw: float | None = Field(default=None, description="Fire radiative power in MW")
    attributes: dict[str, str] = Field(description="Provider CSV row, unmodified")
    ingested_at: AwareDatetime

    _normalize_times = field_validator("observed_at", "ingested_at")(_utc)

    @field_validator("satellite")
    @classmethod
    def satellite_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("satellite must not be blank")
        return value.strip()

    @model_validator(mode="after")
    def consistent(self) -> "FirmsDetection":
        if FIRMS_PRODUCTS[self.product] != self.instrument:
            raise ValueError(f"{self.product} is not a {self.instrument} product")
        if not is_partition_cell(self.h3_cell):
            raise ValueError("h3_cell must be a resolution-2 H3 cell")
        if partition_cell(self.latitude, self.longitude) != self.h3_cell:
            raise ValueError("h3_cell does not contain the detection")
        return self


class HourlyWeather(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, title="HourlyWeather")

    valid_at: AwareDatetime
    temperature_c: float | None
    precipitation_mm: float | None = Field(ge=0)
    wind_speed_kmh: float | None = Field(ge=0)
    wind_direction_degrees: float | None = Field(ge=0, le=360)

    _normalize_time = field_validator("valid_at")(_utc)


class WeatherForecast(BaseModel):
    """Latest hourly forecast for one H3 cell; the compacted topic keeps one per cell."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, title="WeatherForecast")

    cell: str = Field(description="Resolution-2 H3 cell; also the Kafka message key")
    latitude: float = Field(ge=-90, le=90, description="Cell center used for the forecast")
    longitude: float = Field(ge=-180, le=180)
    provider: Literal["open-meteo"]
    model: str = Field(min_length=1)
    generated_at: AwareDatetime
    hourly: list[HourlyWeather] = Field(min_length=1)

    _normalize_time = field_validator("generated_at")(_utc)

    @model_validator(mode="after")
    def consistent(self) -> "WeatherForecast":
        if not is_partition_cell(self.cell):
            raise ValueError("cell must be a resolution-2 H3 cell")
        times = [hour.valid_at for hour in self.hourly]
        if any(later <= earlier for earlier, later in zip(times, times[1:], strict=False)):
            raise ValueError("hourly values must be in strictly increasing time order")
        return self
