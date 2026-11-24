"""Event payload models.

The pydantic models are the source of the JSON Schemas registered in the schema
registry (see ``wildfire_events.schemas``). Evolve them backward-compatibly: add
optional fields with defaults, never remove, rename, or retype fields.
"""

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum, auto
from itertools import pairwise
from typing import ClassVar, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from wildfire_events.cells import is_partition_cell, partition_cell
from wildfire_events.countries import CountryLiteral


class Instrument(StrEnum):
    """Satellite sensor family; values are the names FIRMS uses.

    FIRMS detections come from VIIRS and MODIS. MSI (the Sentinel-2 MultiSpectral
    Instrument) only provides imagery.
    """

    VIIRS = "VIIRS"
    MODIS = "MODIS"
    MSI = "MSI"


class FirmsProduct(StrEnum):
    """FIRMS near-real-time products; values are the FIRMS API product names."""

    VIIRS_SNPP_NRT = "VIIRS_SNPP_NRT"
    VIIRS_NOAA20_NRT = "VIIRS_NOAA20_NRT"
    VIIRS_NOAA21_NRT = "VIIRS_NOAA21_NRT"
    MODIS_NRT = "MODIS_NRT"

    @property
    def instrument(self) -> "FirmsInstrumentLiteral":
        match self:
            case (
                FirmsProduct.VIIRS_SNPP_NRT
                | FirmsProduct.VIIRS_NOAA20_NRT
                | FirmsProduct.VIIRS_NOAA21_NRT
            ):
                return Instrument.VIIRS
            case FirmsProduct.MODIS_NRT:
                return Instrument.MODIS


class WeatherProvider(StrEnum):
    OPEN_METEO = "open-meteo"


class ImageryProvider(StrEnum):
    PLANETARY_COMPUTER = "planetary-computer"


class ImageryLayer(StrEnum):
    """Imagery layers of fire incidents.

    The Sentinel-2 layer is a 20 m short-wave infrared composite (bands B12, B8A,
    B04) of individual Sentinel-2 L2A passes, every 2-5 days: active flames show
    bright red to yellow, fresh burn scars dark red-brown, vegetation green, and
    it sees through smoke (not cloud). It is the layer for confirming a fire.
    """

    SENTINEL2_SWIR = "Sentinel-2_L2A_SWIR_B12-B8A-B04"

    @property
    def provider(self) -> ImageryProvider:
        match self:
            case ImageryLayer.SENTINEL2_SWIR:
                return ImageryProvider.PLANETARY_COMPUTER

    @property
    def instrument(self) -> Instrument:
        match self.provider:
            case ImageryProvider.PLANETARY_COMPUTER:
                return Instrument.MSI

    @property
    def platform(self) -> str:
        """The constellation (scenes name the satellite)."""
        match self.provider:
            case ImageryProvider.PLANETARY_COMPUTER:
                return "Sentinel-2"


# Sentinel-2 passes up to this many days before an incident's first and after its last
# day of activity are acquired and shown: the earlier ones as a baseline, the later
# ones for the burn scar when no pass caught the fire burning.
SENTINEL_CONTEXT_DAYS = 6


class IncidentStatus(StrEnum):
    """Lifecycle of a fire incident; see ``FireIncident``."""

    NEW = auto()
    GROWING = auto()
    STABLE = auto()
    INACTIVE = auto()
    MERGED = auto()

    @property
    def is_final(self) -> bool:
        """Final incidents never change again."""
        return self in (IncidentStatus.INACTIVE, IncidentStatus.MERGED)


class RiskLevel(StrEnum):
    """Bands of the fire risk score; see ``FireRisk``."""

    LOW = auto()
    MODERATE = auto()
    HIGH = auto()
    EXTREME = auto()


# Event fields annotate enums as a Literal of every member. Pydantic renders an Enum
# as a ``$ref`` into ``$defs``, which would change the registered JSON Schemas; a
# Literal of the members renders inline, exactly like the original string Literals.
# Validated values are still enum members, and type checkers treat each Literal as
# its enum. ``test_wire_literals_cover_their_enums`` keeps them complete.
FirmsInstrumentLiteral = Literal[Instrument.VIIRS, Instrument.MODIS]
InstrumentLiteral = Literal[Instrument.VIIRS, Instrument.MODIS, Instrument.MSI]
FirmsProductLiteral = Literal[
    FirmsProduct.VIIRS_SNPP_NRT,
    FirmsProduct.VIIRS_NOAA20_NRT,
    FirmsProduct.VIIRS_NOAA21_NRT,
    FirmsProduct.MODIS_NRT,
]
WeatherProviderLiteral = Literal[WeatherProvider.OPEN_METEO]
RiskLevelLiteral = Literal[RiskLevel.LOW, RiskLevel.MODERATE, RiskLevel.HIGH, RiskLevel.EXTREME]
ImageryProviderLiteral = Literal[ImageryProvider.PLANETARY_COMPUTER]
ImageryLayerLiteral = Literal[ImageryLayer.SENTINEL2_SWIR]
IncidentStatusLiteral = Literal[
    IncidentStatus.NEW,
    IncidentStatus.GROWING,
    IncidentStatus.STABLE,
    IncidentStatus.INACTIVE,
    IncidentStatus.MERGED,
]


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
    product: FirmsProduct, satellite: str, observed_at: datetime, latitude: str, longitude: str
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


def scene_id(layer: ImageryLayer, acquisition_date: date, tile: str) -> uuid.UUID:
    """Deterministic identity of one image tile of a layer on one day.

    Neighbouring incidents that need the same tile share the scene, and the imagery
    producer recognises scenes it already acquired.
    """
    return uuid.uuid5(_EVENT_NAMESPACE, f"scene|{layer}|{acquisition_date.isoformat()}|{tile}")


class FirmsDetection(BaseModel):
    """One satellite active-fire pixel from a NASA FIRMS NRT product."""

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", allow_inf_nan=False, title="FirmsDetection"
    )

    event_id: uuid.UUID
    product: FirmsProductLiteral
    instrument: FirmsInstrumentLiteral
    satellite: str = Field(min_length=1, max_length=32)
    observed_at: AwareDatetime
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    h3_cell: str = Field(description="Resolution-2 H3 cell; also the Kafka message key")
    frp_mw: float | None = Field(default=None, description="Fire radiative power in MW")
    attributes: dict[str, str] = Field(description="Provider CSV row, unmodified")
    ingested_at: AwareDatetime

    @field_validator("observed_at", "ingested_at")
    @classmethod
    def _normalize_times(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("satellite")
    @classmethod
    def satellite_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("satellite must not be blank")
        return value.strip()

    @model_validator(mode="after")
    def consistent(self) -> "FirmsDetection":
        if self.product.instrument is not self.instrument:
            raise ValueError(f"{self.product} is not a {self.instrument} product")
        if not is_partition_cell(self.h3_cell):
            raise ValueError("h3_cell must be a resolution-2 H3 cell")
        if partition_cell(self.latitude, self.longitude) != self.h3_cell:
            raise ValueError("h3_cell does not contain the detection")
        return self


class HourlyWeather(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", allow_inf_nan=False, title="HourlyWeather"
    )

    valid_at: AwareDatetime
    temperature_c: float | None
    precipitation_mm: float | None = Field(ge=0)
    wind_speed_kmh: float | None = Field(ge=0)
    wind_direction_degrees: float | None = Field(ge=0, le=360)

    @field_validator("valid_at")
    @classmethod
    def _normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)


class WeatherForecast(BaseModel):
    """Latest hourly forecast for one H3 cell; the compacted topic keeps one per cell."""

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", allow_inf_nan=False, title="WeatherForecast"
    )

    cell: str = Field(description="Resolution-2 H3 cell; also the Kafka message key")
    latitude: float = Field(ge=-90, le=90, description="Cell center used for the forecast")
    longitude: float = Field(ge=-180, le=180)
    provider: WeatherProviderLiteral
    model: str = Field(min_length=1)
    generated_at: AwareDatetime
    hourly: list[HourlyWeather] = Field(min_length=1)

    @field_validator("generated_at")
    @classmethod
    def _normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def consistent(self) -> "WeatherForecast":
        if not is_partition_cell(self.cell):
            raise ValueError("cell must be a resolution-2 H3 cell")
        times = [hour.valid_at for hour in self.hourly]
        if any(later <= earlier for earlier, later in pairwise(times)):
            raise ValueError("hourly values must be in strictly increasing time order")
        return self


class IncidentDay(BaseModel):
    """One UTC day of an incident's activity."""

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", allow_inf_nan=False, title="IncidentDay"
    )

    day: date
    detections: int = Field(ge=0)
    new_cells: int = Field(ge=0, description="Footprint cells first detected on this day")
    max_frp_mw: float | None = None


class FireIncident(BaseModel):
    """Latest state of a fire incident: detections clustered by space and time.

    Each change produces a new revision under the incident ID (the Kafka key).
    ``merged`` and ``inactive`` are final: an incident in either state never
    changes again.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", allow_inf_nan=False, title="FireIncident"
    )

    incident_id: uuid.UUID = Field(description="Also the Kafka message key")
    revision: int = Field(ge=1, description="Increases by one with every published change")
    status: IncidentStatusLiteral
    previous_status: IncidentStatusLiteral | None = None
    merged_into: uuid.UUID | None = Field(
        default=None, description="The incident that absorbed this one (status merged)"
    )
    h3_cell: str = Field(description="Resolution-2 H3 cell that contains every detection")
    first_observed_at: AwareDatetime
    last_observed_at: AwareDatetime
    updated_at: AwareDatetime = Field(
        description="Stream time (newest observation seen by the processor) at this revision"
    )
    detection_count: int = Field(ge=1)
    footprint_cells: int = Field(ge=1, description="Distinct resolution-7 H3 cells detected")
    footprint_km2: float = Field(ge=0, description="footprint_cells times the mean cell area")
    latitude: float = Field(ge=-90, le=90, description="Mean detection latitude")
    longitude: float = Field(ge=-180, le=180, description="Mean detection longitude")
    west: float = Field(ge=-180, le=180)
    south: float = Field(ge=-90, le=90)
    east: float = Field(ge=-180, le=180)
    north: float = Field(ge=-90, le=90)
    max_frp_mw: float | None = None
    total_frp_mw: float = Field(ge=0, description="Sum of the detections' known FRP")
    products: list[FirmsProductLiteral]
    daily: list[IncidentDay] = Field(description="Activity per UTC day, oldest first")

    @field_validator("first_observed_at", "last_observed_at", "updated_at")
    @classmethod
    def _normalize_times(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def consistent(self) -> "FireIncident":
        if not is_partition_cell(self.h3_cell):
            raise ValueError("h3_cell must be a resolution-2 H3 cell")
        if self.last_observed_at < self.first_observed_at:
            raise ValueError("last_observed_at is before first_observed_at")
        if (self.status is IncidentStatus.MERGED) != (self.merged_into is not None):
            raise ValueError("merged_into is required for, and only for, merged incidents")
        return self


class ObservedWeather(BaseModel):
    """Forecast weather for a detection's H3 cell at the hour nearest its observation."""

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", allow_inf_nan=False, title="ObservedWeather"
    )

    valid_at: AwareDatetime = Field(description="Forecast hour nearest to the observation")
    forecast_generated_at: AwareDatetime = Field(
        description="Retrieval time of the forecast that supplied this hour"
    )
    temperature_c: float | None
    precipitation_mm: float | None = Field(ge=0)
    precipitation_24h_mm: float | None = Field(
        ge=0, description="Precipitation over the 24 hours ending at valid_at; null if incomplete"
    )
    wind_speed_kmh: float | None = Field(ge=0)
    wind_direction_degrees: float | None = Field(ge=0, le=360)

    @field_validator("valid_at", "forecast_generated_at")
    @classmethod
    def _normalize_times(cls, value: datetime) -> datetime:
        return _utc(value)


class FireRisk(BaseModel):
    """Points-based fire risk indicator; each factor scores 0-3, the total 0-12.

    The rules are documented in docs/operations/event-pipeline.md. It is a simple
    screening heuristic, not a calibrated fire danger rating.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", allow_inf_nan=False, title="FireRisk"
    )

    score: int = Field(ge=0, le=12)
    level: RiskLevelLiteral
    wind_points: int = Field(ge=0, le=3)
    temperature_points: int = Field(ge=0, le=3)
    dryness_points: int = Field(ge=0, le=3)
    intensity_points: int = Field(ge=0, le=3)

    @model_validator(mode="after")
    def consistent(self) -> "FireRisk":
        points = (
            self.wind_points + self.temperature_points + self.dryness_points + self.intensity_points
        )
        if points != self.score:
            raise ValueError("score must be the sum of the factor points")
        return self


class EnrichedDetection(BaseModel):
    """A detection with its country, the weather at observation time, and its fire risk.

    Keyed like the source detection (resolution-2 H3 cell), so it is co-partitioned
    with ``firms.detections`` and ``weather.forecasts``.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", allow_inf_nan=False, title="EnrichedDetection"
    )

    detection: FirmsDetection
    country: CountryLiteral | None = Field(
        description="Selected country containing the detection, if any"
    )
    weather: ObservedWeather | None = Field(
        description="Null when no forecast covered the observation hour"
    )
    risk: FireRisk | None = Field(description="Null when the weather needed for it is missing")


class HourlyDetectionStats(BaseModel):
    """Detections and FRP in one country during one UTC hour of observation time.

    Each update replaces the previous value for the same key (``<country>|<hour_start>``)
    in the compacted topic, so the latest record is the running total.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", allow_inf_nan=False, title="HourlyDetectionStats"
    )

    country: CountryLiteral
    hour_start: AwareDatetime
    detections: int = Field(ge=1)
    detections_with_frp: int = Field(ge=0)
    total_frp_mw: float = Field(ge=0, description="Sum of the known FRP values")
    max_frp_mw: float | None = Field(default=None, ge=0)

    @field_validator("hour_start")
    @classmethod
    def _normalize_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def consistent(self) -> "HourlyDetectionStats":
        if self.hour_start != self.hour_start.replace(minute=0, second=0, microsecond=0):
            raise ValueError("hour_start must be on the hour")
        if self.detections_with_frp > self.detections:
            raise ValueError("detections_with_frp exceeds detections")
        if (self.max_frp_mw is None) != (self.detections_with_frp == 0):
            raise ValueError("max_frp_mw is required exactly when some FRP is known")
        return self

    @property
    def key(self) -> str:
        return f"{self.country}|{self.hour_start.isoformat()}"


# Relative, slash-separated path whose segments do not start with a dot, so it can
# be neither absolute nor contain "." or ".." segments.
STORAGE_KEY_PATTERN = r"^[A-Za-z0-9_-][A-Za-z0-9._-]*(?:/[A-Za-z0-9_-][A-Za-z0-9._-]*)*$"


class SatelliteScene(BaseModel):
    """One satellite image tile in the imagery store, with its time and geography.

    The event is a claim check: the image itself is written to the imagery store
    under ``storage_key`` before the event is published, and is never changed (keys
    contain the content hash). Keyed by ``scene_id``; a later acquisition of the
    same scene replaces the earlier one in the compacted topic.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid", allow_inf_nan=False, title="SatelliteScene"
    )

    scene_id: uuid.UUID = Field(description="Also the Kafka message key; see scene_id()")
    provider: ImageryProviderLiteral
    layer: ImageryLayerLiteral
    instrument: InstrumentLiteral
    platform: str = Field(min_length=1, max_length=32)
    acquisition_date: date = Field(description="UTC day of the pass (see acquired_at)")
    tile: str = Field(
        pattern=r"^x-?\d+y-?\d+$",
        description="Grid tile 'x<column>y<row>' of the imagery producer's tile grid",
    )
    west: float = Field(ge=-180, le=180)
    south: float = Field(ge=-90, le=90)
    east: float = Field(ge=-180, le=180)
    north: float = Field(ge=-90, le=90)
    width: int = Field(ge=1, description="Image width in pixels")
    height: int = Field(ge=1, description="Image height in pixels")
    media_type: Literal["image/webp"]
    byte_size: int = Field(ge=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    storage_key: str = Field(
        pattern=STORAGE_KEY_PATTERN, description="Path of the image in the imagery store"
    )
    source_url: str = Field(min_length=1, description="Request that returned the image")
    retrieved_at: AwareDatetime
    acquired_at: AwareDatetime | None = Field(default=None, description="Sensing time of the pass")
    cloud_cover: float | None = Field(
        default=None,
        ge=0,
        le=100,
        description="Cloud cover of the source granules in percent (not of this tile alone)",
    )
    granules: list[str] = Field(
        default_factory=list,
        max_length=16,
        description="Provider IDs of the source granules composited into the image",
    )

    @field_validator("retrieved_at", "acquired_at")
    @classmethod
    def _normalize_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _utc(value)

    @model_validator(mode="after")
    def consistent(self) -> "SatelliteScene":
        if self.west >= self.east or self.south >= self.north:
            raise ValueError("bounds must have west < east and south < north")
        if self.layer.instrument is not self.instrument:
            raise ValueError(f"{self.layer} is not a {self.instrument} layer")
        if self.layer.provider is not self.provider:
            raise ValueError(f"{self.layer} is not a {self.provider} layer")
        if self.acquired_at is not None and self.acquired_at.date() != self.acquisition_date:
            raise ValueError("acquired_at is not on the acquisition date (UTC)")
        if scene_id(self.layer, self.acquisition_date, self.tile) != self.scene_id:
            raise ValueError("scene_id does not match the layer, date, and tile")
        return self
