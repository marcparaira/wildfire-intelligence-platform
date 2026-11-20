"""Response models of ``GET /pipeline/status`` and the freshness rules behind them."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum, auto

from pydantic import BaseModel

from wildfire_events.models import FirmsProduct, WeatherProvider
from wildfire_events.topics import FIRMS_DETECTIONS, WEATHER_FORECASTS


class Freshness(StrEnum):
    FRESH = auto()
    DELAYED = auto()
    STALE = auto()
    NO_DATA = auto()
    UNKNOWN = auto()
    """The projection could not be read."""


class SourceKind(StrEnum):
    OBSERVATION = auto()
    FORECAST = auto()


@dataclass(frozen=True, slots=True)
class SourceSpec:
    id: str
    label: str
    kind: SourceKind
    topic: str
    fresh_within: timedelta
    stale_after: timedelta

    def classify(self, latest: datetime | None, now: datetime) -> Freshness:
        if latest is None:
            return Freshness.NO_DATA
        age = now - latest
        if age <= self.fresh_within:
            return Freshness.FRESH
        if age <= self.stale_after:
            return Freshness.DELAYED
        return Freshness.STALE


# Polar orbiters pass over the region about twice a day and FIRMS NRT data arrives
# about three hours after acquisition, so the newest detection of a healthy product
# can be up to ~15 hours old.
FIRMS_FRESH_WITHIN = timedelta(hours=18)
FIRMS_STALE_AFTER = timedelta(hours=36)
FIRMS_LABELS: dict[FirmsProduct, str] = {
    FirmsProduct.VIIRS_SNPP_NRT: "VIIRS S-NPP",
    FirmsProduct.VIIRS_NOAA20_NRT: "VIIRS NOAA-20",
    FirmsProduct.VIIRS_NOAA21_NRT: "VIIRS NOAA-21",
    FirmsProduct.MODIS_NRT: "MODIS Terra/Aqua",
}
# The ingestor refreshes forecasts every three hours by default.
FORECAST_FRESH_WITHIN = timedelta(hours=6)
FORECAST_STALE_AFTER = timedelta(hours=24)

SOURCES = (
    *(
        SourceSpec(
            product,
            FIRMS_LABELS[product],
            SourceKind.OBSERVATION,
            FIRMS_DETECTIONS.name,
            FIRMS_FRESH_WITHIN,
            FIRMS_STALE_AFTER,
        )
        for product in FirmsProduct
    ),
    SourceSpec(
        WeatherProvider.OPEN_METEO,
        "Open-Meteo forecast",
        SourceKind.FORECAST,
        WEATHER_FORECASTS.name,
        FORECAST_FRESH_WITHIN,
        FORECAST_STALE_AFTER,
    ),
)


class SourceStatus(BaseModel):
    id: str
    label: str
    kind: SourceKind
    topic: str
    freshness: Freshness
    latest_at: datetime | None
    """Newest observation time, or for a forecast the newest retrieval time."""
    oldest_retrieved_at: datetime | None = None
    """Forecasts only: the oldest cell forecast's retrieval time."""
    forecast_until: datetime | None = None
    latest_ingested_at: datetime | None
    latest_stored_at: datetime | None
    ingested_last_hour: int | None
    ingested_last_day: int | None
    stored: int | None
    """Detections, or forecast cells, in PostGIS."""
    fresh_within_hours: float
    stale_after_hours: float


class TopicStatus(BaseModel):
    name: str
    partitions: int
    retained: int
    """Offset span of the retained log; compacted topics hold fewer records."""
    messages_last_hour: int
    messages_last_day: int
    dead_letters_last_day: int


class PartitionLag(BaseModel):
    partition: int
    committed: int | None
    end: int
    lag: int


class ConsumerStatus(BaseModel):
    group_id: str
    topic: str
    state: str
    members: int
    lag: int
    partitions: list[PartitionLag]


class DeadLetter(BaseModel):
    failed_at: datetime
    topic: str | None
    partition: int | None
    offset: int | None
    group_id: str | None
    error: str


class DeadLetterStatus(BaseModel):
    topics: list[str]
    """Every service's dead-letter topic."""
    retained: int
    last_day: int
    by_topic: dict[str, int]
    """Dead letters of the last 24 hours per original topic."""
    sample_truncated: bool
    """``by_topic`` only counts the newest ``DEAD_LETTER_SAMPLE`` records."""
    recent: list[DeadLetter]


@dataclass(frozen=True, slots=True)
class KafkaSection:
    topics: list[TopicStatus]
    consumers: list[ConsumerStatus]
    dead_letters: DeadLetterStatus


class PipelineStatus(BaseModel):
    checked_at: datetime
    sources: list[SourceStatus]
    topics: list[TopicStatus] | None
    consumers: list[ConsumerStatus] | None
    dead_letters: DeadLetterStatus | None
    errors: list[str]
