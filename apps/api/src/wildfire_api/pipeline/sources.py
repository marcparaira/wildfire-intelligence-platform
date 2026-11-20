"""Source freshness and ingest counts from the PostGIS projections."""

from collections.abc import Iterable
from datetime import datetime, timedelta

from pydantic import BaseModel

from wildfire_api.db import connect, fetch_all
from wildfire_api.pipeline.models import (
    SOURCES,
    Freshness,
    SourceKind,
    SourceSpec,
    SourceStatus,
)

SECONDS_PER_HOUR = 3600


class DetectionStats(BaseModel):
    """Per-product aggregates of ``fire_detections``."""

    product: str
    latest_observed_at: datetime
    latest_ingested_at: datetime
    latest_stored_at: datetime
    ingested_last_hour: int
    ingested_last_day: int
    stored: int


class ForecastStats(BaseModel):
    """Aggregates of ``weather_cells``; times are null while no forecast is stored."""

    newest: datetime | None
    oldest: datetime | None
    latest_stored_at: datetime | None
    forecast_until: datetime | None
    ingested_last_hour: int
    ingested_last_day: int
    stored: int


class _Exists(BaseModel):
    detections: bool
    forecasts: bool


TABLES_EXIST = (
    "SELECT to_regclass('fire_detections') IS NOT NULL AS detections, "
    + "to_regclass('weather_cells') IS NOT NULL "
    + "AND to_regclass('weather_hourly') IS NOT NULL AS forecasts"
)

DETECTION_STATS = """
SELECT product,
       max(observed_at) AS latest_observed_at,
       max(ingested_at) AS latest_ingested_at,
       max(stored_at) AS latest_stored_at,
       count(*) FILTER (WHERE ingested_at >= now() - interval '1 hour') AS ingested_last_hour,
       count(*) FILTER (WHERE ingested_at >= now() - interval '1 day') AS ingested_last_day,
       count(*) AS stored
FROM fire_detections
GROUP BY product
"""

FORECAST_STATS = """
SELECT max(generated_at) AS newest,
       min(generated_at) AS oldest,
       max(stored_at) AS latest_stored_at,
       (SELECT max(valid_at) FROM weather_hourly) AS forecast_until,
       count(*) FILTER (WHERE generated_at >= now() - interval '1 hour') AS ingested_last_hour,
       count(*) FILTER (WHERE generated_at >= now() - interval '1 day') AS ingested_last_day,
       count(*) AS stored
FROM weather_cells
"""


def _hours(value: timedelta) -> float:
    return value.total_seconds() / SECONDS_PER_HOUR


def unknown_source(spec: SourceSpec) -> SourceStatus:
    """A source whose projection could not be read."""
    return SourceStatus(
        id=spec.id,
        label=spec.label,
        kind=spec.kind,
        topic=spec.topic,
        freshness=Freshness.UNKNOWN,
        latest_at=None,
        latest_ingested_at=None,
        latest_stored_at=None,
        ingested_last_hour=None,
        ingested_last_day=None,
        stored=None,
        fresh_within_hours=_hours(spec.fresh_within),
        stale_after_hours=_hours(spec.stale_after),
    )


def _observation_status(spec: SourceSpec, row: DetectionStats | None) -> SourceStatus:
    status = unknown_source(spec)
    status.ingested_last_hour = status.ingested_last_day = status.stored = 0
    if row is not None:
        status.latest_at = row.latest_observed_at
        status.latest_ingested_at = row.latest_ingested_at
        status.latest_stored_at = row.latest_stored_at
        status.ingested_last_hour = row.ingested_last_hour
        status.ingested_last_day = row.ingested_last_day
        status.stored = row.stored
    return status


def _forecast_status(spec: SourceSpec, forecast: ForecastStats | None) -> SourceStatus:
    status = unknown_source(spec)
    status.ingested_last_hour = status.ingested_last_day = status.stored = 0
    if forecast is not None:
        status.latest_at = status.latest_ingested_at = forecast.newest
        status.oldest_retrieved_at = forecast.oldest
        status.forecast_until = forecast.forecast_until
        status.latest_stored_at = forecast.latest_stored_at
        status.ingested_last_hour = forecast.ingested_last_hour
        status.ingested_last_day = forecast.ingested_last_day
        status.stored = forecast.stored
    return status


def source_statuses(
    detections: Iterable[DetectionStats], forecast: ForecastStats | None, now: datetime
) -> list[SourceStatus]:
    """One status per entry of ``SOURCES``; a source without rows has ``no_data``."""
    by_product = {row.product: row for row in detections}
    statuses: list[SourceStatus] = []
    for spec in SOURCES:
        match spec.kind:
            case SourceKind.OBSERVATION:
                status = _observation_status(spec, by_product.get(spec.id))
            case SourceKind.FORECAST:
                status = _forecast_status(spec, forecast)
        status.freshness = spec.classify(status.latest_at, now)
        statuses.append(status)
    return statuses


class PostgisSources:
    """Reads source statuses; ``read`` raises ``psycopg.Error`` if PostGIS is unavailable."""

    def __init__(self, database_url: str) -> None:
        self.database_url: str = database_url

    def read(self, now: datetime) -> list[SourceStatus]:
        with connect(self.database_url) as connection:
            [exists] = fetch_all(connection, _Exists, TABLES_EXIST)
            detections = (
                fetch_all(connection, DetectionStats, DETECTION_STATS) if exists.detections else []
            )
            forecast = (
                fetch_all(connection, ForecastStats, FORECAST_STATS)[0]
                if exists.forecasts
                else None
            )
        return source_statuses(detections, forecast, now)
