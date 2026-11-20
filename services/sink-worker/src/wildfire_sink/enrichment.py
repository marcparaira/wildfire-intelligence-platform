"""PostGIS projection of ``detections.enriched`` into ``detection_conditions``.

One row per detection (``event_id``, the same ID as in ``fire_detections``) holds its
country, the forecast weather at observation time, and its fire risk. The table is
not linked to ``fire_detections`` by a foreign key: the two are filled by independent
jobs that can run in either order. A re-enriched detection replaces its row.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import LiteralString

import psycopg
from kafka.consumer.fetcher import ConsumerRecord

from wildfire_events.codec import EventCodec
from wildfire_events.models import EnrichedDetection
from wildfire_events.registry import SubjectRegistry
from wildfire_events.topics import DETECTIONS_ENRICHED, SINK_ENRICHMENT_GROUP, value_subject
from wildfire_sink.runner import initialize_schema

TABLE = "detection_conditions"

SCHEMA: list[LiteralString] = [
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        event_id uuid PRIMARY KEY,
        h3_cell text NOT NULL,
        observed_at timestamptz NOT NULL,
        country text,
        weather_valid_at timestamptz,
        forecast_generated_at timestamptz,
        temperature_c double precision,
        precipitation_mm double precision,
        precipitation_24h_mm double precision,
        wind_speed_kmh double precision,
        wind_direction_degrees double precision,
        risk_score smallint,
        risk_level text,
        wind_points smallint,
        temperature_points smallint,
        dryness_points smallint,
        intensity_points smallint,
        stored_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    f"CREATE INDEX IF NOT EXISTS {TABLE}_observed_at_idx ON {TABLE} (observed_at)",
]

COLUMNS: tuple[str, ...] = (
    "event_id",
    "h3_cell",
    "observed_at",
    "country",
    "weather_valid_at",
    "forecast_generated_at",
    "temperature_c",
    "precipitation_mm",
    "precipitation_24h_mm",
    "wind_speed_kmh",
    "wind_direction_degrees",
    "risk_score",
    "risk_level",
    "wind_points",
    "temperature_points",
    "dryness_points",
    "intensity_points",
)

UPSERT = f"""
INSERT INTO {TABLE} (
    event_id, h3_cell, observed_at, country, weather_valid_at, forecast_generated_at,
    temperature_c, precipitation_mm, precipitation_24h_mm, wind_speed_kmh,
    wind_direction_degrees, risk_score, risk_level, wind_points, temperature_points,
    dryness_points, intensity_points, stored_at
) VALUES (
    %(event_id)s, %(h3_cell)s, %(observed_at)s, %(country)s, %(weather_valid_at)s,
    %(forecast_generated_at)s, %(temperature_c)s, %(precipitation_mm)s,
    %(precipitation_24h_mm)s, %(wind_speed_kmh)s, %(wind_direction_degrees)s,
    %(risk_score)s, %(risk_level)s, %(wind_points)s, %(temperature_points)s,
    %(dryness_points)s, %(intensity_points)s, now()
)
ON CONFLICT (event_id) DO UPDATE SET
    h3_cell = EXCLUDED.h3_cell,
    observed_at = EXCLUDED.observed_at,
    country = EXCLUDED.country,
    weather_valid_at = EXCLUDED.weather_valid_at,
    forecast_generated_at = EXCLUDED.forecast_generated_at,
    temperature_c = EXCLUDED.temperature_c,
    precipitation_mm = EXCLUDED.precipitation_mm,
    precipitation_24h_mm = EXCLUDED.precipitation_24h_mm,
    wind_speed_kmh = EXCLUDED.wind_speed_kmh,
    wind_direction_degrees = EXCLUDED.wind_direction_degrees,
    risk_score = EXCLUDED.risk_score,
    risk_level = EXCLUDED.risk_level,
    wind_points = EXCLUDED.wind_points,
    temperature_points = EXCLUDED.temperature_points,
    dryness_points = EXCLUDED.dryness_points,
    intensity_points = EXCLUDED.intensity_points,
    stored_at = now()
"""


@dataclass
class EnrichmentProjection:
    """Projection of ``detections.enriched``; see the module docstring."""

    registry: SubjectRegistry
    name: str = "enrichment"
    topic: str = DETECTIONS_ENRICHED.name
    group_id: str = SINK_ENRICHMENT_GROUP
    tombstones: bool = False
    codec: EventCodec[EnrichedDetection] = field(init=False)

    def __post_init__(self) -> None:
        self.codec = EventCodec(EnrichedDetection, value_subject(self.topic), self.registry)

    def initialize(self, connection: psycopg.Connection) -> None:
        initialize_schema(connection, SCHEMA)

    def apply(
        self,
        connection: psycopg.Connection,
        events: Sequence[tuple[ConsumerRecord, EnrichedDetection | None]],
    ) -> None:
        rows = [row_parameters(event) for _, event in events if event is not None]
        if rows:
            with connection.cursor() as cursor:
                cursor.executemany(UPSERT, rows)


def row_parameters(event: EnrichedDetection) -> dict[str, object]:
    """Flatten an enriched detection into ``detection_conditions`` columns (nulls when absent)."""
    detection, weather, risk = event.detection, event.weather, event.risk
    row: dict[str, object] = {}
    row.update(dict.fromkeys(COLUMNS))
    row.update(
        event_id=detection.event_id,
        h3_cell=detection.h3_cell,
        observed_at=detection.observed_at,
        country=event.country,
    )
    if weather is not None:
        row.update(
            weather_valid_at=weather.valid_at,
            forecast_generated_at=weather.forecast_generated_at,
            temperature_c=weather.temperature_c,
            precipitation_mm=weather.precipitation_mm,
            precipitation_24h_mm=weather.precipitation_24h_mm,
            wind_speed_kmh=weather.wind_speed_kmh,
            wind_direction_degrees=weather.wind_direction_degrees,
        )
    if risk is not None:
        row.update(
            risk_score=risk.score,
            risk_level=risk.level,
            wind_points=risk.wind_points,
            temperature_points=risk.temperature_points,
            dryness_points=risk.dryness_points,
            intensity_points=risk.intensity_points,
        )
    return row
