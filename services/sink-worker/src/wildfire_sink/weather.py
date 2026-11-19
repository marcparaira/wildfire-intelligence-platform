"""PostGIS projection of the compacted ``weather.forecasts`` topic.

One row per H3 cell holds the latest forecast's metadata and boundary; hourly values
are replaced whenever a newer forecast for the cell arrives. An older forecast (for
example during a replay that interleaves with live data) never overwrites a newer one.
A tombstone (null value) removes the cell.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import psycopg
from kafka.consumer.fetcher import ConsumerRecord

from wildfire_events.cells import cell_boundary
from wildfire_events.codec import EventCodec
from wildfire_events.models import WeatherForecast
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import WEATHER_FORECASTS, value_subject

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS weather_cells (
        cell text PRIMARY KEY,
        latitude double precision NOT NULL,
        longitude double precision NOT NULL,
        boundary geometry(Polygon, 4326) NOT NULL,
        provider text NOT NULL,
        model text NOT NULL,
        generated_at timestamptz NOT NULL,
        stored_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS weather_cells_boundary_idx ON weather_cells USING gist (boundary)",
    """
    CREATE TABLE IF NOT EXISTS weather_hourly (
        cell text NOT NULL REFERENCES weather_cells (cell) ON DELETE CASCADE,
        valid_at timestamptz NOT NULL,
        temperature_c double precision,
        precipitation_mm double precision,
        wind_speed_kmh double precision,
        wind_direction_degrees double precision,
        PRIMARY KEY (cell, valid_at)
    )
    """,
    "CREATE INDEX IF NOT EXISTS weather_hourly_valid_at_idx ON weather_hourly (valid_at)",
]

UPSERT_CELL = """
INSERT INTO weather_cells (
    cell, latitude, longitude, boundary, provider, model, generated_at, stored_at
) VALUES (
    %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s, %s, %s, now()
)
ON CONFLICT (cell) DO UPDATE SET
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    boundary = EXCLUDED.boundary,
    provider = EXCLUDED.provider,
    model = EXCLUDED.model,
    generated_at = EXCLUDED.generated_at,
    stored_at = now()
WHERE weather_cells.generated_at < EXCLUDED.generated_at
RETURNING cell
"""

INSERT_HOUR = """
INSERT INTO weather_hourly (
    cell, valid_at, temperature_c, precipitation_mm, wind_speed_kmh, wind_direction_degrees
) VALUES (%s, %s, %s, %s, %s, %s)
"""


def boundary_geojson(cell: str) -> str:
    coordinates = ",".join(f"[{lng},{lat}]" for lng, lat in cell_boundary(cell))
    return f'{{"type":"Polygon","coordinates":[[{coordinates}]]}}'


@dataclass
class WeatherProjection:
    registry: SchemaRegistry
    name: str = "weather"
    topic: str = WEATHER_FORECASTS.name
    group_id: str = "sink-worker.postgis.weather"
    tombstones: bool = True
    codec: EventCodec[WeatherForecast] = field(init=False)

    def __post_init__(self) -> None:
        self.codec = EventCodec(WeatherForecast, value_subject(self.topic), self.registry)

    def initialize(self, connection: psycopg.Connection) -> None:
        with connection.transaction():
            for statement in SCHEMA:
                connection.execute(statement)

    def apply(
        self, connection: psycopg.Connection, events: Sequence[tuple[ConsumerRecord, Any]]
    ) -> None:
        for record, forecast in events:
            if forecast is None:
                if record.key:
                    connection.execute(
                        "DELETE FROM weather_cells WHERE cell = %s", (record.key.decode(),)
                    )
                continue
            updated = connection.execute(
                UPSERT_CELL,
                (
                    forecast.cell,
                    forecast.latitude,
                    forecast.longitude,
                    boundary_geojson(forecast.cell),
                    forecast.provider,
                    forecast.model,
                    forecast.generated_at,
                ),
            ).fetchone()
            if updated is None:
                continue
            connection.execute("DELETE FROM weather_hourly WHERE cell = %s", (forecast.cell,))
            with connection.cursor() as cursor:
                cursor.executemany(
                    INSERT_HOUR,
                    [
                        (
                            forecast.cell,
                            hour.valid_at,
                            hour.temperature_c,
                            hour.precipitation_mm,
                            hour.wind_speed_kmh,
                            hour.wind_direction_degrees,
                        )
                        for hour in forecast.hourly
                    ],
                )
