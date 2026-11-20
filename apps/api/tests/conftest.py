"""PostGIS fixtures: a throwaway schema with the sink worker's projection tables.

The DDL mirrors the tables the sink worker creates (the API only reads them).
Tests using ``database`` are skipped unless ``API_TEST_DATABASE_URL`` is set.
"""

import os
import uuid
from collections.abc import Iterator
from typing import LiteralString

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

TABLES: dict[str, LiteralString] = {
    "fire_detections": """
        CREATE TABLE fire_detections (
            id bigint PRIMARY KEY, event_id uuid NOT NULL UNIQUE, product text NOT NULL,
            instrument text NOT NULL, satellite text NOT NULL,
            observed_at timestamptz NOT NULL, latitude double precision NOT NULL,
            longitude double precision NOT NULL, location geometry(Point, 4326) NOT NULL,
            attributes jsonb NOT NULL, duplicate_of uuid
        )
    """,
    "detection_conditions": """
        CREATE TABLE detection_conditions (
            event_id uuid PRIMARY KEY, h3_cell text NOT NULL, observed_at timestamptz NOT NULL,
            country text, weather_valid_at timestamptz, forecast_generated_at timestamptz,
            temperature_c double precision, precipitation_mm double precision,
            precipitation_24h_mm double precision, wind_speed_kmh double precision,
            wind_direction_degrees double precision, risk_score smallint, risk_level text,
            wind_points smallint, temperature_points smallint, dryness_points smallint,
            intensity_points smallint, stored_at timestamptz NOT NULL DEFAULT now()
        )
    """,
    "fire_incidents": """
        CREATE TABLE fire_incidents (
            incident_id uuid PRIMARY KEY, revision integer NOT NULL, status text NOT NULL,
            previous_status text, merged_into uuid, h3_cell text NOT NULL,
            first_observed_at timestamptz NOT NULL, last_observed_at timestamptz NOT NULL,
            updated_at timestamptz NOT NULL, detection_count integer NOT NULL,
            footprint_cells integer NOT NULL, footprint_km2 double precision NOT NULL,
            latitude double precision NOT NULL, longitude double precision NOT NULL,
            location geometry(Point, 4326) NOT NULL, west double precision NOT NULL,
            south double precision NOT NULL, east double precision NOT NULL,
            north double precision NOT NULL, max_frp_mw double precision,
            total_frp_mw double precision NOT NULL, products text[] NOT NULL,
            daily jsonb NOT NULL, stored_at timestamptz NOT NULL DEFAULT now()
        )
    """,
    "weather_cells": """
        CREATE TABLE weather_cells (
            cell text PRIMARY KEY, latitude double precision NOT NULL,
            longitude double precision NOT NULL, boundary geometry(Polygon, 4326) NOT NULL,
            provider text NOT NULL, model text NOT NULL, generated_at timestamptz NOT NULL,
            stored_at timestamptz NOT NULL DEFAULT now()
        )
    """,
    "weather_hourly": """
        CREATE TABLE weather_hourly (
            cell text NOT NULL REFERENCES weather_cells (cell) ON DELETE CASCADE,
            valid_at timestamptz NOT NULL, temperature_c double precision,
            precipitation_mm double precision, wind_speed_kmh double precision,
            wind_direction_degrees double precision, PRIMARY KEY (cell, valid_at)
        )
    """,
    "detection_stats_hourly": """
        CREATE TABLE detection_stats_hourly (
            country text NOT NULL, hour_start timestamptz NOT NULL, detections integer NOT NULL,
            detections_with_frp integer NOT NULL, total_frp_mw double precision NOT NULL,
            max_frp_mw double precision, stored_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (country, hour_start)
        )
    """,
}


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> Iterator[psycopg.Connection]:
    """An autocommit connection to a fresh schema holding every projection table.

    ``DATABASE_URL`` points the API at the same schema; the schema is dropped afterwards.
    """
    database_url = os.environ.get("API_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set API_TEST_DATABASE_URL to run PostGIS integration tests")
    schema = f"api_test_{uuid.uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as db:
        _ = db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            _ = db.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
            for ddl in TABLES.values():
                _ = db.execute(ddl)
            monkeypatch.setenv(
                "DATABASE_URL",
                make_conninfo(database_url, options=f"-csearch_path={schema},public"),
            )
            yield db
        finally:
            _ = db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
