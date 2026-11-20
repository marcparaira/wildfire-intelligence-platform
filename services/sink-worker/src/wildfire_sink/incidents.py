"""PostGIS projection of the compacted ``fire.incidents`` topic.

One row per incident holds its latest revision. A revision never replaces a newer
one (for example when a replay interleaves with live data), and a tombstone (null
value) removes the incident.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import LiteralString
from uuid import UUID

import psycopg
from kafka.consumer.fetcher import ConsumerRecord
from psycopg.types.json import Jsonb

from wildfire_events.codec import EventCodec
from wildfire_events.models import FireIncident
from wildfire_events.registry import SubjectRegistry
from wildfire_events.topics import FIRE_INCIDENTS, SINK_INCIDENTS_GROUP, value_subject
from wildfire_sink.runner import initialize_schema, record_key

LOGGER = logging.getLogger(__name__)
TABLE = "fire_incidents"

SCHEMA: list[LiteralString] = [
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        incident_id uuid PRIMARY KEY,
        revision integer NOT NULL,
        status text NOT NULL,
        previous_status text,
        merged_into uuid,
        h3_cell text NOT NULL,
        first_observed_at timestamptz NOT NULL,
        last_observed_at timestamptz NOT NULL,
        updated_at timestamptz NOT NULL,
        detection_count integer NOT NULL,
        footprint_cells integer NOT NULL,
        footprint_km2 double precision NOT NULL,
        latitude double precision NOT NULL,
        longitude double precision NOT NULL,
        location geometry(Point, 4326) NOT NULL,
        west double precision NOT NULL,
        south double precision NOT NULL,
        east double precision NOT NULL,
        north double precision NOT NULL,
        max_frp_mw double precision,
        total_frp_mw double precision NOT NULL,
        products text[] NOT NULL,
        daily jsonb NOT NULL,
        stored_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    f"CREATE INDEX IF NOT EXISTS {TABLE}_location_idx ON {TABLE} USING gist (location)",
    f"""
    CREATE INDEX IF NOT EXISTS {TABLE}_last_observed_idx ON {TABLE} (last_observed_at DESC)
    """,
]

UPSERT = f"""
INSERT INTO {TABLE} (
    incident_id, revision, status, previous_status, merged_into, h3_cell,
    first_observed_at, last_observed_at, updated_at, detection_count, footprint_cells,
    footprint_km2, latitude, longitude, location, west, south, east, north,
    max_frp_mw, total_frp_mw, products, daily, stored_at
) VALUES (
    %(incident_id)s, %(revision)s, %(status)s, %(previous_status)s, %(merged_into)s,
    %(h3_cell)s, %(first_observed_at)s, %(last_observed_at)s, %(updated_at)s,
    %(detection_count)s, %(footprint_cells)s, %(footprint_km2)s, %(latitude)s,
    %(longitude)s, ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326),
    %(west)s, %(south)s, %(east)s, %(north)s, %(max_frp_mw)s, %(total_frp_mw)s,
    %(products)s, %(daily)s, now()
)
ON CONFLICT (incident_id) DO UPDATE SET
    revision = EXCLUDED.revision,
    status = EXCLUDED.status,
    previous_status = EXCLUDED.previous_status,
    merged_into = EXCLUDED.merged_into,
    h3_cell = EXCLUDED.h3_cell,
    first_observed_at = EXCLUDED.first_observed_at,
    last_observed_at = EXCLUDED.last_observed_at,
    updated_at = EXCLUDED.updated_at,
    detection_count = EXCLUDED.detection_count,
    footprint_cells = EXCLUDED.footprint_cells,
    footprint_km2 = EXCLUDED.footprint_km2,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    location = EXCLUDED.location,
    west = EXCLUDED.west,
    south = EXCLUDED.south,
    east = EXCLUDED.east,
    north = EXCLUDED.north,
    max_frp_mw = EXCLUDED.max_frp_mw,
    total_frp_mw = EXCLUDED.total_frp_mw,
    products = EXCLUDED.products,
    daily = EXCLUDED.daily,
    stored_at = now()
WHERE {TABLE}.revision < EXCLUDED.revision
"""

DELETE = f"DELETE FROM {TABLE} WHERE incident_id = %s"


@dataclass
class IncidentProjection:
    """Projection of ``fire.incidents`` into ``fire_incidents``; see the module docstring."""

    registry: SubjectRegistry
    name: str = "incidents"
    topic: str = FIRE_INCIDENTS.name
    group_id: str = SINK_INCIDENTS_GROUP
    tombstones: bool = True
    codec: EventCodec[FireIncident] = field(init=False)

    def __post_init__(self) -> None:
        self.codec = EventCodec(FireIncident, value_subject(self.topic), self.registry)

    def initialize(self, connection: psycopg.Connection) -> None:
        initialize_schema(connection, SCHEMA)

    def apply(
        self,
        connection: psycopg.Connection,
        events: Sequence[tuple[ConsumerRecord, FireIncident | None]],
    ) -> None:
        for record, incident in events:
            if incident is None:
                _delete(connection, record)
            else:
                _ = connection.execute(UPSERT, row_parameters(incident))


def incident_key(key: bytes | None) -> UUID | None:
    """The incident ID a record key names, or ``None`` if it is missing or malformed."""
    if key is None:
        return None
    try:
        return UUID(key.decode())
    except (UnicodeDecodeError, ValueError):
        return None


def _delete(connection: psycopg.Connection, tombstone: ConsumerRecord) -> None:
    incident_id = incident_key(record_key(tombstone))
    if incident_id is None:
        LOGGER.warning("Ignoring incident tombstone with key %r", record_key(tombstone))
        return
    _ = connection.execute(DELETE, (incident_id,))


def row_parameters(incident: FireIncident) -> dict[str, object]:
    return {
        "incident_id": incident.incident_id,
        "revision": incident.revision,
        "status": incident.status,
        "previous_status": incident.previous_status,
        "merged_into": incident.merged_into,
        "h3_cell": incident.h3_cell,
        "first_observed_at": incident.first_observed_at,
        "last_observed_at": incident.last_observed_at,
        "updated_at": incident.updated_at,
        "detection_count": incident.detection_count,
        "footprint_cells": incident.footprint_cells,
        "footprint_km2": incident.footprint_km2,
        "latitude": incident.latitude,
        "longitude": incident.longitude,
        "west": incident.west,
        "south": incident.south,
        "east": incident.east,
        "north": incident.north,
        "max_frp_mw": incident.max_frp_mw,
        "total_frp_mw": incident.total_frp_mw,
        "products": list(incident.products),
        "daily": Jsonb([day.model_dump(mode="json") for day in incident.daily]),
    }
