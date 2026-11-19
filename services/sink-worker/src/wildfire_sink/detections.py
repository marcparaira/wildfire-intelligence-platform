"""PostGIS projection of ``firms.detections``.

Deduplication happens at two levels:

* Same detection, delivered again (re-polled FIRMS window, producer retry, replay):
  the deterministic ``event_id`` is unique, so the row is stored once.
* Same fire seen by different satellites at nearly the same time (for example
  Aqua MODIS and Suomi-NPP VIIRS, which share an orbit): every observation is kept,
  but the lower-ranked one gets ``duplicate_of`` pointing at the best-ranked match.
  Rank is VIIRS (375 m) before MODIS (1 km), then earlier observation, then event ID.
  The rule is order independent: whatever order events arrive in, each row ends up
  pointing at its best-ranked better match, so replays rebuild the same projection.
  Matches always share an H3 partition key in practice and therefore a partition,
  which preserves their relative order on replay as well.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import psycopg
from kafka.consumer.fetcher import ConsumerRecord
from psycopg.types.json import Jsonb

from wildfire_events.codec import EventCodec
from wildfire_events.models import FirmsDetection
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import FIRMS_DETECTIONS, value_subject

MATCH_DISTANCE_METERS = 1000
MATCH_WINDOW = timedelta(minutes=20)
SENSOR_RANK = {"VIIRS": 0, "MODIS": 1}
TABLE = "fire_detections"

SCHEMA = [
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        event_id uuid NOT NULL UNIQUE,
        product text NOT NULL,
        instrument text NOT NULL,
        satellite text NOT NULL,
        sensor_rank smallint NOT NULL,
        observed_at timestamptz NOT NULL,
        latitude double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
        longitude double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
        location geometry(Point, 4326) NOT NULL,
        h3_cell text NOT NULL,
        frp_mw double precision,
        attributes jsonb NOT NULL,
        ingested_at timestamptz NOT NULL,
        duplicate_of uuid,
        stored_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    f"CREATE INDEX IF NOT EXISTS {TABLE}_location_idx ON {TABLE} USING gist (location)",
    f"CREATE INDEX IF NOT EXISTS {TABLE}_observed_at_idx ON {TABLE} (observed_at DESC, id DESC)",
]

INSERT = f"""
INSERT INTO {TABLE} (
    event_id, product, instrument, satellite, sensor_rank, observed_at, latitude, longitude,
    location, h3_cell, frp_mw, attributes, ingested_at
) VALUES (
    %(event_id)s, %(product)s, %(instrument)s, %(satellite)s, %(sensor_rank)s, %(observed_at)s,
    %(latitude)s, %(longitude)s, ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326),
    %(h3_cell)s, %(frp_mw)s, %(attributes)s, %(ingested_at)s
)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id
"""

# Rows m and d are "matching" when they come from different satellites, are within
# the time window and distance, and are not the same row.
_MATCH = """
    m.event_id <> d.event_id
    AND m.satellite <> d.satellite
    AND m.observed_at BETWEEN d.observed_at - %(window)s AND d.observed_at + %(window)s
    AND m.location && ST_Expand(d.location, 0.02)
    AND ST_DWithin(m.location::geography, d.location::geography, %(meters)s)
"""

POINT_NEW_TO_BEST_MATCH = f"""
UPDATE {TABLE} AS d
SET duplicate_of = (
    SELECT m.event_id FROM {TABLE} AS m
    WHERE {_MATCH}
      AND (m.sensor_rank, m.observed_at, m.event_id) < (d.sensor_rank, d.observed_at, d.event_id)
    ORDER BY m.sensor_rank, m.observed_at, m.event_id
    LIMIT 1
)
WHERE d.event_id = %(event_id)s
"""

POINT_WORSE_MATCHES_TO_NEW = f"""
UPDATE {TABLE} AS m
SET duplicate_of = d.event_id
FROM {TABLE} AS d
WHERE d.event_id = %(event_id)s
  AND {_MATCH}
  AND (d.sensor_rank, d.observed_at, d.event_id) < (m.sensor_rank, m.observed_at, m.event_id)
  AND (
      m.duplicate_of IS NULL
      OR (d.sensor_rank, d.observed_at, d.event_id) < (
          SELECT t.sensor_rank, t.observed_at, t.event_id FROM {TABLE} AS t
          WHERE t.event_id = m.duplicate_of
      )
  )
"""


@dataclass
class DetectionProjection:
    registry: SchemaRegistry
    name: str = "detections"
    topic: str = FIRMS_DETECTIONS.name
    group_id: str = "sink-worker.postgis.detections"
    tombstones: bool = False
    codec: EventCodec[FirmsDetection] = field(init=False)

    def __post_init__(self) -> None:
        self.codec = EventCodec(FirmsDetection, value_subject(self.topic), self.registry)

    def initialize(self, connection: psycopg.Connection) -> None:
        with connection.transaction():
            for statement in SCHEMA:
                connection.execute(statement)

    def apply(
        self, connection: psycopg.Connection, events: Sequence[tuple[ConsumerRecord, Any]]
    ) -> None:
        inserted = []
        for _, event in events:
            row = connection.execute(INSERT, row_parameters(event)).fetchone()
            if row is not None:
                inserted.append(event.event_id)
        for event_id in inserted:
            parameters = {
                "event_id": event_id,
                "window": MATCH_WINDOW,
                "meters": MATCH_DISTANCE_METERS,
            }
            connection.execute(POINT_NEW_TO_BEST_MATCH, parameters)
            connection.execute(POINT_WORSE_MATCHES_TO_NEW, parameters)


def row_parameters(event: FirmsDetection) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "product": event.product,
        "instrument": event.instrument,
        "satellite": event.satellite,
        "sensor_rank": SENSOR_RANK[event.instrument],
        "observed_at": event.observed_at,
        "latitude": event.latitude,
        "longitude": event.longitude,
        "h3_cell": event.h3_cell,
        "frp_mw": event.frp_mw,
        "attributes": Jsonb(event.attributes),
        "ingested_at": event.ingested_at,
    }
