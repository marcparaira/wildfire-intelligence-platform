"""PostGIS projection of the compacted ``imagery.scenes`` topic.

One row per scene holds the metadata of its latest acquisition, with the tile as a
polygon so the API can find the scenes over an incident. The image bytes stay in
the imagery store; ``storage_key`` locates them. An older acquisition never
replaces a newer one, and a tombstone removes the scene.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import LiteralString

import psycopg
from kafka.consumer.fetcher import ConsumerRecord

from wildfire_events.codec import EventCodec
from wildfire_events.models import SatelliteScene
from wildfire_events.registry import SubjectRegistry
from wildfire_events.topics import SATELLITE_SCENES, SINK_IMAGERY_GROUP, value_subject
from wildfire_sink.incidents import incident_key
from wildfire_sink.runner import initialize_schema, record_key

LOGGER = logging.getLogger(__name__)
TABLE = "satellite_scenes"

SCHEMA: list[LiteralString] = [
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        scene_id uuid PRIMARY KEY,
        provider text NOT NULL,
        layer text NOT NULL,
        instrument text NOT NULL,
        platform text NOT NULL,
        acquisition_date date NOT NULL,
        tile text NOT NULL,
        west double precision NOT NULL,
        south double precision NOT NULL,
        east double precision NOT NULL,
        north double precision NOT NULL,
        footprint geometry(Polygon, 4326) NOT NULL,
        width integer NOT NULL,
        height integer NOT NULL,
        media_type text NOT NULL,
        byte_size integer NOT NULL,
        sha256 text NOT NULL,
        storage_key text NOT NULL,
        source_url text NOT NULL,
        retrieved_at timestamptz NOT NULL,
        stored_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    f"CREATE INDEX IF NOT EXISTS {TABLE}_footprint_idx ON {TABLE} USING gist (footprint)",
    f"CREATE INDEX IF NOT EXISTS {TABLE}_date_idx ON {TABLE} (acquisition_date)",
    # Optional SatelliteScene fields added with the Sentinel-2 passes.
    f"""
    ALTER TABLE {TABLE}
        ADD COLUMN IF NOT EXISTS acquired_at timestamptz,
        ADD COLUMN IF NOT EXISTS cloud_cover double precision,
        ADD COLUMN IF NOT EXISTS granules text[] NOT NULL DEFAULT '{{}}'
    """,
]

UPSERT = f"""
INSERT INTO {TABLE} (
    scene_id, provider, layer, instrument, platform, acquisition_date, tile,
    west, south, east, north, footprint, width, height, media_type, byte_size,
    sha256, storage_key, source_url, retrieved_at, acquired_at, cloud_cover, granules,
    stored_at
) VALUES (
    %(scene_id)s, %(provider)s, %(layer)s, %(instrument)s, %(platform)s,
    %(acquisition_date)s, %(tile)s, %(west)s, %(south)s, %(east)s, %(north)s,
    ST_MakeEnvelope(%(west)s, %(south)s, %(east)s, %(north)s, 4326),
    %(width)s, %(height)s, %(media_type)s, %(byte_size)s, %(sha256)s,
    %(storage_key)s, %(source_url)s, %(retrieved_at)s, %(acquired_at)s, %(cloud_cover)s,
    %(granules)s, now()
)
ON CONFLICT (scene_id) DO UPDATE SET
    provider = EXCLUDED.provider,
    layer = EXCLUDED.layer,
    instrument = EXCLUDED.instrument,
    platform = EXCLUDED.platform,
    acquisition_date = EXCLUDED.acquisition_date,
    tile = EXCLUDED.tile,
    west = EXCLUDED.west,
    south = EXCLUDED.south,
    east = EXCLUDED.east,
    north = EXCLUDED.north,
    footprint = EXCLUDED.footprint,
    width = EXCLUDED.width,
    height = EXCLUDED.height,
    media_type = EXCLUDED.media_type,
    byte_size = EXCLUDED.byte_size,
    sha256 = EXCLUDED.sha256,
    storage_key = EXCLUDED.storage_key,
    source_url = EXCLUDED.source_url,
    retrieved_at = EXCLUDED.retrieved_at,
    acquired_at = EXCLUDED.acquired_at,
    cloud_cover = EXCLUDED.cloud_cover,
    granules = EXCLUDED.granules,
    stored_at = now()
WHERE {TABLE}.retrieved_at <= EXCLUDED.retrieved_at
"""

DELETE = f"DELETE FROM {TABLE} WHERE scene_id = %s"


@dataclass
class SceneProjection:
    """Projection of ``imagery.scenes`` into ``satellite_scenes``; see the module docstring."""

    registry: SubjectRegistry
    name: str = "imagery"
    topic: str = SATELLITE_SCENES.name
    group_id: str = SINK_IMAGERY_GROUP
    tombstones: bool = True
    codec: EventCodec[SatelliteScene] = field(init=False)

    def __post_init__(self) -> None:
        self.codec = EventCodec(SatelliteScene, value_subject(self.topic), self.registry)

    def initialize(self, connection: psycopg.Connection) -> None:
        initialize_schema(connection, SCHEMA)

    def apply(
        self,
        connection: psycopg.Connection,
        events: Sequence[tuple[ConsumerRecord, SatelliteScene | None]],
    ) -> None:
        for record, scene in events:
            if scene is None:
                _delete(connection, record)
            else:
                _ = connection.execute(UPSERT, row_parameters(scene))


def _delete(connection: psycopg.Connection, tombstone: ConsumerRecord) -> None:
    # Scene keys are UUIDs, like incident keys.
    scene_id = incident_key(record_key(tombstone))
    if scene_id is None:
        LOGGER.warning("Ignoring scene tombstone with key %r", record_key(tombstone))
        return
    _ = connection.execute(DELETE, (scene_id,))


def row_parameters(scene: SatelliteScene) -> dict[str, object]:
    return {
        "scene_id": scene.scene_id,
        "provider": scene.provider,
        "layer": scene.layer,
        "instrument": scene.instrument,
        "platform": scene.platform,
        "acquisition_date": scene.acquisition_date,
        "tile": scene.tile,
        "west": scene.west,
        "south": scene.south,
        "east": scene.east,
        "north": scene.north,
        "width": scene.width,
        "height": scene.height,
        "media_type": scene.media_type,
        "byte_size": scene.byte_size,
        "sha256": scene.sha256,
        "storage_key": scene.storage_key,
        "source_url": scene.source_url,
        "retrieved_at": scene.retrieved_at,
        "acquired_at": scene.acquired_at,
        "cloud_cover": scene.cloud_cover,
        "granules": scene.granules,
    }
