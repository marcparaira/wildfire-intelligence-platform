"""PostGIS projection of the compacted ``stats.hourly`` topic.

One row per country and UTC hour holds the latest running total. Updates for one key
arrive in order (a key always maps to the same partition), so the newest record
simply replaces the row. A tombstone removes the row named by its key.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import LiteralString

import psycopg
from kafka.consumer.fetcher import ConsumerRecord

from wildfire_events.codec import EventCodec
from wildfire_events.countries import Country
from wildfire_events.models import HourlyDetectionStats
from wildfire_events.registry import SubjectRegistry
from wildfire_events.topics import SINK_STATISTICS_GROUP, STATS_HOURLY, value_subject
from wildfire_sink.runner import initialize_schema, record_key

LOGGER = logging.getLogger(__name__)
TABLE = "detection_stats_hourly"
KEY_SEPARATOR = "|"

SCHEMA: list[LiteralString] = [
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        country text NOT NULL,
        hour_start timestamptz NOT NULL,
        detections integer NOT NULL,
        detections_with_frp integer NOT NULL,
        total_frp_mw double precision NOT NULL,
        max_frp_mw double precision,
        stored_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (country, hour_start)
    )
    """,
]

UPSERT = f"""
INSERT INTO {TABLE} (
    country, hour_start, detections, detections_with_frp, total_frp_mw, max_frp_mw, stored_at
) VALUES (%s, %s, %s, %s, %s, %s, now())
ON CONFLICT (country, hour_start) DO UPDATE SET
    detections = EXCLUDED.detections,
    detections_with_frp = EXCLUDED.detections_with_frp,
    total_frp_mw = EXCLUDED.total_frp_mw,
    max_frp_mw = EXCLUDED.max_frp_mw,
    stored_at = now()
"""

DELETE = f"DELETE FROM {TABLE} WHERE country = %s AND hour_start = %s"


@dataclass
class StatisticsProjection:
    """Projection of ``stats.hourly`` into ``detection_stats_hourly``; see the module docstring."""

    registry: SubjectRegistry
    name: str = "statistics"
    topic: str = STATS_HOURLY.name
    group_id: str = SINK_STATISTICS_GROUP
    tombstones: bool = True
    codec: EventCodec[HourlyDetectionStats] = field(init=False)

    def __post_init__(self) -> None:
        self.codec = EventCodec(HourlyDetectionStats, value_subject(self.topic), self.registry)

    def initialize(self, connection: psycopg.Connection) -> None:
        initialize_schema(connection, SCHEMA)

    def apply(
        self,
        connection: psycopg.Connection,
        events: Sequence[tuple[ConsumerRecord, HourlyDetectionStats | None]],
    ) -> None:
        for record, stats in events:
            if stats is None:
                _delete(connection, record)
            else:
                _ = connection.execute(
                    UPSERT,
                    (
                        stats.country,
                        stats.hour_start,
                        stats.detections,
                        stats.detections_with_frp,
                        stats.total_frp_mw,
                        stats.max_frp_mw,
                    ),
                )


def parse_stats_key(key: bytes | None) -> tuple[Country, datetime] | None:
    """The (country, hour start) a ``stats.hourly`` key names, or ``None`` if malformed.

    Keys are ``HourlyDetectionStats.key``: ``<ISO-3 country>|<timezone-aware ISO hour>``.
    """
    if key is None:
        return None
    try:
        country, separator, hour = key.decode().partition(KEY_SEPARATOR)
        hour_start = datetime.fromisoformat(hour)
        if not separator or hour_start.tzinfo is None:
            return None
        return Country(country), hour_start
    except (UnicodeDecodeError, ValueError):
        return None


def _delete(connection: psycopg.Connection, tombstone: ConsumerRecord) -> None:
    parsed = parse_stats_key(record_key(tombstone))
    if parsed is None:
        LOGGER.warning("Ignoring statistics tombstone with key %r", record_key(tombstone))
        return
    country, hour_start = parsed
    _ = connection.execute(DELETE, (country, hour_start))
