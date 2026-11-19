"""Rebuild the PostGIS projections by replaying their topics from the beginning.

Run only while the sink worker is stopped. The command drops the projection tables
and deletes the sink worker's consumer groups; on its next start the worker
recreates the tables and, with no committed offsets, consumes from the earliest
retained offset.
"""

import argparse
import logging
import sys
from typing import cast

import psycopg
from kafka.errors import GroupIdNotFoundError, NoError

from wildfire_events.kafka import admin_client
from wildfire_sink.detections import DetectionProjection
from wildfire_sink.service import Settings
from wildfire_sink.weather import WeatherProjection

LOGGER = logging.getLogger(__name__)
TABLES = ("fire_detections", "weather_hourly", "weather_cells")
GROUPS = [DetectionProjection.group_id, WeatherProjection.group_id]
# kafka-python reports each group's outcome as an error class; NoError means deleted.
DELETED_ERRNOS = (NoError.errno, GroupIdNotFoundError.errno)


class Arguments(argparse.Namespace):
    yes: bool = False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--yes", action="store_true", help="Confirm dropping projection tables"
    )
    args = parser.parse_args(namespace=Arguments())
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not args.yes:
        parser.error("this drops the PostGIS projection tables; pass --yes to confirm")
    settings = Settings.from_env()
    groups = GROUPS

    admin = admin_client(settings.kafka, attempts=3)
    try:
        active = [
            group.group
            for group in admin.describe_consumer_groups(groups)
            if group.state not in ("Empty", "Dead")
        ]
        if active:
            LOGGER.error("Stop the sink worker first; active consumer groups: %s", active)
            sys.exit(1)
        with psycopg.connect(settings.database_url) as connection:
            for table in TABLES:
                _ = connection.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        LOGGER.info("Dropped projection tables: %s", ", ".join(TABLES))
        for group, error in admin.delete_consumer_groups(groups):
            if cast(int | None, getattr(error, "errno", None)) not in DELETED_ERRNOS:
                LOGGER.error("Could not delete consumer group %s: %s", group, error)
                sys.exit(1)
        LOGGER.info("Reset consumer groups: %s", ", ".join(groups))
    finally:
        admin.close()
    LOGGER.info("Start the sink worker to replay all topics into PostGIS")


if __name__ == "__main__":
    main()
