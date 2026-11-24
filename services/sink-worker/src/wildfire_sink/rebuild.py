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
from kafka import KafkaAdminClient
from kafka.errors import GroupIdNotFoundError, NoError
from psycopg import sql

from wildfire_events.kafka import admin_client
from wildfire_events.topics import SINK_WORKER_GROUPS
from wildfire_sink import detections, enrichment, imagery, incidents, statistics, weather
from wildfire_sink.service import Settings

LOGGER = logging.getLogger(__name__)
TABLES = (
    detections.TABLE,
    *weather.TABLES,
    incidents.TABLE,
    enrichment.TABLE,
    statistics.TABLE,
    imagery.TABLE,
)
GROUPS = list(SINK_WORKER_GROUPS)
# Fail fast: this is an interactive command, not a service waiting for the stack.
KAFKA_CONNECT_ATTEMPTS = 3
# Kafka consumer-group states in which no member is consuming.
STOPPED_GROUP_STATES = frozenset({"Empty", "Dead"})
# kafka-python reports each group's outcome as an error class; NoError means deleted.
DELETED_ERRNOS = (NoError.errno, GroupIdNotFoundError.errno)


class RebuildRefused(RuntimeError):
    """The rebuild cannot proceed safely; nothing after the failed step was changed."""


class Arguments(argparse.Namespace):
    yes: bool = False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--yes", action="store_true", help="Confirm dropping projection tables")
    args = parser.parse_args(namespace=Arguments())
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not args.yes:
        parser.error("this drops the PostGIS projection tables; pass --yes to confirm")
    try:
        rebuild(Settings.from_env())
    except RebuildRefused as exc:
        LOGGER.error("%s", exc)  # noqa: TRY400 -- an expected refusal; a traceback adds nothing
        sys.exit(1)
    LOGGER.info("Start the sink worker to replay all topics into PostGIS")


def rebuild(settings: Settings) -> None:
    """Drop the projection tables and reset the sink worker's consumer groups.

    Raises ``RebuildRefused`` if the sink worker is running or a group cannot be deleted.
    """
    admin = admin_client(settings.kafka, attempts=KAFKA_CONNECT_ATTEMPTS)
    try:
        _ensure_sink_worker_stopped(admin)
        _drop_tables(settings.database_url)
        _delete_consumer_groups(admin)
    finally:
        admin.close()


def _ensure_sink_worker_stopped(admin: KafkaAdminClient) -> None:
    active = [
        group.group
        for group in admin.describe_consumer_groups(GROUPS)
        if group.state not in STOPPED_GROUP_STATES
    ]
    if active:
        raise RebuildRefused(f"Stop the sink worker first; active consumer groups: {active}")


def _drop_tables(database_url: str) -> None:
    with psycopg.connect(database_url) as connection:
        for table in TABLES:
            _ = connection.execute(
                sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table))
            )
    LOGGER.info("Dropped projection tables: %s", ", ".join(TABLES))


def _delete_consumer_groups(admin: KafkaAdminClient) -> None:
    for group, error in admin.delete_consumer_groups(GROUPS):
        if cast(int | None, getattr(error, "errno", None)) not in DELETED_ERRNOS:
            raise RebuildRefused(f"Could not delete consumer group {group}: {error}")
    LOGGER.info("Reset consumer groups: %s", ", ".join(GROUPS))


if __name__ == "__main__":
    main()
