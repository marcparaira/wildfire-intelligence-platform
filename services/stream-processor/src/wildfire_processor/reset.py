"""Reset one stream-processor job so it reprocesses its input from the start.

Run only while the stream processor is stopped. The command deletes the job's
consumer group and its Quix internal topics (repartition and state changelog);
the processor keeps its local state in a tmpfs, so a restart starts empty. On its
next start the job consumes its input topics from the earliest offset.

``--job`` defaults to ``incidents``. Output topics are kept: incident IDs and
revisions are deterministic, so a replay republishes the same records and
compaction keeps the latest of each.
"""

import argparse
import logging
import sys
from typing import cast

from kafka import KafkaAdminClient
from kafka.errors import GroupIdNotFoundError, NoError

from wildfire_events.kafka import admin_client
from wildfire_events.topics import CONSUMER_GROUPS
from wildfire_processor.settings import Job, Settings

LOGGER = logging.getLogger(__name__)
DELETED_ERRNOS = (NoError.errno, GroupIdNotFoundError.errno)
# Kafka consumer-group states in which no member is consuming.
STOPPED_GROUP_STATES = frozenset({"Empty", "Dead"})
ADMIN_CONNECT_ATTEMPTS = 3


class ResetRefused(RuntimeError):
    """The reset cannot proceed safely; nothing further was changed."""


class Arguments(argparse.Namespace):
    yes: bool = False
    job: Job = Job.INCIDENTS


def internal_topics(topics: list[str], consumer_group: str) -> list[str]:
    """Quix's repartition and changelog topics for ``consumer_group``."""
    prefixes = (f"repartition__{consumer_group}--", f"changelog__{consumer_group}--")
    return sorted(topic for topic in topics if topic.startswith(prefixes))


def delete_consumer_group(admin: KafkaAdminClient, group: str) -> None:
    """Delete the group; raises ``ResetRefused`` while the processor is running."""
    [described] = admin.describe_consumer_groups([group])
    if described.state not in STOPPED_GROUP_STATES:
        raise ResetRefused(f"Stop the stream processor first; {group} is {described.state}")
    for _, error in admin.delete_consumer_groups([group]):
        if cast(int | None, getattr(error, "errno", None)) not in DELETED_ERRNOS:
            raise ResetRefused(f"Could not delete consumer group {group}: {error}")
    LOGGER.info("Deleted consumer group %s", group)


def delete_internal_topics(admin: KafkaAdminClient, group: str) -> None:
    internal = internal_topics(admin.list_topics(), group)
    if not internal:
        return
    admin.delete_topics(internal)  # pyright: ignore[reportUnknownMemberType] -- untyped in kafka-python
    LOGGER.info("Deleted internal topics: %s", ", ".join(internal))


def reset(settings: Settings) -> None:
    group = settings.consumer_group
    admin = admin_client(settings.kafka, attempts=ADMIN_CONNECT_ATTEMPTS)
    try:
        delete_consumer_group(admin, group)
        delete_internal_topics(admin, group)
    finally:
        admin.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--yes", action="store_true", help="Confirm the reset")
    _ = parser.add_argument(
        "--job", type=Job, choices=list(Job), default=Job.INCIDENTS, help="Job to reset"
    )
    args = parser.parse_args(namespace=Arguments())
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not args.yes:
        parser.error("this discards the processor's state; pass --yes to confirm")
    settings = Settings.from_env(args.job)
    try:
        reset(settings)
    except ResetRefused as exc:
        LOGGER.error("%s", exc)  # noqa: TRY400 -- an expected refusal, not a crash
        sys.exit(1)
    sources = ", ".join(CONSUMER_GROUPS.get(args.job.default_group, ()))
    LOGGER.info("Start the stream processor to rerun the %s job from %s", args.job, sources)


if __name__ == "__main__":
    main()
