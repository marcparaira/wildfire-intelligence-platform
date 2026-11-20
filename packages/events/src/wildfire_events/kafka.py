"""Kafka connection helpers shared by services."""

import logging
import time
from collections.abc import Callable, Sequence
from typing import cast

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

from wildfire_events.topics import TopicSpec

LOGGER = logging.getLogger(__name__)
CONNECT_ATTEMPTS = 60
CONNECT_RETRY_SECONDS = 5.0
# The local stack runs a single broker.
REPLICATION_FACTOR = 1
# kafka-python's maximum; the idempotent producer makes unlimited retries safe.
MAX_PRODUCER_RETRIES = 2_147_483_647


def _connect[C](
    connect: Callable[[], C],
    attempts: int = CONNECT_ATTEMPTS,
    delay: float = CONNECT_RETRY_SECONDS,
) -> C:
    """Retry a client constructor while no broker is reachable (e.g. at stack startup)."""
    for attempt in range(1, attempts + 1):
        try:
            return connect()
        except NoBrokersAvailable:
            if attempt == attempts:
                raise
            LOGGER.warning("Kafka unavailable; retrying in %ss", delay)
            time.sleep(delay)
    raise AssertionError("unreachable")


def admin_client(
    bootstrap_servers: str,
    attempts: int = CONNECT_ATTEMPTS,
    delay: float = CONNECT_RETRY_SECONDS,
) -> KafkaAdminClient:
    return _connect(
        lambda: KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="wildfire-admin"),
        attempts,
        delay,
    )


def ensure_topics(bootstrap_servers: str, specs: Sequence[TopicSpec]) -> None:
    """Create missing topics with their configuration; existing topics are left as they are.

    Existing topics with the wrong partition count are logged, not changed: fixing
    them requires recreating the topic and replaying (see ``wildfire_events.topics``).
    """
    admin = admin_client(bootstrap_servers)
    try:
        existing = set(admin.list_topics())
        for spec in specs:
            if spec.name in existing:
                _warn_on_partition_mismatch(admin, spec)
            else:
                _create_topic(admin, spec)
    finally:
        admin.close()


def _create_topic(admin: KafkaAdminClient, spec: TopicSpec) -> None:
    topic = NewTopic(
        name=spec.name,
        num_partitions=spec.partitions,
        replication_factor=REPLICATION_FACTOR,
        topic_configs=spec.config,
    )
    try:
        admin.create_topics([topic])  # pyright: ignore[reportUnknownMemberType] -- untyped in kafka-python
    except TopicAlreadyExistsError:
        # Another service created it since we listed topics; that is the desired state.
        return
    LOGGER.info("Created topic %s", spec.name)


def _warn_on_partition_mismatch(admin: KafkaAdminClient, spec: TopicSpec) -> None:
    described = admin.describe_topics([spec.name])[0]
    partitions = len(cast(list[object], described.get("partitions", [])))
    if partitions != spec.partitions:
        LOGGER.warning(
            "Topic %s has %d partitions; expected %d (co-partitioning is broken)",
            spec.name,
            partitions,
            spec.partitions,
        )


def idempotent_producer(bootstrap_servers: str, client_id: str) -> KafkaProducer:
    """A producer whose broker-side retries cannot duplicate or reorder records.

    kafka-python only supports idempotence with one in-flight request per broker.
    """
    return _connect(
        lambda: KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            enable_idempotence=True,
            acks="all",
            retries=MAX_PRODUCER_RETRIES,
            max_in_flight_requests_per_connection=1,
            linger_ms=20,
            compression_type="gzip",
            request_timeout_ms=30_000,
        )
    )
