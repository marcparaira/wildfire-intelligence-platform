"""Kafka connection helpers shared by services."""

import logging
import time
from collections.abc import Iterable

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

from wildfire_events.topics import TopicSpec

LOGGER = logging.getLogger(__name__)


def admin_client(bootstrap_servers: str, attempts: int = 60, delay: float = 5) -> KafkaAdminClient:
    for attempt in range(1, attempts + 1):
        try:
            return KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="wildfire-admin")
        except NoBrokersAvailable:
            if attempt == attempts:
                raise
            LOGGER.warning("Kafka unavailable; retrying in %ss", delay)
            time.sleep(delay)
    raise AssertionError("unreachable")


def ensure_topics(bootstrap_servers: str, specs: Iterable[TopicSpec]) -> None:
    """Create missing topics with their configuration; existing topics are left as they are."""
    admin = admin_client(bootstrap_servers)
    try:
        existing = set(admin.list_topics())
        missing = [spec for spec in specs if spec.name not in existing]
        for spec in missing:
            try:
                admin.create_topics(
                    [
                        NewTopic(
                            name=spec.name,
                            num_partitions=spec.partitions,
                            replication_factor=1,
                            topic_configs=spec.config,
                        )
                    ]
                )
                LOGGER.info("Created topic %s", spec.name)
            except TopicAlreadyExistsError:
                pass
        for spec in specs:
            if spec.name in existing:
                described = admin.describe_topics([spec.name])[0]
                partitions = len(described.get("partitions", []))
                if partitions != spec.partitions:
                    LOGGER.warning(
                        "Topic %s has %d partitions; expected %d (co-partitioning is broken)",
                        spec.name,
                        partitions,
                        spec.partitions,
                    )
    finally:
        admin.close()


def idempotent_producer(bootstrap_servers: str, client_id: str) -> KafkaProducer:
    """A producer whose broker-side retries cannot duplicate or reorder records.

    kafka-python only supports idempotence with one in-flight request per broker.
    """
    for attempt in range(1, 61):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                client_id=client_id,
                enable_idempotence=True,
                acks="all",
                retries=2147483647,
                max_in_flight_requests_per_connection=1,
                linger_ms=20,
                compression_type="gzip",
                request_timeout_ms=30000,
            )
        except NoBrokersAvailable:
            if attempt == 60:
                raise
            LOGGER.warning("Kafka unavailable; retrying in 5s")
            time.sleep(5)
    raise AssertionError("unreachable")
