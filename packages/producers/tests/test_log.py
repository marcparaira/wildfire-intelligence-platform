"""Kafka integration tests of the topic readers.

Skipped unless PRODUCERS_TEST_KAFKA is set.
"""

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from kafka.admin import KafkaAdminClient, NewTopic

from wildfire_events.kafka import idempotent_producer
from wildfire_producers.log import latest_values

KAFKA = os.environ.get("PRODUCERS_TEST_KAFKA", "")


@pytest.mark.skipif(not KAFKA, reason="set PRODUCERS_TEST_KAFKA")
def test_latest_values_reads_a_topic_like_a_table() -> None:
    topic = f"producers-test-{uuid.uuid4().hex[:12]}"
    admin = KafkaAdminClient(bootstrap_servers=KAFKA)
    admin.create_topics([NewTopic(topic, 3, 1)])  # pyright: ignore[reportUnknownMemberType]
    producer = idempotent_producer(KAFKA, "producers-test")
    try:
        assert latest_values(KAFKA, topic) == {}
        for key, value in [(b"a", b"1"), (b"b", b"1"), (b"a", b"2"), (b"b", None), (None, b"x")]:
            _ = producer.send(topic, key=key, value=value)
        producer.flush()
        assert latest_values(KAFKA, topic) == {b"a": b"2", b"b": None}
        future = datetime.now(UTC) + timedelta(hours=1)
        assert latest_values(KAFKA, topic, since=future) == {}
    finally:
        producer.close()
        admin.delete_topics([topic])  # pyright: ignore[reportUnknownMemberType]
        admin.close()
