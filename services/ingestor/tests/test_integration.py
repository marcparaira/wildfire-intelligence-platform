"""Kafka and schema-registry integration test.

Skipped unless INGESTOR_TEST_KAFKA and INGESTOR_TEST_SCHEMA_REGISTRY are set.
"""

import os
import uuid
from datetime import timedelta
from pathlib import Path
from urllib.request import Request, urlopen

import pytest
from kafka.admin import KafkaAdminClient, NewTopic

from wildfire_events.codec import EventCodec
from wildfire_events.kafka import idempotent_producer
from wildfire_events.models import FirmsDetection
from wildfire_events.registry import SchemaRegistry
from wildfire_ingestor.firms import parse_csv
from wildfire_ingestor.log import PublishedEvents, latest_publish_time
from wildfire_ingestor.publisher import Publisher

KAFKA = os.environ.get("INGESTOR_TEST_KAFKA", "")
REGISTRY = os.environ.get("INGESTOR_TEST_SCHEMA_REGISTRY", "")
FIXTURE = Path(__file__).parent / "fixtures" / "viirs_noaa20.csv"


@pytest.mark.skipif(
    not (KAFKA and REGISTRY), reason="set INGESTOR_TEST_KAFKA and INGESTOR_TEST_SCHEMA_REGISTRY"
)
def test_a_restarted_ingestor_does_not_republish_what_is_in_the_log() -> None:
    topic = f"ingestor-test-{uuid.uuid4().hex[:12]}"
    admin = KafkaAdminClient(bootstrap_servers=KAFKA)
    admin.create_topics([NewTopic(topic, 6, 1)])
    producer = idempotent_producer(KAFKA, "ingestor-test")
    codec = EventCodec(FirmsDetection, f"{topic}-value", SchemaRegistry(REGISTRY))
    events = parse_csv(FIXTURE.read_text(), "VIIRS_NOAA20_NRT").detections
    try:
        assert latest_publish_time(KAFKA, topic) is None

        def publisher(published: PublishedEvents) -> Publisher[FirmsDetection]:
            return Publisher(
                producer=producer,
                codec=codec,
                topic=topic,
                key=lambda event: event.h3_cell,
                event_id=lambda event: event.event_id,
                published=published,
            )

        assert publisher(PublishedEvents(timedelta(days=3))).publish(events) == (2, 0)

        restarted = PublishedEvents(timedelta(days=3))
        assert restarted.load(KAFKA, topic) == 2
        assert publisher(restarted).publish(events) == (0, 2)
        assert latest_publish_time(KAFKA, topic) is not None
    finally:
        producer.close()
        admin.delete_topics([topic])
        admin.close()
        for suffix in ("", "?permanent=true"):
            try:
                urlopen(
                    Request(f"{REGISTRY}/subjects/{topic}-value{suffix}", method="DELETE"),
                    timeout=10,
                )
            except OSError:
                pass
