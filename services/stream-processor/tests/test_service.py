import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from http.client import HTTPResponse
from pathlib import Path
from typing import cast
from urllib.request import Request, urlopen

import h3
import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import NoError
from quixstreams.context import set_message_context
from quixstreams.models import MessageContext

from wildfire_events.codec import EventCodec, InvalidEvent
from wildfire_events.models import FireIncident, FirmsDetection
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import TopicSpec, value_subject
from wildfire_processor.adapters import DeadLetters, RecordDecoder, repartition_topic
from wildfire_processor.incidents_job import (
    DetectionDecoder,
    IncidentTopics,
    build_incidents_application,
)
from wildfire_processor.reset import ResetRefused, delete_consumer_group, internal_topics
from wildfire_processor.settings import Job, Settings

from .test_incidents import CENTER, FAR, RING, detection


@dataclass
class FakeProducer:
    sent: list[tuple[str, bytes | None, bytes | None, dict[str, bytes]]] = field(
        default_factory=list
    )

    def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
    ) -> None:
        self.sent.append(
            (topic, cast(bytes | None, key), cast(bytes | None, value), dict(headers or []))
        )

    def flush(self, timeout: float | None = None) -> int:
        del timeout
        return 0


class FakeCodec:
    def decode(self, value: bytes | None) -> FirmsDetection:
        if value != b"good":
            raise InvalidEvent("not a detection")
        return detection(CENTER, 0)


def test_invalid_records_are_dead_lettered_and_dropped() -> None:
    producer = FakeProducer()
    decoder = DetectionDecoder(RecordDecoder(FakeCodec(), DeadLetters(producer)))
    set_message_context(MessageContext("firms.detections", 4, 812, 10, None))

    decoded = decoder(b"good", b"cell", 0, None)
    assert decoded is not None
    assert decoded["product"] == "VIIRS_NOAA20_NRT"
    assert decoder(b"junk", b"cell", 0, [("event_id", b"abc")]) is None

    [(topic, key, value, headers)] = producer.sent
    assert (topic, key, value) == ("stream-processor.dlq", b"cell", b"junk")
    assert headers["event_id"] == b"abc"
    assert headers["dlq.original.partition"] == b"4"
    assert headers["dlq.original.offset"] == b"812"
    assert headers["dlq.error"] == b"not a detection"
    assert headers["dlq.consumer.group"] == b"stream-processor.incidents"


def test_repartition_topic_matches_the_quix_naming_scheme() -> None:
    spec = repartition_topic("group", "firms.detections", "source-partition")
    assert spec.name == "repartition__group--firms.detections--source-partition"
    assert spec.config["retention.ms"] == str(3 * 24 * 60 * 60 * 1000)
    assert spec.config["cleanup.policy"] == "delete"


KAFKA = os.environ.get("PROCESSOR_TEST_KAFKA")
REGISTRY = os.environ.get("PROCESSOR_TEST_SCHEMA_REGISTRY")


@pytest.mark.skipif(
    not (KAFKA and REGISTRY),
    reason="set PROCESSOR_TEST_KAFKA and PROCESSOR_TEST_SCHEMA_REGISTRY to run",
)
def test_pipeline_clusters_detections_from_kafka(tmp_path: Path) -> None:
    assert KAFKA
    assert REGISTRY
    suffix = uuid.uuid4().hex[:8]
    topics = IncidentTopics(
        detections=TopicSpec(f"processor-test-detections-{suffix}"),
        incidents=TopicSpec(f"processor-test-incidents-{suffix}"),
        dead_letters=TopicSpec(f"processor-test-dlq-{suffix}", partitions=1),
    )
    settings = Settings(
        kafka=KAFKA,
        registry_url=REGISTRY,
        state_dir=str(tmp_path / "state"),
        consumer_group=f"processor-test-{suffix}",
    )
    registry = SchemaRegistry(REGISTRY)
    app = build_incidents_application(settings, registry, topics)

    codec = EventCodec(FirmsDetection, value_subject(topics.detections.name), registry)
    events = [
        detection(CENTER, 0),
        detection(RING[0], 0.2),
        detection(FAR, 0.5),
        detection(FAR, 80),  # moves the clock: the first incident becomes inactive
    ]
    producer = KafkaProducer(bootstrap_servers=KAFKA)
    try:
        for event in events:
            _ = producer.send(
                topics.detections.name, key=event.h3_cell.encode(), value=codec.encode(event)
            )
        _ = producer.send(topics.detections.name, key=events[0].h3_cell.encode(), value=b"junk")
        producer.flush()
    finally:
        producer.close()

    try:
        # A new, A joined, B new; at hour 80 C starts and A and B become inactive.
        outputs = app.run(count=6, timeout=90)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] -- partially typed in Quix
        assert len(outputs) == 6  # pyright: ignore[reportUnknownArgumentType] -- see app.run

        incident_codec = EventCodec(FireIncident, value_subject(topics.incidents.name), registry)
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            consumer_timeout_ms=10000,
            isolation_level="read_committed",
        )
        consumer.subscribe([topics.incidents.name, topics.dead_letters.name])
        incidents: list[tuple[str, FireIncident]] = []
        dead: list[bytes | None] = []
        for record in consumer:
            value = cast(bytes | None, record.value)
            if record.topic == topics.dead_letters.name:
                dead.append(value)
            else:
                key = cast(bytes, record.key).decode()
                incidents.append((key, incident_codec.decode(value)))
            if len(incidents) == 6 and dead:
                break
        consumer.close()

        assert dead == [b"junk"]
        assert all(key == str(incident.incident_id) for key, incident in incidents)
        final = dict(incidents)
        statuses = sorted(incident.status for incident in final.values())
        assert statuses == ["inactive", "inactive", "new"]
        first = next(i for i in final.values() if i.detection_count > 1)
        assert (first.detection_count, first.footprint_cells) == (2, 2)
        assert h3.latlng_to_cell(first.latitude, first.longitude, 2) == first.h3_cell
    finally:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA)
        try:
            created = [name for name in admin.list_topics() if suffix in name]
            admin.delete_topics(created)  # pyright: ignore[reportUnknownMemberType] -- untyped in kafka-python
            _ = admin.delete_consumer_groups([settings.consumer_group])
        finally:
            admin.close()
        for topic in (topics.detections.name, topics.incidents.name):
            delete_subject(REGISTRY, value_subject(topic))


def delete_subject(registry: str, subject: str) -> None:
    for suffix in ("", "?permanent=true"):
        try:
            request = Request(f"{registry}/subjects/{subject}{suffix}", method="DELETE")
            with cast(HTTPResponse, urlopen(request, timeout=10)):
                pass
        except OSError:
            pass


def test_reset_selects_only_the_groups_internal_topics() -> None:
    topics = [
        "firms.detections",
        "fire.incidents",
        "repartition__stream-processor.incidents--firms.detections--source-partition",
        "changelog__stream-processor.incidents--repartition.firms.detections.source-partition--default",
        "changelog__other-group--x--default",
    ]
    assert internal_topics(topics, "stream-processor.incidents") == [topics[3], topics[2]]


@dataclass
class FakeGroup:
    state: str


@dataclass
class FakeAdmin:
    state: str
    deleted: list[str] = field(default_factory=list)

    def describe_consumer_groups(self, groups: list[str]) -> list[FakeGroup]:
        return [FakeGroup(self.state) for _ in groups]

    def delete_consumer_groups(self, groups: list[str]) -> list[tuple[str, object]]:
        self.deleted.extend(groups)
        return [(group, NoError) for group in groups]


def _as_admin(fake: FakeAdmin) -> KafkaAdminClient:
    # FakeAdmin implements only the two calls delete_consumer_group makes.
    return cast(KafkaAdminClient, cast(object, fake))


def test_reset_refuses_while_the_processor_is_running() -> None:
    admin = FakeAdmin("Stable")
    with pytest.raises(ResetRefused, match="Stop the stream processor first"):
        delete_consumer_group(_as_admin(admin), "group")
    assert admin.deleted == []


def test_reset_deletes_a_stopped_group() -> None:
    admin = FakeAdmin("Empty")
    delete_consumer_group(_as_admin(admin), "group")
    assert admin.deleted == ["group"]


def test_job_settings_use_each_jobs_consumer_group(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INCIDENTS_CONSUMER_GROUP", raising=False)
    monkeypatch.delenv("ENRICHMENT_CONSUMER_GROUP", raising=False)
    monkeypatch.setenv("STATISTICS_CONSUMER_GROUP", "custom-statistics")
    assert Settings.from_env().consumer_group == "stream-processor.incidents"
    assert Settings.from_env(Job.ENRICHMENT).consumer_group == "stream-processor.enrichment"
    assert Settings.from_env(Job.STATISTICS).consumer_group == "custom-statistics"
