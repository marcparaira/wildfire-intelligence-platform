"""Incidents job: clusters ``firms.detections`` into ``fire.incidents``.

Pipeline::

    firms.detections ─ decode (invalid → stream-processor.dlq)
        ─ group by source partition (repartition topic)
        ─ IncidentTracker (stateful) ─ fire.incidents

Quix Streams keeps state per message key. Detections are keyed by H3 cell, so a
cell that stops reporting would never see its incidents age. Re-keying each
detection by its source partition gives every partition one state whose clock
advances with any detection in that partition, and keeps each partition's
original order, so a replay produces the same incidents.

Processing is exactly-once: state changes, the repartition topic, outputs, and
consumer offsets commit together in Kafka transactions.
"""

from dataclasses import dataclass
from typing import cast

from quixstreams import Application, State, message_context

from wildfire_events.codec import EventCodec
from wildfire_events.kafka import ensure_topics
from wildfire_events.models import FireIncident, FirmsDetection
from wildfire_events.registry import SubjectRegistry
from wildfire_events.topics import (
    FIRE_INCIDENTS,
    FIRMS_DETECTIONS,
    STREAM_PROCESSOR_DLQ,
    TopicSpec,
    value_subject,
)
from wildfire_processor.adapters import (
    DeadLetters,
    QuixState,
    RecordDecoder,
    RecordProducer,
    is_present,
    repartition_topic,
)
from wildfire_processor.incidents import IncidentTracker
from wildfire_processor.quix import application, output_topic
from wildfire_processor.settings import Settings

REPARTITION_NAME = "source-partition"


@dataclass(frozen=True, slots=True)
class IncidentTopics:
    """Topic definitions; tests substitute their own."""

    detections: TopicSpec = FIRMS_DETECTIONS
    incidents: TopicSpec = FIRE_INCIDENTS
    dead_letters: TopicSpec = STREAM_PROCESSOR_DLQ


@dataclass(frozen=True, slots=True)
class DetectionDecoder:
    """Decodes detections to JSON-ready dicts, which the repartition topic carries."""

    decoder: RecordDecoder[FirmsDetection]

    def __call__(
        self,
        value: bytes | None,
        key: bytes | None,
        timestamp: int,
        headers: list[tuple[str, bytes]] | None,
    ) -> dict[str, object] | None:
        detection = self.decoder(value, key, timestamp, headers)
        return None if detection is None else detection.model_dump(mode="json")


def track(value: dict[str, object], state: State[str, object]) -> list[FireIncident]:
    """Quix stateful step: apply one decoded detection to its partition's state."""
    tracker = IncidentTracker(QuixState(state))
    return tracker.process(FirmsDetection.model_validate(value))


def source_partition(_value: object) -> str:
    return str(message_context().partition)


def incident_key(incident: FireIncident) -> str:
    return str(incident.incident_id)


def build_incidents_application(
    settings: Settings, registry: SubjectRegistry, topics: IncidentTopics | None = None
) -> Application:
    """Create the topics and the Quix application with its pipeline.

    Side effects: creates missing topics (including the repartition topic) and
    registers the ``FireIncident`` schema, so an incompatible schema fails here.
    """
    topics = topics or IncidentTopics()
    ensure_topics(
        settings.kafka,
        [
            topics.detections,
            topics.incidents,
            topics.dead_letters,
            repartition_topic(settings.consumer_group, topics.detections.name, REPARTITION_NAME),
        ],
    )
    app = application(settings)
    detections = app.topic(topics.detections.name, value_deserializer="bytes")
    incidents = output_topic(
        app,
        topics.incidents,
        EventCodec(FireIncident, value_subject(topics.incidents.name), registry),
    )
    decoder = DetectionDecoder(
        RecordDecoder(
            EventCodec(FirmsDetection, value_subject(topics.detections.name), registry),
            DeadLetters(
                cast(RecordProducer, app.get_producer()),
                topic=topics.dead_letters.name,
                group_id=settings.consumer_group,
            ),
        )
    )
    sdf = app.dataframe(detections)
    sdf = sdf.apply(decoder, metadata=True).filter(is_present)  # pyright: ignore[reportUnknownMemberType] -- partially typed in Quix
    sdf = sdf.group_by(source_partition, name=REPARTITION_NAME)
    sdf = sdf.apply(track, stateful=True, expand=True)  # pyright: ignore[reportUnknownMemberType] -- partially typed in Quix
    _ = sdf.to_topic(incidents, key=incident_key)
    return app
