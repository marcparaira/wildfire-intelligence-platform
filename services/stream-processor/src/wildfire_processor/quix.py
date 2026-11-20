"""Quix application and topic construction shared by the jobs."""

from pydantic import BaseModel
from quixstreams import Application
from quixstreams.models import Topic, TopicConfig

from wildfire_events.codec import EventCodec
from wildfire_events.kafka import REPLICATION_FACTOR
from wildfire_events.topics import TopicSpec
from wildfire_processor.adapters import CodecSerializer
from wildfire_processor.settings import Settings


def application(settings: Settings) -> Application:
    """An exactly-once application that starts new consumer groups at the earliest offset."""
    return Application(
        broker_address=settings.kafka,
        consumer_group=settings.consumer_group,
        auto_offset_reset="earliest",
        processing_guarantee="exactly-once",
        state_dir=settings.state_dir,
        loglevel=None,
    )


def output_topic[T: BaseModel](app: Application, spec: TopicSpec, codec: EventCodec[T]) -> Topic:
    """A wire-format output topic with string keys; registers the schema now.

    Registering at build time makes an incompatible schema fail at startup.
    """
    _ = codec.schema_id
    return app.topic(
        spec.name,
        key_serializer="str",
        value_serializer=CodecSerializer(codec),
        config=TopicConfig(
            num_partitions=spec.partitions,
            replication_factor=REPLICATION_FACTOR,
            extra_config=spec.config,
        ),
    )
