"""Republish Phase 2-5 events from ``firms.detections.v1`` into ``firms.detections``.

The legacy topic holds plain-JSON VIIRS NOAA-20 events. Each is converted to the
registered FirmsDetection schema with the same deterministic event ID that live
ingestion would assign, so running this more than once, or after fresh ingestion
of the same days, publishes nothing twice.
"""

import argparse
import json
import logging
import sys
import time
from datetime import UTC, datetime, timedelta

from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from pydantic import ValidationError

from wildfire_events.cells import partition_cell
from wildfire_events.models import FIRMS_PRODUCTS, FirmsDetection, detection_event_id
from wildfire_events.topics import FIRMS_DETECTIONS
from wildfire_ingestor.firms import _frp
from wildfire_ingestor.log import PublishedEvents
from wildfire_ingestor.service import Settings, detection_publisher

LOGGER = logging.getLogger(__name__)
LEGACY_TOPIC = "firms.detections.v1"


def convert(value: bytes, ingested_at: datetime) -> FirmsDetection:
    legacy = json.loads(value)
    if legacy.get("schema_version") != 1 or legacy.get("source") not in FIRMS_PRODUCTS:
        raise ValueError("not a version 1 FIRMS event")
    attributes = {str(key): str(item) for key, item in legacy["attributes"].items()}
    observed_at = datetime.fromisoformat(legacy["observed_at"])
    latitude_text = attributes.get("latitude", str(legacy["latitude"]))
    longitude_text = attributes.get("longitude", str(legacy["longitude"]))
    latitude, longitude = float(legacy["latitude"]), float(legacy["longitude"])
    return FirmsDetection(
        event_id=detection_event_id(
            legacy["source"], legacy["satellite"], observed_at, latitude_text, longitude_text
        ),
        product=legacy["source"],
        instrument=FIRMS_PRODUCTS[legacy["source"]],
        satellite=legacy["satellite"],
        observed_at=observed_at,
        latitude=latitude,
        longitude=longitude,
        h3_cell=partition_cell(latitude, longitude),
        frp_mw=_frp(attributes.get("frp")),
        attributes=attributes,
        ingested_at=ingested_at,
    )


def read_legacy(bootstrap_servers: str) -> list[tuple[bytes, datetime]]:
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=None)
    try:
        partitions = consumer.partitions_for_topic(LEGACY_TOPIC) or set()
        assignment = [TopicPartition(LEGACY_TOPIC, partition) for partition in partitions]
        if not assignment:
            return []
        consumer.assign(assignment)
        consumer.seek_to_beginning()
        end = consumer.end_offsets(assignment)
        values = []
        deadline = time.monotonic() + 600
        pending = {tp for tp in assignment if end[tp] > 0}
        while pending and time.monotonic() < deadline:
            for records in consumer.poll(timeout_ms=1000, max_records=5000).values():
                values.extend(
                    (record.value, datetime.fromtimestamp(record.timestamp / 1000, UTC))
                    for record in records
                )
            pending = {tp for tp in pending if consumer.position(tp) < end[tp]}
        if pending:
            raise TimeoutError("timed out reading the legacy topic")
        return values
    finally:
        consumer.close()


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    records = read_legacy(settings.kafka)
    detections, invalid = [], 0
    for value, published_at in records:
        try:
            detections.append(convert(value, published_at))
        except (KeyError, TypeError, ValueError, ValidationError):
            invalid += 1
    oldest = min((item.ingested_at for item in detections), default=datetime.now(UTC))
    published = PublishedEvents(datetime.now(UTC) - oldest + timedelta(days=1))
    published.load(settings.kafka, FIRMS_DETECTIONS.name)
    publisher, producer = detection_publisher(settings, published)
    try:
        count, skipped = publisher.publish(detections)
    finally:
        producer.close()
    LOGGER.info(
        "Read %d legacy events: %d invalid, %d published, %d already present",
        len(records),
        invalid,
        count,
        skipped,
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
