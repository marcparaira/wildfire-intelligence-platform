"""Imagery worker: scheduled Sentinel-2 imagery of current fire incidents.

It runs apart from the ingestor because it is downstream of the stream processor
(it reads ``fire.incidents``), slow (a Sentinel-2 run takes minutes), and needs the
imagery store and image processing; restarting it never interrupts live ingestion.
"""

import argparse
import logging
import sys

from kafka import KafkaProducer

from wildfire_events.kafka import ensure_topics, idempotent_producer
from wildfire_events.models import ImageryLayer, ImageryProvider
from wildfire_events.registry import SchemaRegistry
from wildfire_events.topics import SATELLITE_SCENES
from wildfire_imagery.config import (
    PRODUCER_CLIENT_ID,
    IncidentReader,
    Settings,
    load_acquired_scenes,
    scene_publisher,
)
from wildfire_imagery.planetary_computer import PlanetaryComputerClient
from wildfire_imagery.scenes import FileSceneStore
from wildfire_imagery.sentinel import SentinelProducer
from wildfire_producers.runtime import (
    PRODUCER_CLOSE_TIMEOUT_SECONDS,
    configure_logging,
    connect_registry,
    run_each_once,
    run_until_stopped,
)
from wildfire_producers.scheduler import ScheduledJob

LOGGER = logging.getLogger(__name__)


class Arguments(argparse.Namespace):
    once: bool = False


def build_jobs(
    settings: Settings, producer: KafkaProducer, registry: SchemaRegistry
) -> list[ScheduledJob]:
    """The Sentinel-2 job, if its layer is configured."""
    ensure_topics(settings.kafka, (SATELLITE_SCENES,))
    incidents = IncidentReader(settings, registry)
    store = FileSceneStore(settings.imagery_dir)
    publisher = scene_publisher(producer, registry)
    acquired = load_acquired_scenes(settings)
    jobs: list[ScheduledJob] = []
    if ImageryLayer.SENTINEL2_SWIR in settings.imagery_layers:
        sentinel = SentinelProducer(
            incidents=incidents,
            source=PlanetaryComputerClient(),
            store=store,
            publisher=publisher,
            acquired=acquired,
            days=settings.imagery_days,
            max_scenes_per_run=settings.sentinel_max_scenes_per_run,
        )
        jobs.append(
            ScheduledJob(
                f"imagery:{ImageryProvider.PLANETARY_COMPUTER}",
                settings.imagery_interval.total_seconds(),
                sentinel.run_once,
            )
        )
    return jobs


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the scheduled imagery producers")
    _ = parser.add_argument("--once", action="store_true", help="Run every producer once and exit")
    args = parser.parse_args(namespace=Arguments())
    configure_logging()
    settings = Settings.from_env()
    if not settings.imagery_layers:
        LOGGER.warning("IMAGERY_LAYERS is empty; there is nothing to do")
        return
    producer = idempotent_producer(settings.kafka, PRODUCER_CLIENT_ID)
    try:
        jobs = build_jobs(settings, producer, connect_registry(settings.registry_url))
        if args.once:
            sys.exit(0 if run_each_once(jobs) else 1)
        run_until_stopped(jobs, "Imagery worker")
    finally:
        producer.close(timeout=PRODUCER_CLOSE_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
