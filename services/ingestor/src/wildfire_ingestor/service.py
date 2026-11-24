"""Ingestor service: scheduled FIRMS and Open-Meteo producers.

Satellite imagery of incidents is acquired by the separate imagery worker.
"""

import argparse
import logging
import sys
from datetime import UTC, datetime

from kafka import KafkaProducer

from wildfire_events.countries import selected_countries
from wildfire_events.kafka import idempotent_producer
from wildfire_events.models import WeatherProvider
from wildfire_events.topics import WEATHER_FORECASTS
from wildfire_ingestor.config import (
    PRODUCER_CLIENT_ID,
    Settings,
    detection_publisher,
    load_published_detections,
    weather_publisher,
)
from wildfire_ingestor.detections import FirmsProducer
from wildfire_ingestor.sources import FirmsClient, OpenMeteoClient
from wildfire_ingestor.weather import WeatherProducer
from wildfire_producers.log import latest_publish_time
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


def build_jobs(settings: Settings, producer: KafkaProducer) -> list[ScheduledJob]:
    """One job per configured FIRMS product, and the weather job."""
    published = load_published_detections(settings)
    registry = connect_registry(settings.registry_url)
    detections = detection_publisher(producer, registry, published)
    jobs: list[ScheduledJob] = []
    if settings.firms_map_key:
        firms = FirmsClient(settings.firms_map_key)
        countries = selected_countries()
        for product in settings.firms_products:
            producer_job = FirmsProducer(product, settings.firms_days, firms, detections, countries)
            jobs.append(
                ScheduledJob(
                    f"firms:{product}",
                    settings.firms_interval.total_seconds(),
                    producer_job.run_once,
                )
            )
    else:
        LOGGER.warning("FIRMS_MAP_KEY is not set; FIRMS producers are disabled")

    weather = WeatherProducer(OpenMeteoClient(), weather_publisher(producer, registry))
    jobs.append(
        ScheduledJob(
            f"weather:{WeatherProvider.OPEN_METEO}",
            settings.weather_interval.total_seconds(),
            weather.run_once,
            lambda: _seconds_until_weather_due(settings),
        )
    )
    return jobs


def _seconds_until_weather_due(settings: Settings) -> float:
    """Resume the weather schedule across restarts instead of re-fetching at once."""
    latest = latest_publish_time(settings.kafka, WEATHER_FORECASTS.name)
    if latest is None:
        return 0.0
    due = latest + settings.weather_interval
    return max(0.0, (due - datetime.now(UTC)).total_seconds())


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the scheduled FIRMS and weather producers")
    _ = parser.add_argument("--once", action="store_true", help="Run every producer once and exit")
    args = parser.parse_args(namespace=Arguments())
    configure_logging()
    settings = Settings.from_env()
    producer = idempotent_producer(settings.kafka, PRODUCER_CLIENT_ID)
    try:
        jobs = build_jobs(settings, producer)
        if args.once:
            sys.exit(0 if run_each_once(jobs) else 1)
        run_until_stopped(jobs, "Ingestor")
    finally:
        producer.close(timeout=PRODUCER_CLOSE_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
