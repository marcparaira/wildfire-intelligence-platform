"""Ingestor service: scheduled FIRMS and Open-Meteo producers."""

import argparse
import logging
import signal
import sys
from datetime import UTC, datetime
from threading import Event
from types import FrameType

from kafka import KafkaProducer

from wildfire_events.kafka import ensure_topics
from wildfire_events.topics import FIRMS_DETECTIONS, SOURCE_TOPICS, WEATHER_FORECASTS
from wildfire_ingestor.config import Settings, detection_publisher, weather_publisher
from wildfire_ingestor.firms import FirmsProducer
from wildfire_ingestor.log import PublishedEvents, latest_publish_time
from wildfire_ingestor.scheduler import ScheduledJob, run_forever
from wildfire_ingestor.weather import WeatherProducer

LOGGER = logging.getLogger(__name__)


class Arguments(argparse.Namespace):
    once: bool = False


def build_jobs(settings: Settings) -> tuple[list[ScheduledJob], KafkaProducer]:
    published = PublishedEvents(settings.dedup_window)
    ensure_topics(settings.kafka, SOURCE_TOPICS)
    loaded = published.load(settings.kafka, FIRMS_DETECTIONS.name)
    LOGGER.info("Loaded %d recently published detection IDs from %s", loaded, FIRMS_DETECTIONS.name)
    detections, producer = detection_publisher(settings, published)
    jobs: list[ScheduledJob] = []
    if settings.firms_map_key:
        for product in settings.firms_products:
            firms = FirmsProducer(product, settings.firms_map_key, settings.firms_days, detections)

            def run_firms(firms: FirmsProducer = firms) -> str:
                published.prune()
                return firms.run_once()

            jobs.append(
                ScheduledJob(f"firms:{product}", settings.firms_interval.total_seconds(), run_firms)
            )
    else:
        LOGGER.warning("FIRMS_MAP_KEY is not set; FIRMS producers are disabled")

    weather = WeatherProducer(weather_publisher(settings, producer))

    def weather_delay() -> float:
        latest = latest_publish_time(settings.kafka, WEATHER_FORECASTS.name)
        if latest is None:
            return 0.0
        due = latest + settings.weather_interval
        return max(0.0, (due - datetime.now(UTC)).total_seconds())

    jobs.append(
        ScheduledJob(
            "weather:open-meteo",
            settings.weather_interval.total_seconds(),
            weather.run_once,
            weather_delay,
        )
    )
    return jobs, producer


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the scheduled FIRMS and weather producers")
    _ = parser.add_argument(
        "--once", action="store_true", help="Run every producer once and exit"
    )
    args = parser.parse_args(namespace=Arguments())
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    settings = Settings.from_env()
    jobs, producer = build_jobs(settings)
    try:
        if args.once:
            failed = False
            for job in jobs:
                try:
                    LOGGER.info("%s: %s", job.name, job.run())
                except Exception:
                    LOGGER.exception("%s: run failed", job.name)
                    failed = True
            sys.exit(1 if failed else 0)
        stop = Event()

        def request_stop(_signum: int, _frame: FrameType | None) -> None:
            stop.set()

        _ = signal.signal(signal.SIGTERM, request_stop)
        _ = signal.signal(signal.SIGINT, request_stop)
        LOGGER.info("Running %d scheduled producers", len(jobs))
        run_forever(jobs, stop)
        LOGGER.info("Ingestor stopped")
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
