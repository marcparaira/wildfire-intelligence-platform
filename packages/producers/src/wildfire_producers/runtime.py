"""Settings, logging, and the process loop shared by the producer services."""

import logging
import os
import signal
from collections.abc import Sequence
from datetime import timedelta
from threading import Event
from types import FrameType

from wildfire_events.registry import SchemaRegistry
from wildfire_producers.scheduler import ScheduledJob, run_forever

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = "%(levelname)s %(name)s %(message)s"
DEFAULT_KAFKA = "localhost:19092"
DEFAULT_REGISTRY_URL = "http://localhost:18081"
PRODUCER_CLOSE_TIMEOUT_SECONDS = 10


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    logging.getLogger("kafka").setLevel(logging.WARNING)


def kafka_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA)


def registry_url() -> str:
    return os.environ.get("SCHEMA_REGISTRY_URL", DEFAULT_REGISTRY_URL)


def env_minutes(name: str, default: float) -> timedelta:
    return timedelta(minutes=float(os.environ.get(name, str(default))))


def env_bounded_int(name: str, default: int, lowest: int, highest: int) -> int:
    """An integer setting; raises ``ValueError`` outside ``lowest``-``highest``."""
    value = int(os.environ.get(name, str(default)))
    if not lowest <= value <= highest:
        raise ValueError(f"{name} must be between {lowest} and {highest}")
    return value


def connect_registry(url: str) -> SchemaRegistry:
    """The schema registry, once it accepts requests."""
    registry = SchemaRegistry(url)
    registry.wait_until_ready()
    return registry


def run_each_once(jobs: Sequence[ScheduledJob]) -> bool:
    """Run every job once, continuing past failures; return whether all succeeded."""
    succeeded = True
    for job in jobs:
        try:
            LOGGER.info("%s: %s", job.name, job.run())
        # Supervisor boundary: report every job's outcome; the exit code reflects failures.
        except Exception:
            LOGGER.exception("%s: run failed", job.name)
            succeeded = False
    return succeeded


def run_until_stopped(jobs: list[ScheduledJob], service: str) -> None:
    """Run the jobs on their schedules until SIGTERM or SIGINT."""
    stop = Event()

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        stop.set()

    _ = signal.signal(signal.SIGTERM, request_stop)
    _ = signal.signal(signal.SIGINT, request_stop)
    LOGGER.info("%s: running %d scheduled producers", service, len(jobs))
    run_forever(jobs, stop)
    LOGGER.info("%s stopped", service)
