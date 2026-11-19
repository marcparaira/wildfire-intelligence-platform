"""A small thread-per-producer scheduler."""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from threading import Event, Thread

LOGGER = logging.getLogger(__name__)
FAILURE_RETRY_SECONDS = 300.0


class RetryLater(RuntimeError):
    def __init__(self, message: str, seconds: float) -> None:
        super().__init__(message)
        self.seconds: float = seconds


@dataclass
class ScheduledJob:
    name: str
    interval_seconds: float
    run: Callable[[], str]
    initial_delay: Callable[[], float] = lambda: 0.0


def run_job(job: ScheduledJob, stop: Event) -> None:
    try:
        delay = job.initial_delay()
    except Exception:
        LOGGER.exception("%s: could not determine initial delay; running now", job.name)
        delay = 0.0
    if delay > 0:
        LOGGER.info("%s: next run in %.0f s", job.name, delay)
    while not stop.wait(delay):
        try:
            LOGGER.info("%s: %s", job.name, job.run())
            delay = job.interval_seconds
        except RetryLater as exc:
            delay = max(1.0, min(exc.seconds, job.interval_seconds))
            LOGGER.warning("%s: %s; retrying in %.0f s", job.name, exc, delay)
        except Exception:
            delay = min(FAILURE_RETRY_SECONDS, job.interval_seconds)
            LOGGER.exception("%s: run failed; retrying in %.0f s", job.name, delay)


def run_forever(jobs: list[ScheduledJob], stop: Event) -> None:
    threads = [Thread(target=run_job, args=(job, stop), name=job.name, daemon=True) for job in jobs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
