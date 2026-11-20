"""A small thread-per-producer scheduler."""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from threading import Event, Thread

LOGGER = logging.getLogger(__name__)
FAILURE_RETRY_SECONDS = 300.0
MIN_RETRY_SECONDS = 1.0


class RetryLater(RuntimeError):
    """A transient failure whose source said when to try again (e.g. HTTP 429)."""

    def __init__(self, message: str, seconds: float) -> None:
        super().__init__(message)
        self.seconds: float = seconds


def _run_now() -> float:
    return 0.0


@dataclass(frozen=True, slots=True)
class ScheduledJob:
    """``run`` returns a summary to log; ``initial_delay`` is seconds until the first run."""

    name: str
    interval_seconds: float
    run: Callable[[], str]
    initial_delay: Callable[[], float] = _run_now


def run_job(job: ScheduledJob, stop: Event) -> None:
    """Run ``job`` every interval until ``stop`` is set; failures delay the next run."""
    delay = _initial_delay(job)
    if delay > 0:
        LOGGER.info("%s: next run in %.0f s", job.name, delay)
    while not stop.wait(delay):
        delay = _run_and_schedule(job)


def _initial_delay(job: ScheduledJob) -> float:
    try:
        return job.initial_delay()
    # Supervisor boundary: an unknown delay must not stop the job; it runs now instead.
    except Exception:
        LOGGER.exception("%s: could not determine initial delay; running now", job.name)
        return 0.0


def _run_and_schedule(job: ScheduledJob) -> float:
    """Run once and return the delay until the next run."""
    try:
        LOGGER.info("%s: %s", job.name, job.run())
    except RetryLater as exc:
        delay = max(MIN_RETRY_SECONDS, min(exc.seconds, job.interval_seconds))
        LOGGER.warning("%s: %s; retrying in %.0f s", job.name, exc, delay)
        return delay
    # Supervisor boundary: one failed run (provider outage, Kafka error, ...) must not
    # kill the producer thread. The traceback is logged and the run is retried.
    except Exception:
        delay = min(FAILURE_RETRY_SECONDS, job.interval_seconds)
        LOGGER.exception("%s: run failed; retrying in %.0f s", job.name, delay)
        return delay
    return job.interval_seconds


def run_forever(jobs: list[ScheduledJob], stop: Event) -> None:
    """Run every job on its own thread until ``stop`` is set."""
    threads = [Thread(target=run_job, args=(job, stop), name=job.name, daemon=True) for job in jobs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
