"""Stream processor entry point: runs the incidents, enrichment, and statistics jobs.

Each job is its own Quix Streams application (and consumer group). ``main`` runs
every job in a child process and stops them all when one exits or on SIGTERM or
SIGINT; it exits non-zero when a job failed. ``--job NAME`` runs one job in this
process instead.

| Job | Reads | Writes |
|---|---|---|
| incidents | firms.detections | fire.incidents |
| enrichment | firms.detections, weather.forecasts | detections.enriched |
| statistics | detections.enriched | stats.hourly |

Invalid input records go to ``stream-processor.dlq``.
"""

import argparse
import logging
import multiprocessing
import signal
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import Event
from types import FrameType
from typing import Protocol

from quixstreams import Application

from wildfire_events.registry import SchemaRegistry, SubjectRegistry
from wildfire_processor.enrichment_job import build_enrichment_application
from wildfire_processor.incidents_job import build_incidents_application
from wildfire_processor.settings import Job, Settings
from wildfire_processor.statistics_job import build_statistics_application

LOGGER = logging.getLogger(__name__)
POLL_SECONDS = 1.0
# How long a job gets to commit and close after SIGTERM before it is killed.
STOP_TIMEOUT_SECONDS = 30.0
FAILED_EXIT_CODE = 1

BUILDERS: Mapping[Job, Callable[[Settings, SubjectRegistry], Application]] = {
    Job.INCIDENTS: build_incidents_application,
    Job.ENRICHMENT: build_enrichment_application,
    Job.STATISTICS: build_statistics_application,
}


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)


def run_job(job: Job) -> None:
    """Run one job until Quix stops it (on SIGTERM or SIGINT)."""
    configure_logging()
    settings = Settings.from_env(job)
    registry = SchemaRegistry(settings.registry_url)
    registry.wait_until_ready()
    app = BUILDERS[job](settings, registry)
    LOGGER.info("Running the %s job as %s", job, settings.consumer_group)
    _ = app.run()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType] -- partially typed in Quix


class ChildProcess(Protocol):
    """The part of ``multiprocessing.Process`` the supervisor uses."""

    @property
    def name(self) -> str: ...

    @property
    def exitcode(self) -> int | None: ...

    def start(self) -> None: ...

    def is_alive(self) -> bool: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    def join(self, timeout: float | None = None) -> None: ...


def job_process(job: Job) -> ChildProcess:
    """A fresh (spawned) interpreter per job, so no Kafka client state is inherited."""
    context = multiprocessing.get_context("spawn")
    return context.Process(target=run_job, args=(job,), name=f"job:{job}")


@dataclass(frozen=True, slots=True)
class Supervisor:
    """Keeps all jobs running together: when one exits, the others are stopped."""

    processes: list[ChildProcess]
    stop: Event
    poll_seconds: float = POLL_SECONDS
    stop_timeout: float = STOP_TIMEOUT_SECONDS

    def run(self) -> int:
        """Run until a job exits or ``stop`` is set; return the exit code."""
        for process in self.processes:
            process.start()
        while not self.stop.is_set() and all(process.is_alive() for process in self.processes):
            _ = self.stop.wait(self.poll_seconds)
        failed = [
            process.name
            for process in self.processes
            if not process.is_alive() and process.exitcode != 0
        ]
        exited = [process.name for process in self.processes if not process.is_alive()]
        if exited and not self.stop.is_set():
            LOGGER.error("Job stopped unexpectedly: %s; stopping the others", ", ".join(exited))
        self._stop_all()
        return FAILED_EXIT_CODE if failed or (exited and not self.stop.is_set()) else 0

    def _stop_all(self) -> None:
        for process in self.processes:
            if process.is_alive():
                process.terminate()
        deadline = time.monotonic() + self.stop_timeout
        for process in self.processes:
            process.join(max(0.0, deadline - time.monotonic()))
            if process.is_alive():
                LOGGER.warning("%s did not stop in time; killing it", process.name)
                process.kill()
                process.join()


class Arguments(argparse.Namespace):
    job: Job | None = None


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the stream-processing jobs")
    _ = parser.add_argument(
        "--job", type=Job, choices=list(Job), help="Run only this job, in this process"
    )
    args = parser.parse_args(namespace=Arguments())
    if args.job is not None:
        run_job(args.job)
        return
    configure_logging()
    stop = Event()

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        stop.set()

    _ = signal.signal(signal.SIGTERM, request_stop)
    _ = signal.signal(signal.SIGINT, request_stop)
    LOGGER.info("Starting jobs: %s", ", ".join(Job))
    code = Supervisor([job_process(job) for job in Job], stop).run()
    LOGGER.info("Stream processor stopped")
    sys.exit(code)


if __name__ == "__main__":
    main()
