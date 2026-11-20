from dataclasses import dataclass
from threading import Event

from wildfire_processor.service import Supervisor


@dataclass
class FakeProcess:
    name: str
    exit_after_polls: int | None = None
    exit_code: int = 0
    ignores_terminate: bool = False
    polls: int = 0
    started: bool = False
    terminated: bool = False
    killed: bool = False
    _exitcode: int | None = None

    @property
    def exitcode(self) -> int | None:
        return self._exitcode

    def start(self) -> None:
        self.started = True

    def is_alive(self) -> bool:
        if self._exitcode is not None:
            return False
        self.polls += 1
        if self.exit_after_polls is not None and self.polls > self.exit_after_polls:
            self._exitcode = self.exit_code
            return False
        return True

    def terminate(self) -> None:
        self.terminated = True
        if not self.ignores_terminate:
            self._exitcode = 0

    def kill(self) -> None:
        self.killed = True
        self._exitcode = -9

    def join(self, timeout: float | None = None) -> None:
        del timeout


def supervise(*processes: FakeProcess, stop: Event | None = None) -> int:
    return Supervisor(list(processes), stop or Event(), poll_seconds=0, stop_timeout=0).run()


def test_a_failed_job_stops_the_others_and_fails_the_service() -> None:
    failing = FakeProcess("job:enrichment", exit_after_polls=2, exit_code=1)
    other = FakeProcess("job:incidents")
    assert supervise(failing, other) == 1
    assert failing.started
    assert other.started
    assert other.terminated
    assert not failing.terminated


def test_a_job_that_exits_cleanly_on_its_own_still_fails_the_service() -> None:
    assert supervise(FakeProcess("a", exit_after_polls=1), FakeProcess("b")) == 1


def test_stop_request_terminates_all_jobs_and_succeeds() -> None:
    stop = Event()
    stop.set()
    jobs = [FakeProcess("a"), FakeProcess("b")]
    assert supervise(*jobs, stop=stop) == 0
    assert all(job.terminated and not job.killed for job in jobs)


def test_a_job_that_ignores_terminate_is_killed() -> None:
    stop = Event()
    stop.set()
    stubborn = FakeProcess("a", ignores_terminate=True)
    assert supervise(stubborn, stop=stop) == 0
    assert stubborn.killed
