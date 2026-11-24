from datetime import timedelta
from threading import Event

import pytest

from wildfire_producers.http import excerpt, retry_after_seconds
from wildfire_producers.runtime import env_bounded_int, env_minutes, run_each_once
from wildfire_producers.scheduler import RetryLater, ScheduledJob, run_job


@pytest.mark.parametrize(
    ("header", "seconds"),
    [(None, 60), ("0", 1), ("not a date", 60), ("Wed, 21 Oct 2015 07:28:00 GMT", 1)],
)
def test_retry_after_header_parsing(header: str | None, seconds: int) -> None:
    assert retry_after_seconds(header) == seconds


def test_scheduler_waits_the_requested_retry_delay() -> None:
    stop = Event()
    delays: list[float | None] = []
    calls = iter([RetryLater("busy", 17), "ok"])

    def run() -> str:
        outcome = next(calls)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    original_wait = stop.wait

    def recording_wait(timeout: float | None = None) -> bool:
        delays.append(timeout)
        if len(delays) == 3:
            stop.set()
        return original_wait(0)

    stop.wait = recording_wait  # type: ignore[method-assign]
    run_job(ScheduledJob("test", 600, run, lambda: 5), stop)
    assert delays == [5, 17, 600]


def test_run_each_once_reports_whether_every_job_succeeded() -> None:
    def fails() -> str:
        raise RuntimeError("provider down")

    ok = ScheduledJob("ok", 60, lambda: "done")
    assert run_each_once([ok])
    assert not run_each_once([ScheduledJob("broken", 60, fails), ok])


def test_settings_helpers_read_and_bound_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOME_MINUTES", "1.5")
    assert env_minutes("SOME_MINUTES", 60) == timedelta(seconds=90)
    assert env_minutes("UNSET_MINUTES", 60) == timedelta(hours=1)
    monkeypatch.setenv("SOME_COUNT", "11")
    with pytest.raises(ValueError, match="SOME_COUNT must be between 1 and 10"):
        _ = env_bounded_int("SOME_COUNT", 5, 1, 10)
    assert env_bounded_int("UNSET_COUNT", 5, 1, 10) == 5


def test_excerpts_are_short_and_never_fail_to_decode() -> None:
    assert excerpt(b"\xff" + b"x" * 1000) == "\ufffd" + "x" * 299
