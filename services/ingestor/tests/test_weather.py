import io
import json
from datetime import UTC, datetime
from email.message import Message
from threading import Event
from typing import Self
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

import pytest

from wildfire_ingestor import weather
from wildfire_ingestor.scheduler import RetryLater, ScheduledJob, run_job

GENERATED = datetime(2026, 3, 4, 20, tzinfo=UTC)
CELL = weather.forecast_cells()[0]


class Response:
    def __init__(self, payload: object) -> None:
        self.payload: object = payload

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode()


def hourly(**overrides: list[object]) -> dict[str, dict[str, list[object]]]:
    values: dict[str, list[object]] = {
        "time": ["2026-03-05T00:00", "2026-03-05T01:00"],
        "temperature_2m": [28.5, 28.0],
        "precipitation": [0.2, 0],
        "wind_speed_10m": [12, 14],
        "wind_direction_10m": [90, 110],
    }
    values.update(overrides)
    return {"hourly": values}


def test_forecast_grid_is_the_h3_partition_grid_over_the_region() -> None:
    cells = weather.forecast_cells()
    assert len(cells) == 385
    assert len({cell.cell for cell in cells}) == 385


def test_fetch_chunk_builds_one_event_per_cell_with_explicit_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    urls: list[str] = []

    def fake_urlopen(url: str, timeout: int) -> Response:
        del timeout
        urls.append(url)
        return Response([hourly(), hourly()])

    monkeypatch.setattr(weather, "urlopen", fake_urlopen)
    second = weather.forecast_cells()[1]
    forecasts = weather.fetch_chunk([CELL, second], GENERATED)

    assert [forecast.cell for forecast in forecasts] == [CELL.cell, second.cell]
    first = forecasts[0]
    assert first.generated_at == GENERATED
    assert first.hourly[0].valid_at == datetime(2026, 3, 5, tzinfo=UTC)
    assert first.hourly[0].temperature_c == 28.5
    assert first.hourly[1].wind_direction_degrees == 110
    query = parse_qs(urlparse(urls[0]).query)
    assert query["timezone"] == ["GMT"]
    assert query["wind_speed_unit"] == ["kmh"]
    assert query["precipitation_unit"] == ["mm"]
    assert query["forecast_days"] == ["7"]
    assert query["latitude"] == [f"{CELL.latitude},{second.latitude}"]


def test_fetch_chunk_skips_invalid_cells(monkeypatch: pytest.MonkeyPatch) -> None:
    second = weather.forecast_cells()[1]
    payload = [hourly(precipitation=[0.2]), hourly()]

    def respond(*_args: object, **_kwargs: object) -> Response:
        return Response(payload)

    monkeypatch.setattr(weather, "urlopen", respond)
    assert [item.cell for item in weather.fetch_chunk([CELL, second], GENERATED)] == [second.cell]


def test_rate_limit_asks_the_scheduler_to_retry_later(monkeypatch: pytest.MonkeyPatch) -> None:
    def limited(url: str, **_kwargs: object) -> None:
        headers = Message()
        headers["Retry-After"] = "17"
        raise HTTPError(url, 429, "Too Many Requests", headers, io.BytesIO())

    monkeypatch.setattr(weather, "urlopen", limited)
    with pytest.raises(RetryLater) as raised:
        _ = weather.fetch_chunk([CELL], GENERATED)
    assert raised.value.seconds == 17


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
