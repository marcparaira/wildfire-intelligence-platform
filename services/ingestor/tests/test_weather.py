import io
import json
from datetime import UTC, datetime
from email.message import Message
from threading import Event
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

import pytest

from wildfire_ingestor import weather
from wildfire_ingestor.scheduler import RetryLater, ScheduledJob, run_job
from wildfire_ingestor.sources import OpenMeteoClient, retry_after_seconds

GENERATED = datetime(2026, 3, 4, 20, tzinfo=UTC)
CELL = weather.forecast_cells()[0]


class FakeGet:
    """Serves one canned JSON payload and records the requested URLs."""

    def __init__(self, payload: object) -> None:
        self.payload: object = payload
        self.urls: list[str] = []

    def __call__(self, url: str, *, timeout: float) -> bytes:
        del timeout
        self.urls.append(url)
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


def test_fetch_builds_one_event_per_cell_with_explicit_units() -> None:
    get = FakeGet([hourly(), hourly()])
    second = weather.forecast_cells()[1]
    forecasts = weather.fetch_forecasts(OpenMeteoClient(get), [CELL, second], GENERATED)

    assert [forecast.cell for forecast in forecasts] == [CELL.cell, second.cell]
    first = forecasts[0]
    assert first.generated_at == GENERATED
    assert first.hourly[0].valid_at == datetime(2026, 3, 5, tzinfo=UTC)
    assert first.hourly[0].temperature_c == 28.5
    assert first.hourly[1].wind_direction_degrees == 110
    query = parse_qs(urlparse(get.urls[0]).query)
    assert query["timezone"] == ["GMT"]
    assert query["wind_speed_unit"] == ["kmh"]
    assert query["precipitation_unit"] == ["mm"]
    assert query["forecast_days"] == ["7"]
    assert query["latitude"] == [f"{CELL.latitude},{second.latitude}"]


def test_fetch_accepts_a_single_location_object() -> None:
    source = OpenMeteoClient(FakeGet(hourly()))
    assert len(weather.fetch_forecasts(source, [CELL], GENERATED)) == 1


def test_fetch_skips_invalid_cells() -> None:
    second = weather.forecast_cells()[1]
    source = OpenMeteoClient(FakeGet([hourly(precipitation=[0.2]), hourly()]))
    forecasts = weather.fetch_forecasts(source, [CELL, second], GENERATED)
    assert [item.cell for item in forecasts] == [second.cell]


def test_fetch_rejects_a_location_count_mismatch() -> None:
    source = OpenMeteoClient(FakeGet([hourly()]))
    with pytest.raises(RuntimeError, match="unexpected number of locations"):
        _ = weather.fetch_forecasts(source, [CELL, weather.forecast_cells()[1]], GENERATED)


def test_rate_limit_asks_the_scheduler_to_retry_later() -> None:
    def limited(url: str, *, timeout: float) -> bytes:
        del timeout
        headers = Message()
        headers["Retry-After"] = "17"
        raise HTTPError(url, 429, "Too Many Requests", headers, io.BytesIO())

    with pytest.raises(RetryLater) as raised:
        _ = OpenMeteoClient(limited).fetch_locations([CELL])
    assert raised.value.seconds == 17


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
