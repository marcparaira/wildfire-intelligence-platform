from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import httpx
import psycopg
import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel

from wildfire_api.main import app
from wildfire_api.stats import (
    DEFAULT_WINDOW,
    MAX_WINDOW,
    HourlyStatsResults,
    InvalidWindow,
    stats_window,
)
from wildfire_events.countries import Country

client: httpx.Client = TestClient(app)
NOW = datetime(2026, 3, 6, 10, 25, tzinfo=UTC)


class ErrorBody(BaseModel):
    detail: str


def test_window_defaults_to_the_last_48_hours() -> None:
    assert stats_window(None, None, NOW) == (NOW - DEFAULT_WINDOW, NOW)
    until = datetime(2026, 3, 1, tzinfo=UTC)
    assert stats_window(None, until, NOW) == (until - DEFAULT_WINDOW, until)
    since = datetime(2026, 3, 6, tzinfo=UTC)
    assert stats_window(since, None, NOW) == (since, NOW)


def test_window_boundaries() -> None:
    assert stats_window(NOW - MAX_WINDOW, NOW, NOW) == (NOW - MAX_WINDOW, NOW)
    assert stats_window(NOW, NOW, NOW) == (NOW, NOW)
    with pytest.raises(InvalidWindow, match="31 days"):
        _ = stats_window(NOW - MAX_WINDOW - timedelta(seconds=1), NOW, NOW)
    with pytest.raises(InvalidWindow, match="later than until"):
        _ = stats_window(NOW, NOW - timedelta(hours=1), NOW)
    with pytest.raises(InvalidWindow, match="timezone"):
        _ = stats_window(NOW.replace(tzinfo=None), None, NOW)


@pytest.mark.parametrize(
    "query",
    [
        "?country=AUS",
        "?country=idn",
        "?since=2026-03-06T00:00:00",
        "?since=2026-01-01T00:00:00Z&until=2026-03-06T00:00:00Z",
        "?since=2026-03-06T00:00:00Z&until=2026-03-05T00:00:00Z",
        "?since=yesterday",
    ],
)
def test_invalid_requests_are_rejected(query: str) -> None:
    assert client.get(f"/stats/hourly{query}").status_code == 422


def test_database_failure_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost:1/unavailable")
    response = client.get("/stats/hourly")
    assert response.status_code == 503
    assert ErrorBody.model_validate_json(response.content).detail == (
        "Statistics are currently unavailable"
    )


def stats(params: Mapping[str, str | list[str]]) -> HourlyStatsResults:
    response = client.get("/stats/hourly", params=params)
    assert response.status_code == 200
    return HourlyStatsResults.model_validate_json(response.content)


def test_hourly_stats_filter_by_window_and_country(database: psycopg.Connection) -> None:
    _ = database.execute(
        """
        INSERT INTO detection_stats_hourly (
            country, hour_start, detections, detections_with_frp, total_frp_mw, max_frp_mw
        ) VALUES
            ('THA', '2026-03-05 01:00:00+00', 4, 3, 30.5, 20.0),
            ('IDN', '2026-03-05 01:00:00+00', 2, 0, 0, NULL),
            ('IDN', '2026-03-05 00:00:00+00', 1, 1, 5.0, 5.0),
            ('IDN', '2026-03-03 00:00:00+00', 9, 9, 90.0, 30.0)
        """
    )
    window = {"since": "2026-03-05T00:00:00Z", "until": "2026-03-05T01:00:00Z"}
    result = stats(window)
    assert [(item.country, item.hour_start.hour) for item in result.items] == [
        (Country.IDN, 0),
        (Country.IDN, 1),
        (Country.THA, 1),
    ]
    assert result.items[1].max_frp_mw is None
    assert result.items[2].total_frp_mw == 30.5
    only_thailand = stats({**window, "country": ["THA"]})
    assert [item.detections for item in only_thailand.items] == [4]
    both = stats({**window, "country": ["THA", "IDN"]})
    assert len(both.items) == 3
    # The default window ends now, long after these hours.
    assert stats({}).items == []
