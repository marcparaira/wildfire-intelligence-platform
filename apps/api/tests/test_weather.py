import io
import json
import os
import uuid
from datetime import UTC, datetime
from typing import Self
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo

import wildfire_api.weather as weather
from wildfire_api.main import app

client = TestClient(app)


def test_weather_grid_covers_the_configured_region() -> None:
    cells = weather.grid_cells()

    assert len(cells) == 308
    assert min(cell.west for cell in cells) == weather.REGION_BOUNDS[0]
    assert min(cell.south for cell in cells) == weather.REGION_BOUNDS[1]
    assert max(cell.east for cell in cells) == weather.REGION_BOUNDS[2]
    assert max(cell.north for cell in cells) == weather.REGION_BOUNDS[3]
    assert all(cell.west < cell.east and cell.south < cell.north for cell in cells)


def test_fetch_chunk_parses_hourly_values_and_uses_explicit_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "hourly": {
            "time": ["2026-03-05T00:00", "2026-03-05T01:00"],
            "temperature_2m": [28.5, 28.0],
            "precipitation": [0.2, 0],
            "wind_speed_10m": [12, 14],
            "wind_direction_10m": [90, 110],
        }
    }

    class Response:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(payload).encode()

    requested_urls: list[str] = []

    def fake_urlopen(url: str, timeout: int) -> Response:
        requested_urls.append(url)
        assert timeout == 30
        return Response()

    monkeypatch.setattr(weather, "urlopen", fake_urlopen)
    cell = weather.GridCell(1, 115, 114, 0, 116, 2)
    generated_at = datetime(2026, 3, 4, 20, tzinfo=UTC)
    times, rows = weather.fetch_chunk([cell], generated_at)

    assert times == [
        datetime(2026, 3, 5, 0, tzinfo=UTC),
        datetime(2026, 3, 5, 1, tzinfo=UTC),
    ]
    assert len(rows) == 2
    assert rows[0] == (
        generated_at,
        times[0],
        1,
        115,
        114,
        0,
        116,
        2,
        28.5,
        0.2,
        12,
        90,
    )
    query = parse_qs(urlparse(requested_urls[0]).query)
    assert query["timezone"] == ["GMT"]
    assert query["wind_speed_unit"] == ["kmh"]
    assert query["precipitation_unit"] == ["mm"]
    assert query["forecast_days"] == ["7"]


def test_fetch_chunk_rejects_malformed_provider_data(monkeypatch: pytest.MonkeyPatch) -> None:
    class Response:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"hourly": {"time": []}}'

    monkeypatch.setattr(weather, "urlopen", lambda *_args, **_kwargs: Response())
    with pytest.raises(weather.WeatherProviderError, match="invalid forecast data"):
        weather.fetch_chunk(
            [weather.GridCell(1, 115, 114, 0, 116, 2)],
            datetime(2026, 3, 4, 20, tzinfo=UTC),
        )


def test_fetch_chunk_reports_provider_unavailability(monkeypatch: pytest.MonkeyPatch) -> None:
    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise URLError("unavailable")

    monkeypatch.setattr(weather, "urlopen", unavailable)
    with pytest.raises(weather.WeatherProviderError, match="request failed"):
        weather.fetch_chunk(
            [weather.GridCell(1, 115, 114, 0, 116, 2)],
            datetime(2026, 3, 4, 20, tzinfo=UTC),
        )


def test_fetch_chunk_honors_provider_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(weather, "_RATE_LIMITED_UNTIL", 0.0)

    def rate_limited(url: str, **_kwargs: object) -> None:
        raise HTTPError(url, 429, "Too Many Requests", {"Retry-After": "17"}, io.BytesIO())

    monkeypatch.setattr(weather, "urlopen", rate_limited)
    with pytest.raises(weather.WeatherProviderError) as raised:
        weather.fetch_chunk(
            [weather.GridCell(1, 115, 114, 0, 116, 2)],
            datetime(2026, 3, 4, 20, tzinfo=UTC),
        )
    assert raised.value.retry_after == 17

    def should_not_retry(*_args: object, **_kwargs: object) -> None:
        pytest.fail("rate-limited requests should be backed off")

    monkeypatch.setattr(weather, "urlopen", should_not_retry)
    with pytest.raises(weather.WeatherProviderError) as backed_off:
        weather.fetch_chunk(
            [weather.GridCell(1, 115, 114, 0, 116, 2)],
            datetime(2026, 3, 4, 20, tzinfo=UTC),
        )
    assert backed_off.value.retry_after is not None


@pytest.mark.parametrize(
    "query",
    [
        "?west=108",
        "?west=120&south=-5&east=108&north=8",
        "?west=108&south=8&east=120&north=-5",
        "?at=2026-03-05T00:00:00",
    ],
)
def test_invalid_weather_filters_are_rejected(query: str) -> None:
    assert client.post(f"/weather/forecast{query}").status_code == 422


@pytest.mark.skipif(
    not os.environ.get("API_TEST_DATABASE_URL"),
    reason="set API_TEST_DATABASE_URL to run PostGIS integration test",
)
def test_weather_forecasts_are_stored_and_filtered(monkeypatch: pytest.MonkeyPatch) -> None:
    database_url = os.environ["API_TEST_DATABASE_URL"]
    schema = f"api_weather_test_{uuid.uuid4().hex}"
    valid_at = datetime(2026, 3, 5, 0, tzinfo=UTC)
    with psycopg.connect(database_url, autocommit=True) as db:
        db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            monkeypatch.setenv(
                "DATABASE_URL",
                make_conninfo(database_url, options=f"-csearch_path={schema},public"),
            )

            def forecast(
                generated_at: datetime,
            ) -> tuple[list[datetime], list[weather.ForecastRow]]:
                return (
                    [valid_at],
                    [
                        (
                            generated_at,
                            valid_at,
                            0,
                            114,
                            113,
                            -1,
                            115,
                            1,
                            28.5,
                            0.2,
                            12.0,
                            90.0,
                        ),
                        (
                            generated_at,
                            valid_at,
                            0,
                            130,
                            129,
                            -1,
                            131,
                            1,
                            27.5,
                            0,
                            9.0,
                            120.0,
                        ),
                    ],
                )

            monkeypatch.setattr(weather, "fetch_forecast", forecast)
            params = {
                "west": 108,
                "south": -5,
                "east": 120,
                "north": 8,
                "at": valid_at.isoformat(),
                "refresh": "true",
            }
            response = client.post("/weather/forecast", params=params)
            assert response.status_code == 200
            result = response.json()
            assert len(result["times"]) == 1
            assert len(result["items"]) == 1
            assert result["items"][0]["longitude"] == 114
            assert result["items"][0]["temperature_c"] == 28.5

            monkeypatch.setattr(
                weather,
                "fetch_forecast",
                lambda _generated_at: pytest.fail("fresh forecasts should be reused"),
            )
            cached_params = {key: value for key, value in params.items() if key != "refresh"}
            cached = client.post("/weather/forecast", params=cached_params)
            assert cached.status_code == 200
            assert cached.json()["items"] == result["items"]
        finally:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
