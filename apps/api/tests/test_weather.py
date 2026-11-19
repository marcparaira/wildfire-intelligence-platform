import os
import uuid
from datetime import UTC, datetime

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo

from wildfire_api.main import app

client = TestClient(app)


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
    assert client.get(f"/weather/forecast{query}").status_code == 422


def test_weather_is_read_only() -> None:
    assert client.post("/weather/forecast").status_code == 405


def test_weather_database_failure_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost:1/unavailable")
    response = client.get("/weather/forecast")
    assert response.status_code == 503
    assert response.json()["detail"] == "Weather data is currently unavailable"


@pytest.mark.skipif(
    not os.environ.get("API_TEST_DATABASE_URL"),
    reason="set API_TEST_DATABASE_URL to run PostGIS integration test",
)
def test_weather_forecasts_are_read_from_the_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    database_url = os.environ["API_TEST_DATABASE_URL"]
    schema = f"api_weather_test_{uuid.uuid4().hex}"
    first, second = datetime(2026, 3, 5, 0, tzinfo=UTC), datetime(2026, 3, 5, 1, tzinfo=UTC)
    with psycopg.connect(database_url, autocommit=True) as db:
        db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            db.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
            db.execute(
                """
                CREATE TABLE weather_cells (
                    cell text PRIMARY KEY, latitude double precision, longitude double precision,
                    boundary geometry(Polygon, 4326), generated_at timestamptz
                );
                CREATE TABLE weather_hourly (
                    cell text, valid_at timestamptz, temperature_c double precision,
                    precipitation_mm double precision, wind_speed_kmh double precision,
                    wind_direction_degrees double precision
                );
                INSERT INTO weather_cells VALUES
                ('near', 0, 114, ST_MakeEnvelope(113, -1, 115, 1, 4326),
                 '2026-03-04 18:00+00'),
                ('far', 0, 130, ST_MakeEnvelope(129, -1, 131, 1, 4326),
                 '2026-03-04 21:00+00');
                INSERT INTO weather_hourly VALUES
                ('near', '2026-03-05 00:00+00', 28.5, 0.2, 12, 90),
                ('near', '2026-03-05 01:00+00', 29.0, 0, 10, 100),
                ('far', '2026-03-05 00:00+00', 27.5, 0, 9, 120);
                """
            )
            monkeypatch.setenv(
                "DATABASE_URL",
                make_conninfo(database_url, options=f"-csearch_path={schema},public"),
            )
            params = {"west": 108, "south": -5, "east": 120, "north": 8}
            response = client.get(
                "/weather/forecast", params={**params, "at": second.isoformat()}
            )
            assert response.status_code == 200
            result = response.json()
            assert [datetime.fromisoformat(value) for value in result["times"]] == [first, second]
            assert [item["cell"] for item in result["items"]] == ["near"]
            assert result["items"][0]["temperature_c"] == 29.0
            assert result["items"][0]["boundary"][0] == [113, -1]
            assert datetime.fromisoformat(result["generated_at"]).hour == 21
            assert datetime.fromisoformat(result["oldest_generated_at"]).hour == 18

            everywhere = client.get("/weather/forecast", params={"at": first.isoformat()})
            assert len(everywhere.json()["items"]) == 2
            missing = client.get(
                "/weather/forecast", params={"at": "2026-03-06T00:00:00+00:00"}
            )
            assert missing.status_code == 422

            db.execute("TRUNCATE weather_hourly")
            empty = client.get("/weather/forecast")
            assert empty.status_code == 503
            assert empty.json()["detail"] == "No weather forecast has been ingested yet"
        finally:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
