import httpx
from fastapi.testclient import TestClient
from pydantic import TypeAdapter

from wildfire_api.main import app

client: httpx.Client = TestClient(app)


def test_health_returns_ok() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert TypeAdapter(dict[str, str]).validate_json(response.content) == {"status": "ok"}
