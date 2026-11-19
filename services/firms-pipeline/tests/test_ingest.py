from datetime import UTC, datetime
from io import BytesIO, StringIO
from pathlib import Path
from urllib.error import HTTPError

import pytest
from pydantic import ValidationError

from wildfire_firms.events import Detection
from wildfire_firms.ingest import fetch_csv, main, parse_csv

FIXTURE = Path(__file__).parent / "fixtures" / "borneo.csv"


def test_normalizes_source_fields_and_utc_times() -> None:
    records = list(parse_csv(FIXTURE.read_text()))

    assert len(records) == 2
    assert records[0].observed_at == datetime(2026, 3, 5, 0, 7, tzinfo=UTC)
    assert records[0].attributes["frp"] == "12.5"
    assert records[1].observed_at == datetime(2026, 3, 5, 12, 34, tzinfo=UTC)
    assert Detection.model_validate_json(records[0].model_dump_json()) == records[0]


@pytest.mark.parametrize(
    "text",
    [
        "Invalid MAP_KEY",
        "latitude,longitude,acq_date,acq_time,satellite\n91,114,2026-03-05,12,N20\n",
        "latitude,longitude,acq_date,acq_time,satellite\n0,114,2026-03-05,2460,N20\n",
        "latitude,longitude,acq_date,acq_time,satellite\n0,114,2026-03-05,12\n",
    ],
)
def test_rejects_bad_responses_and_detections(text: str) -> None:
    with pytest.raises(ValueError):
        list(parse_csv(text))


def test_event_rejects_naive_timestamp() -> None:
    record = next(parse_csv(FIXTURE.read_text())).model_dump()
    record["observed_at"] = datetime(2026, 3, 5)
    with pytest.raises(ValidationError):
        Detection.model_validate(record)


def test_requires_key_and_valid_day_range() -> None:
    with pytest.raises(ValueError, match="FIRMS_MAP_KEY"):
        fetch_csv("", 1)
    with pytest.raises(ValueError, match="between 1 and 5"):
        fetch_csv("key", 6)


def test_firms_request_uses_region_and_redacts_key_on_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(url: str, *, timeout: int) -> None:
        assert url.endswith("/test-key/VIIRS_NOAA20_NRT/92,-12,156,29/2")
        assert timeout == 60
        raise HTTPError(url, 403, "Forbidden", {}, BytesIO())

    monkeypatch.setattr("wildfire_firms.ingest.urlopen", fail)
    with pytest.raises(RuntimeError, match="HTTP 403") as error:
        fetch_csv("test-key", 2)
    assert "test-key" not in str(error.value)


def test_firms_success_response_is_decoded_and_parsed(monkeypatch: pytest.MonkeyPatch) -> None:
    content = FIXTURE.read_bytes()
    monkeypatch.setattr(
        "wildfire_firms.ingest.urlopen",
        lambda url, timeout: BytesIO(b"\xef\xbb\xbf" + content),
    )
    assert len(list(parse_csv(fetch_csv("test-key", 1)))) == 2


@pytest.mark.parametrize("arguments", [["--csv", str(FIXTURE)], ["--key-stdin"]])
def test_ingestion_filters_before_publishing(
    monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    published: list[Detection] = []
    monkeypatch.setattr("sys.argv", ["ingest", *arguments])
    monkeypatch.setattr("sys.stdin", StringIO("test-key\n"))

    def fake_fetch(_key: str, _days: int) -> str:
        return FIXTURE.read_text()

    def fake_filter(records: list[Detection], _url: str) -> list[Detection]:
        return records[1:]

    def capture(records: list[Detection], _bootstrap: str) -> int:
        published.extend(records)
        return len(records)

    monkeypatch.setattr("wildfire_firms.ingest.fetch_csv", fake_fetch)
    monkeypatch.setattr("wildfire_firms.ingest.filter_countries", fake_filter)
    monkeypatch.setattr("wildfire_firms.ingest.publish", capture)
    main()
    assert published == list(parse_csv(FIXTURE.read_text()))[1:]
