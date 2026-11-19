import json
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from wildfire_events.models import FirmsDetection
from wildfire_ingestor.firms import fetch_csv, parse_csv, select_countries
from wildfire_ingestor.log import PublishedEvents
from wildfire_ingestor.migrate_legacy import convert
from wildfire_ingestor.publisher import Publisher

FIXTURES = Path(__file__).parent / "fixtures"
VIIRS = (FIXTURES / "viirs_noaa20.csv").read_text()
MODIS = (FIXTURES / "modis.csv").read_text()


def test_normalizes_viirs_rows_with_utc_times_and_partition_keys() -> None:
    parsed = parse_csv(VIIRS, "VIIRS_NOAA20_NRT")

    assert parsed.rejected == []
    first, second = parsed.detections
    assert first.observed_at == datetime(2026, 3, 5, 0, 7, tzinfo=UTC)
    assert second.observed_at == datetime(2026, 3, 5, 12, 34, tzinfo=UTC)
    assert first.instrument == "VIIRS" and first.frp_mw == 12.5
    assert first.attributes["frp"] == "12.5"
    assert first.h3_cell.startswith("82")
    assert FirmsDetection.model_validate_json(first.model_dump_json()) == first
    assert parse_csv(VIIRS, "VIIRS_NOAA20_NRT").detections[0].event_id == first.event_id


def test_normalizes_modis_rows_and_missing_frp() -> None:
    first, second = parse_csv(MODIS, "MODIS_NRT").detections
    assert (first.instrument, first.satellite, first.frp_mw) == ("MODIS", "Aqua", 30.2)
    assert second.frp_mw is None


@pytest.mark.parametrize(
    "text",
    ["Invalid MAP_KEY", "latitude,latitude,longitude,acq_date,acq_time,satellite\n"],
)
def test_rejects_malformed_responses(text: str) -> None:
    with pytest.raises(ValueError):
        parse_csv(text, "VIIRS_NOAA20_NRT")


def test_rejects_bad_rows_without_dropping_good_ones() -> None:
    header = "latitude,longitude,acq_date,acq_time,satellite\n"
    rows = [
        "91,114,2026-03-05,12,N20",
        "0,114,2026-03-05,2460,N20",
        "0,114,2026-03-05,12",
        "0,abc,2026-03-05,12,N20",
        "0,114,2026-03-05,12,N20",
    ]
    parsed = parse_csv(header + "\n".join(rows) + "\n", "VIIRS_NOAA20_NRT")
    assert len(parsed.detections) == 1
    assert [reason.split(":")[0] for reason in parsed.rejected] == [
        "line 2", "line 3", "line 4", "line 5"
    ]


def test_requires_key_product_and_valid_day_range() -> None:
    with pytest.raises(ValueError, match="FIRMS_MAP_KEY"):
        fetch_csv("", "MODIS_NRT", 1)
    with pytest.raises(ValueError, match="Unknown"):
        fetch_csv("key", "LANDSAT", 1)
    with pytest.raises(ValueError, match="between 1 and 5"):
        fetch_csv("key", "MODIS_NRT", 6)


def test_firms_request_uses_product_and_region_and_redacts_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(url: str, *, timeout: int) -> None:
        assert url.endswith("/test-key/VIIRS_SNPP_NRT/92,-12,156,29/2")
        raise HTTPError(url, 403, "Forbidden", {}, BytesIO())

    monkeypatch.setattr("wildfire_ingestor.firms.urlopen", fail)
    with pytest.raises(RuntimeError, match="HTTP 403") as error:
        fetch_csv("test-key", "VIIRS_SNPP_NRT", 2)
    assert "test-key" not in str(error.value)


def test_country_filter_covers_selected_countries_and_excludes_sea_and_neighbors() -> None:
    cities = [
        (16.866, 96.195),   # Myanmar
        (13.756, 100.502),  # Thailand
        (17.975, 102.633),  # Laos
        (11.557, 104.928),  # Cambodia
        (21.028, 105.854),  # Vietnam
        (4.903, 114.939),   # Brunei
        (3.139, 101.687),   # Malaysia
        (1.352, 103.82),    # Singapore
        (14.599, 120.984),  # Philippines
        (-6.208, 106.845),  # Indonesia
        (-9.443, 147.18),   # Papua New Guinea
        (-8.56, 125.573),   # Timor-Leste
        (27.33, 97.42),     # Northern Myanmar
        (-6.22, 155.56),    # Bougainville, Papua New Guinea
    ]
    excluded = [(2, 105), (25, 100), (-13, 135), (-20, 170)]
    template = parse_csv(VIIRS, "VIIRS_NOAA20_NRT").detections[0]
    detections = [
        template.model_copy(update={"latitude": lat, "longitude": lon})
        for lat, lon in cities + excluded
    ]
    assert select_countries(detections) == detections[: len(cities)]


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes, list]] = []

    def send(self, topic: str, *, key: bytes, value: bytes, headers: list) -> SimpleNamespace:
        self.sent.append((topic, key, value, headers))
        return SimpleNamespace(get=lambda timeout: SimpleNamespace(timestamp=1_000))

    def flush(self) -> None:
        pass


class FakeCodec:
    def encode(self, event: FirmsDetection) -> bytes:
        return event.model_dump_json().encode()


def test_publisher_keys_by_h3_cell_and_never_republishes() -> None:
    events = parse_csv(VIIRS, "VIIRS_NOAA20_NRT").detections
    producer = FakeProducer()
    published = PublishedEvents(window=timedelta(days=3))
    publisher = Publisher(
        producer=producer,
        codec=FakeCodec(),  # type: ignore[arg-type]
        topic="firms.detections",
        key=lambda event: event.h3_cell,
        event_id=lambda event: event.event_id,
        published=published,
    )

    assert publisher.publish([*events, events[0]]) == (2, 1)
    assert publisher.publish(events) == (0, 2)
    topic, key, _, headers = producer.sent[0]
    assert (topic, key.decode()) == ("firms.detections", events[0].h3_cell)
    assert headers == [("event_id", str(events[0].event_id).encode())]
    assert len(published) == 2


def test_published_events_expire_after_the_window() -> None:
    published = PublishedEvents(timedelta(hours=1))
    published.add("old", published_at=0)
    published.add("new", published_at=7200)
    published.prune(now=7200)
    assert "old" not in published and "new" in published


def test_legacy_events_keep_the_live_event_id() -> None:
    live = parse_csv(VIIRS, "VIIRS_NOAA20_NRT").detections[0]
    legacy = {
        "schema_version": 1,
        "source": "VIIRS_NOAA20_NRT",
        "satellite": "N20",
        "observed_at": "2026-03-05T00:07:00Z",
        "latitude": "0.5234",
        "longitude": "114.1234",
        "attributes": live.attributes,
    }
    converted = convert(json.dumps(legacy).encode(), live.ingested_at)
    assert converted == live
    with pytest.raises(ValueError):
        convert(json.dumps({**legacy, "schema_version": 2}).encode(), live.ingested_at)
