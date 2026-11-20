import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.message import Message
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

import pytest

from wildfire_events.countries import CountryLocator, selected_countries
from wildfire_events.models import FirmsDetection, FirmsProduct, Instrument
from wildfire_ingestor.config import parse_products
from wildfire_ingestor.detections import FirmsProducer, parse_csv, select_countries
from wildfire_ingestor.publisher import PublishedEvents, Publisher
from wildfire_ingestor.sources import FirmsClient

FIXTURES = Path(__file__).parent / "fixtures"
VIIRS = (FIXTURES / "viirs_noaa20.csv").read_text()
MODIS = (FIXTURES / "modis.csv").read_text()


def test_normalizes_viirs_rows_with_utc_times_and_partition_keys() -> None:
    parsed = parse_csv(VIIRS, FirmsProduct.VIIRS_NOAA20_NRT)

    assert parsed.rejected == []
    first, second = parsed.detections
    assert first.observed_at == datetime(2026, 3, 5, 0, 7, tzinfo=UTC)
    assert second.observed_at == datetime(2026, 3, 5, 12, 34, tzinfo=UTC)
    assert first.instrument is Instrument.VIIRS
    assert first.frp_mw == 12.5
    assert first.attributes["frp"] == "12.5"
    assert first.h3_cell.startswith("82")
    assert FirmsDetection.model_validate_json(first.model_dump_json()) == first
    assert parse_csv(VIIRS, FirmsProduct.VIIRS_NOAA20_NRT).detections[0].event_id == first.event_id


def test_normalizes_modis_rows_and_missing_frp() -> None:
    first, second = parse_csv(MODIS, FirmsProduct.MODIS_NRT).detections
    assert (first.instrument, first.satellite, first.frp_mw) == ("MODIS", "Aqua", 30.2)
    assert second.frp_mw is None


@pytest.mark.parametrize(
    "text",
    ["Invalid MAP_KEY", "latitude,latitude,longitude,acq_date,acq_time,satellite\n"],
)
def test_rejects_malformed_responses(text: str) -> None:
    with pytest.raises(ValueError, match="CSV headers"):
        _ = parse_csv(text, FirmsProduct.VIIRS_NOAA20_NRT)


def test_rejects_bad_rows_without_dropping_good_ones() -> None:
    header = "latitude,longitude,acq_date,acq_time,satellite\n"
    rows = [
        "91,114,2026-03-05,12,N20",
        "0,114,2026-03-05,2460,N20",
        "0,114,2026-03-05,12",
        "0,abc,2026-03-05,12,N20",
        "0,114,2026-03-05,12,N20",
    ]
    parsed = parse_csv(header + "\n".join(rows) + "\n", FirmsProduct.VIIRS_NOAA20_NRT)
    assert len(parsed.detections) == 1
    assert [reason.split(":")[0] for reason in parsed.rejected] == [
        "line 2",
        "line 3",
        "line 4",
        "line 5",
    ]


def test_requires_key_and_valid_day_range() -> None:
    with pytest.raises(ValueError, match="FIRMS_MAP_KEY"):
        _ = FirmsClient(" ")
    with pytest.raises(ValueError, match="between 1 and 5"):
        _ = FirmsClient("key").fetch_csv(FirmsProduct.MODIS_NRT, 6)


def test_firms_request_uses_product_and_region_and_redacts_key() -> None:
    def fail(url: str, *, timeout: float) -> bytes:
        del timeout
        assert url.endswith("/test-key/VIIRS_SNPP_NRT/92,-12,156,29/2")
        raise HTTPError(url, 403, "Forbidden", Message(), BytesIO())

    with pytest.raises(RuntimeError, match="HTTP 403") as error:
        _ = FirmsClient("test-key", get=fail).fetch_csv(FirmsProduct.VIIRS_SNPP_NRT, 2)
    assert "test-key" not in str(error.value)
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__


def test_product_list_parses_to_enums_and_rejects_unknown_names() -> None:
    assert parse_products(" MODIS_NRT , VIIRS_SNPP_NRT,") == (
        FirmsProduct.MODIS_NRT,
        FirmsProduct.VIIRS_SNPP_NRT,
    )
    with pytest.raises(ValueError, match="Unknown FIRMS products: LANDSAT"):
        _ = parse_products("MODIS_NRT,LANDSAT")


def test_country_filter_covers_selected_countries_and_excludes_sea_and_neighbors() -> None:
    cities = [
        (16.866, 96.195),  # Myanmar
        (13.756, 100.502),  # Thailand
        (17.975, 102.633),  # Laos
        (11.557, 104.928),  # Cambodia
        (21.028, 105.854),  # Vietnam
        (4.903, 114.939),  # Brunei
        (3.139, 101.687),  # Malaysia
        (1.352, 103.82),  # Singapore
        (14.599, 120.984),  # Philippines
        (-6.208, 106.845),  # Indonesia
        (-9.443, 147.18),  # Papua New Guinea
        (-8.56, 125.573),  # Timor-Leste
        (27.33, 97.42),  # Northern Myanmar
        (-6.22, 155.56),  # Bougainville, Papua New Guinea
    ]
    excluded = [(2, 105), (25, 100), (-13, 135), (-20, 170)]
    template = parse_csv(VIIRS, FirmsProduct.VIIRS_NOAA20_NRT).detections[0]
    detections = [
        template.model_copy(update={"latitude": lat, "longitude": lon})
        for lat, lon in cities + excluded
    ]
    assert select_countries(detections, selected_countries()) == detections[: len(cities)]


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True, slots=True)
class FakeMetadata:
    timestamp: int = field(default_factory=now_ms)


class FakeResult:
    def get(self, timeout: float | None = None) -> FakeMetadata:
        del timeout
        return FakeMetadata()


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, object, object, list[tuple[str, bytes]]]] = []

    def send(
        self,
        topic: str,
        value: object = None,
        key: object = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
    ) -> FakeResult:
        self.sent.append((topic, key, value, list(headers or [])))
        return FakeResult()

    def flush(self, timeout: float | None = None) -> None:
        del timeout


class FakeCodec:
    def encode(self, event: FirmsDetection) -> bytes:
        return event.model_dump_json().encode()


def test_publisher_keys_by_h3_cell_and_never_republishes() -> None:
    events = parse_csv(VIIRS, FirmsProduct.VIIRS_NOAA20_NRT).detections
    producer = FakeProducer()
    published = PublishedEvents(window=timedelta(days=3))
    publisher = detection_publisher(producer, published)

    assert publisher.publish([*events, events[0]]) == (2, 1)
    assert publisher.publish(events) == (0, 2)
    topic, key, _, headers = producer.sent[0]
    assert (topic, key) == ("firms.detections", events[0].h3_cell.encode())
    assert headers == [("event_id", str(events[0].event_id).encode())]
    assert len(published) == 2


def test_published_events_expire_after_the_window() -> None:
    published = PublishedEvents(timedelta(hours=1))
    published.add("old", published_at=0)
    published.add("new", published_at=7200)
    published.prune(now=7200)
    assert "old" not in published
    assert "new" in published


def detection_publisher(
    producer: FakeProducer, published: PublishedEvents
) -> Publisher[FirmsDetection]:
    return Publisher(
        producer=producer,
        codec=FakeCodec(),
        topic="firms.detections",
        key=lambda event: event.h3_cell,
        event_id=lambda event: event.event_id,
        published=published,
    )


def test_publisher_forgets_ids_older_than_the_window() -> None:
    events = parse_csv(VIIRS, FirmsProduct.VIIRS_NOAA20_NRT).detections
    published = PublishedEvents(timedelta(hours=1))
    published.add(str(events[0].event_id), published_at=0)
    assert detection_publisher(FakeProducer(), published).publish(events) == (2, 0)


@dataclass(frozen=True, slots=True)
class FakeFirms:
    text: str

    def fetch_csv(self, product: FirmsProduct, day_range: int) -> str:
        assert (product, day_range) == (FirmsProduct.VIIRS_NOAA20_NRT, 2)
        return self.text


def test_producer_publishes_only_detections_in_selected_countries() -> None:
    producer = FakeProducer()
    publisher = detection_publisher(producer, PublishedEvents(timedelta(days=3)))
    firms = FirmsProducer(
        FirmsProduct.VIIRS_NOAA20_NRT, 2, FakeFirms(VIIRS), publisher, selected_countries()
    )
    summary = firms.run_once()
    assert "2 rows, 0 rejected" in summary
    assert summary.endswith("2 in selected countries, 2 published, 0 already published")
    assert len(producer.sent) == 2

    nowhere = FirmsProducer(
        FirmsProduct.VIIRS_NOAA20_NRT,
        2,
        FakeFirms(VIIRS),
        publisher,
        CountryLocator({"features": []}),
    )
    assert nowhere.run_once().endswith("0 in selected countries, 0 published, 0 already published")
