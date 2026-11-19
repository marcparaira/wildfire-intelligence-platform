from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
import pytest
from fastapi.testclient import TestClient
from kafka.structs import TopicPartition

from wildfire_api.live import (
    Bounds,
    DetectionFeed,
    format_position,
    parse_position,
    sse_message,
)
from wildfire_api.main import app
from wildfire_events.cells import partition_cell
from wildfire_events.codec import InvalidEvent
from wildfire_events.models import FirmsDetection, detection_event_id

client: httpx.Client = TestClient(app)
OBSERVED = datetime(2026, 3, 5, 5, 30, tzinfo=UTC)


def detection(latitude: float, longitude: float) -> FirmsDetection:
    return FirmsDetection(
        event_id=detection_event_id("MODIS_NRT", "Terra", OBSERVED, str(latitude), str(longitude)),
        product="MODIS_NRT",
        instrument="MODIS",
        satellite="Terra",
        observed_at=OBSERVED,
        latitude=latitude,
        longitude=longitude,
        h3_cell=partition_cell(latitude, longitude),
        frp_mw=4.5,
        attributes={"frp": "4.5"},
        ingested_at=OBSERVED,
    )


INSIDE = detection(0.5, 114.0)
OUTSIDE = detection(20.0, 100.0)
BORNEO = Bounds(108, -5, 120, 8)


@dataclass
class Record:
    offset: int
    value: bytes | None


class FakeCodec:
    """Decodes a record value that is an index into ``events``; ``b"bad"`` is invalid."""

    def __init__(self, events: list[FirmsDetection]) -> None:
        self.events: list[FirmsDetection] = events

    def decode(self, value: bytes | None) -> FirmsDetection:
        if value is None or value == b"bad":
            raise InvalidEvent("bad record")
        return self.events[int(value)]


def feed_for(events: list[FirmsDetection], position: dict[int, int]) -> DetectionFeed:
    feed = DetectionFeed.__new__(DetectionFeed)
    feed.codec = FakeCodec(events)  # pyright: ignore[reportAttributeAccessIssue]
    feed.position = dict(position)
    return feed


@pytest.mark.parametrize(
    "query",
    [
        "?west=108",
        "?west=120&south=-5&east=108&north=8",
        "?west=108&south=8&east=120&north=-5",
        "?west=-181&south=-5&east=120&north=8",
    ],
)
def test_invalid_bounds_are_rejected(query: str) -> None:
    assert client.get(f"/events/detections{query}").status_code == 422


def test_position_round_trips() -> None:
    position = {2: 1209, 0: 824, 1: 0}
    assert format_position(position) == "0:824,1:0,2:1209"
    assert parse_position(format_position(position)) == position


@pytest.mark.parametrize("value", [None, "", "0:1,x", "0-1", "0:-1", "-1:4", "0:1:2"])
def test_malformed_last_event_id_means_no_position(value: str | None) -> None:
    assert parse_position(value) == {}


def test_sse_message_format() -> None:
    assert sse_message("detections", "[]", "0:1") == "id: 0:1\nevent: detections\ndata: []\n\n"
    assert sse_message(None, None, "0:1") == "id: 0:1\n\n"


def test_batch_filters_bounds_skips_invalid_records_and_advances_position() -> None:
    feed = feed_for([INSIDE, OUTSIDE], {0: 10, 1: 5, 2: 7})
    polled = {
        TopicPartition("firms.detections", 0): [Record(10, b"0"), Record(11, b"bad")],
        TopicPartition("firms.detections", 1): [Record(5, b"1")],
    }
    batch = list(feed._matching(polled, BORNEO))  # pyright: ignore[reportPrivateUsage, reportArgumentType]

    assert [item.event_id for item in batch] == [INSIDE.event_id]
    assert batch[0].id is None and batch[0].duplicate_of is None
    assert batch[0].source == "MODIS_NRT"
    # Every record read moves the position, including invalid and filtered ones.
    assert feed.position == {0: 12, 1: 6, 2: 7}


def test_without_bounds_every_valid_detection_is_sent() -> None:
    feed = feed_for([INSIDE, OUTSIDE], {0: 0})
    polled = {TopicPartition("firms.detections", 0): [Record(0, b"0"), Record(1, b"1")]}
    batch = list(feed._matching(polled, None))  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    assert [item.event_id for item in batch] == [INSIDE.event_id, OUTSIDE.event_id]

