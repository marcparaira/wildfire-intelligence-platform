import hashlib
import io
import json
import math
import uuid
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from email.message import Message
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse

import pytest
from PIL import Image

from wildfire_events.cells import partition_cell
from wildfire_events.models import (
    SENTINEL_CONTEXT_DAYS,
    FireIncident,
    ImageryLayer,
    ImageryProvider,
    IncidentDay,
    IncidentStatus,
    SatelliteScene,
)
from wildfire_imagery.planetary_computer import PlanetaryComputerClient, SentinelGranule
from wildfire_imagery.scenes import FetchedImage
from wildfire_imagery.sentinel import (
    TILE_PIXELS,
    SentinelProducer,
    Timing,
    clipped_area,
    composite,
    plan_passes,
    reaches,
    search_areas,
    search_window,
    tile_activity,
)
from wildfire_imagery.tiles import Bounds, Tile
from wildfire_producers.publisher import Publisher
from wildfire_producers.scheduler import RetryLater

NOW = datetime(2026, 3, 10, 12, tzinfo=UTC)
TODAY = NOW.date()
TILE = Tile(228, 2)  # 114.0-114.5 E, 1.0-1.5 N
GRANULE_BOUNDS: Bounds = (113.9, 0.9, 114.9, 1.9)


def incident(
    *,
    days: Sequence[date],
    status: IncidentStatus = IncidentStatus.GROWING,
    bounds: Bounds = (114.1, 1.1, 114.2, 1.2),
) -> FireIncident:
    west, south, east, north = bounds
    latitude, longitude = (south + north) / 2, (west + east) / 2
    last = datetime.combine(max(days), datetime.min.time(), tzinfo=UTC)
    return FireIncident(
        incident_id=uuid.uuid4(),
        revision=1,
        status=status,
        merged_into=uuid.uuid4() if status is IncidentStatus.MERGED else None,
        h3_cell=partition_cell(latitude, longitude),
        first_observed_at=datetime.combine(min(days), datetime.min.time(), tzinfo=UTC),
        last_observed_at=last,
        updated_at=last,
        detection_count=len(days),
        footprint_cells=1,
        footprint_km2=5.0,
        latitude=latitude,
        longitude=longitude,
        west=west,
        south=south,
        east=east,
        north=north,
        total_frp_mw=0,
        products=[],
        daily=[IncidentDay(day=day, detections=1, new_cells=1) for day in days],
    )


def granule(
    day: date, *, name: str = "T49NGA", cloud: float = 10.0, bounds: Bounds = GRANULE_BOUNDS
) -> SentinelGranule:
    return SentinelGranule(
        item_id=f"S2B_MSIL2A_{day:%Y%m%d}T021349_R060_{name}",
        platform="Sentinel-2B",
        acquired_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=2),
        cloud_cover=cloud,
        bounds=bounds,
    )


def webp(width: int, height: int, covered: tuple[int, int, int, int] | None) -> bytes:
    """A transparent WebP with an opaque orange rectangle (left, top, right, bottom)."""
    image = Image.new("RGBA", (width, height))
    if covered is not None:
        image.paste((230, 120, 40, 255), covered)
    output = io.BytesIO()
    image.save(output, format="WEBP", lossless=True)
    return output.getvalue()


def days_ago(*offsets: int) -> list[date]:
    return [TODAY - timedelta(days=offset) for offset in offsets]


def test_activity_maps_tiles_to_their_days_and_keeps_recent_past_activity() -> None:
    # The window counts back from the newest activity (yesterday), like plan_scenes.
    old = TODAY - timedelta(days=1 + 5 + SENTINEL_CONTEXT_DAYS)
    activity = tile_activity(
        [
            incident(days=days_ago(1, 2)),
            incident(days=days_ago(9), bounds=(114.6, 1.1, 114.7, 1.2)),
            incident(days=[old], bounds=(115.1, 1.1, 115.2, 1.2)),
            incident(
                days=days_ago(0), status=IncidentStatus.MERGED, bounds=(116.1, 1.1, 116.2, 1.2)
            ),
        ],
        NOW,
        days=5,
    )
    # Activity nine days ago still counts (for the passes after it); older does not.
    assert activity == {TILE: frozenset(days_ago(1, 2)), Tile(229, 2): frozenset(days_ago(9))}
    assert search_window(activity, NOW) == (
        TODAY - timedelta(days=9 + SENTINEL_CONTEXT_DAYS),
        TODAY,
    )


def test_search_areas_merge_neighbouring_tiles_of_a_row() -> None:
    tiles = [Tile(228, 2), Tile(229, 2), Tile(231, 2), Tile(228, 3)]
    assert search_areas(tiles) == [
        (114.0, 1.0, 115.0, 1.5),
        (115.5, 1.0, 116.0, 1.5),
        (114.0, 1.5, 114.5, 2.0),
    ]


def test_passes_are_ordered_during_then_after_then_before_activity() -> None:
    activity = {TILE: frozenset(days_ago(5, 6))}
    far = TODAY - timedelta(days=6 + SENTINEL_CONTEXT_DAYS + 1)
    elsewhere: Bounds = (120.0, 5.0, 121.0, 6.0)
    granules = [
        granule(TODAY - timedelta(days=8)),
        granule(TODAY - timedelta(days=5)),
        granule(TODAY - timedelta(days=2)),
        granule(far),
        granule(TODAY - timedelta(days=6), bounds=elsewhere),
    ]
    requests = plan_passes(activity, granules)
    assert [(request.scene.day, request.timing) for request in requests] == [
        (TODAY - timedelta(days=5), Timing.DURING),
        (TODAY - timedelta(days=2), Timing.AFTER),
        (TODAY - timedelta(days=8), Timing.BEFORE),
    ]
    assert {request.scene.layer for request in requests} == {ImageryLayer.SENTINEL2_SWIR}


def test_tiles_are_those_of_the_detections_without_a_margin() -> None:
    # 0.02 degrees from the tile edge: the daily imagery's margin would add x229y2.
    near_edge = incident(days=days_ago(1), bounds=(114.40, 1.1, 114.48, 1.2))
    assert set(tile_activity([near_edge], NOW, days=5)) == {TILE}


def test_granules_reach_a_tile_by_their_footprint_not_their_bounding_box() -> None:
    day = TODAY - timedelta(days=1)
    tile = TILE.bounds  # 114.0-114.5 E, 1.0-1.5 N
    # Swath edge: the bounding box covers the tile, the data a sliver of its corner.
    sliver = ((114.497, 0.9), (114.9, 0.9), (114.9, 1.9), (114.498, 1.9), (114.497, 0.9))
    wedge = ((114.3, 0.9), (114.9, 0.9), (114.9, 1.9), (114.4, 1.9), (114.3, 0.9))
    edge = granule(day, name="T49NGA", bounds=(114.3, 0.9, 114.9, 1.9))
    assert not reaches(replace(edge, footprint=(sliver,)), tile)
    assert reaches(replace(edge, footprint=(wedge,)), tile)
    assert reaches(edge, tile)  # no footprint: the bounding box decides
    # The wedge's left edge crosses the tile from 114.31 E (south) to 114.36 E (north).
    assert math.isclose(clipped_area(wedge, tile), 0.5 * (0.19 + 0.14) / 2)
    assert clipped_area(wedge, (120.0, 1.0, 120.5, 1.5)) == 0.0


def test_a_pass_groups_its_granules_with_the_clearest_drawn_last() -> None:
    day = TODAY - timedelta(days=1)
    clear, cloudy = granule(day, name="T49NGA", cloud=5), granule(day, name="T49NHA", cloud=60)
    [request] = plan_passes({TILE: frozenset([day])}, [clear, cloudy, clear])
    assert request.granules == (cloudy, clear)


def test_composite_overlays_images_and_measures_coverage() -> None:
    left = webp(8, 4, (0, 0, 4, 4))
    right = webp(8, 4, (4, 0, 8, 4))
    assert composite([left, right], 8, 4)[1] == 1.0
    assert composite([webp(8, 4, None)], 8, 4)[1] == 0.0
    content, coverage = composite([left], 8, 4)
    assert coverage == 0.5
    # The area without data stays transparent.
    with Image.open(io.BytesIO(content)) as image:
        assert (image.format, image.size, image.mode) == ("WEBP", (8, 4), "RGBA")
        assert image.getchannel("A").getpixel((6, 1)) == 0
        assert image.getchannel("A").getpixel((1, 1)) == 255


@pytest.mark.parametrize(
    ("images", "message"),
    [([b"RIFF....WEBPnot really"], "undecodable"), ([webp(4, 4, None)], "expected")],
)
def test_composite_rejects_broken_images(images: list[bytes], message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        _ = composite(images, 8, 4)


class FakeHttp:
    """Canned GET and POST responses; records the requests."""

    def __init__(
        self, *, pages: Sequence[object] = (), image: bytes = b"", error: Exception | None = None
    ) -> None:
        self.pages: list[object] = list(pages)
        self.image: bytes = image
        self.error: Exception | None = error
        self.posts: list[tuple[str, dict[str, object]]] = []
        self.gets: list[str] = []

    def get(self, url: str, *, timeout: float) -> bytes:
        del timeout
        self.gets.append(url)
        if self.error is not None:
            raise self.error
        return self.image

    def post(self, url: str, body: bytes, *, timeout: float) -> bytes:
        del timeout
        self.posts.append((url, json.loads(body)))
        if self.error is not None:
            raise self.error
        page = self.pages.pop(0)
        return page if isinstance(page, bytes) else json.dumps(page).encode()

    def client(self) -> PlanetaryComputerClient:
        return PlanetaryComputerClient(get=self.get, post=self.post)


def stac_item(item_id: str, bbox: list[float], **extra: object) -> dict[str, object]:
    return {
        "id": item_id,
        "bbox": bbox,
        **extra,
        "properties": {
            "datetime": "2026-03-09T02:13:49.024000Z",
            "platform": "Sentinel-2B",
            "eo:cloud_cover": 12.5,
        },
    }


def test_search_posts_the_areas_and_follows_the_next_pages() -> None:
    next_body = {"collections": ["sentinel-2-l2a"], "token": "next:abc"}
    http = FakeHttp(
        pages=[
            {
                "features": [stac_item("A", [113.9, 0.9, 114.9, 1.9])],
                "links": [
                    {
                        "rel": "next",
                        "method": "POST",
                        "href": "https://pc.example/search",
                        "body": next_body,
                    }
                ],
            },
            {
                "features": [
                    stac_item(
                        "B",
                        [113.9, 0.9, 0, 114.9, 1.9, 0],
                        geometry={
                            "type": "MultiPolygon",
                            "coordinates": [[[[114, 1, 0], [115, 1, 0], [115, 2, 0], [114, 1, 0]]]],
                        },
                    )
                ],
                "links": [],
            },
        ]
    )
    found = http.client().search([(114.0, 1.0, 115.0, 1.5)], date(2026, 3, 1), TODAY, 90)

    assert [item.item_id for item in found] == ["A", "B"]
    assert found[1].bounds == (113.9, 0.9, 114.9, 1.9)
    assert found[0].footprint == ()
    assert found[1].footprint == (((114, 1), (115, 1), (115, 2), (114, 1)),)
    assert found[0].acquired_at == datetime(2026, 3, 9, 2, 13, 49, 24000, tzinfo=UTC)
    (_, first), (second_url, second) = http.posts
    assert first["intersects"] == {
        "type": "MultiPolygon",
        "coordinates": [[[[114.0, 1.0], [115.0, 1.0], [115.0, 1.5], [114.0, 1.5], [114.0, 1.0]]]],
    }
    assert first["datetime"] == "2026-03-01T00:00:00Z/2026-03-10T23:59:59Z"
    assert first["query"] == {"eo:cloud_cover": {"lte": 90}}
    assert (second_url, second) == ("https://pc.example/search", next_body)


def _http_error(code: int, retry_after: str | None = None) -> HTTPError:
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return HTTPError("https://pc.example", code, "error", headers, None)


@pytest.mark.parametrize(
    ("http", "expected"),
    [
        (FakeHttp(error=_http_error(429, "30")), RetryLater),
        (FakeHttp(error=_http_error(500)), RuntimeError),
        (FakeHttp(error=URLError("down")), RuntimeError),
        (FakeHttp(pages=[b"not json"]), RuntimeError),
        (FakeHttp(pages=[{"features": [{"id": "A"}]}]), RuntimeError),
    ],
)
def test_search_failures_are_translated(http: FakeHttp, expected: type[Exception]) -> None:
    with pytest.raises(expected):
        _ = http.client().search([(114.0, 1.0, 115.0, 1.5)], TODAY, TODAY, 90)


def test_render_requests_the_swir_bands_over_the_bounds() -> None:
    image = webp(4, 4, None)
    http = FakeHttp(image=image)
    fetched = http.client().render("S2B_X", (114.0, 1.0, 114.5, 1.5), 2048, 2048)

    assert fetched.content == image
    url = urlparse(fetched.url)
    assert url.path.endswith("/item/bbox/114.0,1.0,114.5,1.5/2048x2048.webp")
    query = parse_qs(url.query)
    assert query["item"] == ["S2B_X"]
    assert query["assets"] == ["B12", "B8A", "B04"]
    assert query["rescale"] == ["1000,5000"] * 3


@pytest.mark.parametrize(
    ("http", "expected"),
    [
        (FakeHttp(error=_http_error(429)), RetryLater),
        (FakeHttp(error=_http_error(404)), RuntimeError),
        (FakeHttp(image=b'{"detail": "Not Found"}'), RuntimeError),
    ],
)
def test_render_failures_are_translated(http: FakeHttp, expected: type[Exception]) -> None:
    with pytest.raises(expected):
        _ = http.client().render("S2B_X", (114.0, 1.0, 114.5, 1.5), 4, 4)


class ScriptedSource:
    """Granules to find, and per-granule render answers; records the renders."""

    def __init__(
        self, granules: list[SentinelGranule], answers: dict[str, bytes | Exception]
    ) -> None:
        self.granules: list[SentinelGranule] = granules
        self.answers: dict[str, bytes | Exception] = answers
        self.renders: list[str] = []
        self.searches: int = 0

    def search(
        self, areas: Sequence[Bounds], start: date, end: date, max_cloud_cover: float
    ) -> list[SentinelGranule]:
        del areas, start, end, max_cloud_cover
        self.searches += 1
        return self.granules

    def render(self, item_id: str, bounds: Bounds, width: int, height: int) -> FetchedImage:
        del bounds, width, height
        self.renders.append(item_id)
        answer = self.answers[item_id]
        if isinstance(answer, Exception):
            raise answer
        return FetchedImage(f"https://pc.example/{item_id}", answer)


class MemoryStore:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    def put(self, key: str, content: bytes) -> None:
        self.files[key] = content


class RecordingPublisher:
    """Stands in for the producer and codec behind a ``Publisher``."""

    def __init__(self) -> None:
        self.events: list[SatelliteScene] = []

    def publisher(self) -> Publisher[SatelliteScene]:
        return Publisher(
            producer=self, codec=self, topic="imagery.scenes", key=lambda e: str(e.scene_id)
        )

    def encode(self, event: SatelliteScene) -> bytes:
        self.events.append(event)
        return b""

    def send(
        self,
        topic: str,
        value: object = None,
        key: object = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
    ) -> "RecordingPublisher":
        del topic, value, key, headers
        return self

    def flush(self, timeout: float | None = None) -> None:
        del timeout

    def get(self, timeout: float | None = None) -> "RecordingPublisher":
        del timeout
        return self

    @property
    def timestamp(self) -> int:
        return 0


# Rendering real 2048 px tiles in every test would be slow; a small full-cover image
# of the wrong size is rejected, so tests render at the producer's size once.
FULL = webp(TILE_PIXELS, TILE_PIXELS, (0, 0, TILE_PIXELS, TILE_PIXELS))
HALF = webp(TILE_PIXELS, TILE_PIXELS, (0, 0, TILE_PIXELS // 2, TILE_PIXELS))
EMPTY = webp(TILE_PIXELS, TILE_PIXELS, None)


def producer(
    source: ScriptedSource,
    incidents: list[FireIncident],
    published: RecordingPublisher,
    store: MemoryStore,
    *,
    max_scenes: int = 100,
) -> SentinelProducer:
    return SentinelProducer(
        incidents=lambda: incidents,
        source=source,
        store=store,
        publisher=published.publisher(),
        acquired=set(),
        days=5,
        max_scenes_per_run=max_scenes,
        clock=lambda: NOW,
    )


def test_producer_composites_a_pass_and_publishes_it_once() -> None:
    day = TODAY - timedelta(days=1)
    west, east = granule(day, name="T49NGA", cloud=30), granule(day, name="T49NHA", cloud=10)
    source = ScriptedSource([west, east], {west.item_id: HALF, east.item_id: HALF})
    published, store = RecordingPublisher(), MemoryStore()
    job = producer(source, [incident(days=[day])], published, store)

    assert job.run_once().startswith("found 2 granules; acquired 1 scenes")
    [scene] = published.events
    assert scene.layer is ImageryLayer.SENTINEL2_SWIR
    assert scene.provider is ImageryProvider.PLANETARY_COMPUTER
    assert (scene.acquisition_date, scene.tile, scene.platform) == (day, "x228y2", "Sentinel-2B")
    assert scene.acquired_at == west.acquired_at
    assert scene.cloud_cover == 20.0
    assert scene.granules == [west.item_id, east.item_id]
    assert scene.media_type == "image/webp"
    assert scene.storage_key.endswith(".webp")
    assert scene.sha256 == hashlib.sha256(store.files[scene.storage_key]).hexdigest()
    assert scene.source_url == f"https://pc.example/{east.item_id}"

    assert job.run_once().startswith("found 2 granules; acquired 0 scenes")
    assert len(source.renders) == 2


def test_producer_skips_passes_without_data_and_retries_failures_later() -> None:
    days = days_ago(1, 2, 3)
    granules = [granule(day) for day in days]
    source = ScriptedSource(
        granules,
        {
            granules[0].item_id: EMPTY,
            granules[1].item_id: RuntimeError("HTTP 500"),
            granules[2].item_id: FULL,
        },
    )
    published, store = RecordingPublisher(), MemoryStore()
    job = producer(source, [incident(days=days)], published, store, max_scenes=2)

    assert job.run_once().endswith(
        "acquired 0 scenes, 1 without data, 1 failed, 1 deferred to the next run"
    )
    assert job.run_once().endswith(
        "acquired 1 scenes, 0 without data, 0 failed, 0 deferred to the next run"
    )
    assert [event.acquisition_date for event in published.events] == [days[2]]


def test_producer_run_fails_when_every_request_fails_and_stops_on_rate_limits() -> None:
    day = TODAY - timedelta(days=1)
    found = granule(day)
    failing = ScriptedSource([found], {found.item_id: RuntimeError("down")})
    job = producer(failing, [incident(days=[day])], RecordingPublisher(), MemoryStore())
    with pytest.raises(RuntimeError, match="all 1 Sentinel-2 requests failed"):
        _ = job.run_once()

    limited = ScriptedSource([found], {found.item_id: RetryLater("slow down", 30)})
    job = producer(limited, [incident(days=[day])], RecordingPublisher(), MemoryStore())
    with pytest.raises(RetryLater):
        _ = job.run_once()


def test_producer_does_not_search_without_incidents() -> None:
    source = ScriptedSource([], {})
    job = producer(source, [], RecordingPublisher(), MemoryStore())
    assert job.run_once() == "no incidents need Sentinel-2 imagery"
    assert source.searches == 0
