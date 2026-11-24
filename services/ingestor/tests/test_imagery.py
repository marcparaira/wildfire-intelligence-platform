import hashlib
import io
import uuid
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from email.message import Message
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse

import pytest

from wildfire_events.cells import partition_cell
from wildfire_events.models import (
    FireIncident,
    ImageryLayer,
    IncidentDay,
    IncidentStatus,
    SatelliteScene,
)
from wildfire_ingestor.config import parse_layers
from wildfire_ingestor.imagery import (
    TILE_PIXELS,
    FileSceneStore,
    ImageryProducer,
    SceneRequest,
    Tile,
    is_blank,
    plan_scenes,
    storage_key,
    tiles_covering,
)
from wildfire_ingestor.publisher import Publisher
from wildfire_ingestor.scheduler import RetryLater
from wildfire_ingestor.sources import FetchedImage, GibsClient

NOW = datetime(2026, 3, 6, 13, tzinfo=UTC)
TODAY = NOW.date()
TRUE_COLOR = ImageryLayer.VIIRS_NOAA20_TRUE_COLOR
FALSE_COLOR = ImageryLayer.VIIRS_NOAA20_FALSE_COLOR
JPEG = b"\xff\xd8\xff\xe0" + bytes(range(256)) * 40
BLANK_JPEG = b"\xff\xd8\xff\xe0" + bytes(1800)


def incident(
    *,
    days: Sequence[date],
    status: IncidentStatus = IncidentStatus.GROWING,
    bounds: tuple[float, float, float, float] = (114.1, 1.1, 114.2, 1.2),
    last_observed_at: datetime | None = None,
) -> FireIncident:
    west, south, east, north = bounds
    latitude, longitude = (south + north) / 2, (west + east) / 2
    last = last_observed_at or datetime.combine(max(days), datetime.min.time(), tzinfo=UTC)
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


def test_tiles_cover_the_bounds_with_a_margin() -> None:
    assert tiles_covering(114.1, 1.1, 114.2, 1.2) == [Tile(228, 2)]
    assert Tile(228, 2).name == "x228y2"
    assert Tile(228, 2).bounds == (114.0, 1.0, 114.5, 1.5)
    # The margin reaches into the neighbouring tiles to the west and south.
    assert tiles_covering(114.02, 1.02, 114.2, 1.2) == [
        Tile(227, 1),
        Tile(228, 1),
        Tile(227, 2),
        Tile(228, 2),
    ]
    assert Tile(-1, -3).bounds == (-0.5, -1.5, 0.0, -1.0)


def test_plan_covers_active_ready_days_newest_first_and_shares_tiles() -> None:
    days = [TODAY - timedelta(days=offset) for offset in range(7)]
    recent = incident(days=days, last_observed_at=NOW)
    # A neighbour in the same tile on one shared day adds no scenes.
    neighbour = incident(days=[TODAY - timedelta(days=1)], bounds=(114.3, 1.3, 114.35, 1.35))
    plan = plan_scenes([neighbour, recent], [TRUE_COLOR, FALSE_COLOR], NOW, days=5)
    assert [(request.day, request.layer) for request in plan] == [
        (day, layer) for day in days[:5] for layer in (TRUE_COLOR, FALSE_COLOR)
    ]
    assert {request.tile for request in plan} == {Tile(228, 2)}


def test_plan_waits_for_the_days_composite_and_skips_merged_incidents() -> None:
    morning = NOW.replace(hour=11)
    today_only = incident(days=[TODAY])
    assert plan_scenes([today_only], [TRUE_COLOR], morning, days=5) == []
    assert len(plan_scenes([today_only], [TRUE_COLOR], NOW, days=5)) == 1
    merged = incident(days=[TODAY], status=IncidentStatus.MERGED)
    assert plan_scenes([merged], [TRUE_COLOR], NOW, days=5) == []


def test_plan_counts_days_back_from_the_newest_activity_when_data_is_stale() -> None:
    # Detections stopped arriving a week ago; the incidents still get their days.
    week_ago = TODAY - timedelta(days=7)
    days = [week_ago - timedelta(days=offset) for offset in range(6)]
    stale = incident(days=days)
    plan = plan_scenes([stale], [TRUE_COLOR], NOW, days=5)
    assert [request.day for request in plan] == days[:5]
    assert plan_scenes([], [TRUE_COLOR], NOW, days=5) == []


def test_blank_frames_are_recognized_by_their_size() -> None:
    assert is_blank(BLANK_JPEG, 512, 512)
    assert not is_blank(JPEG, 512, 512)


def test_storage_keys_are_valid_scene_keys() -> None:
    request = SceneRequest(FALSE_COLOR, date(2026, 3, 5), Tile(-3, 2))
    key = storage_key(request, "f" * 64)
    assert key == f"{FALSE_COLOR.value}/2026-03-05/x-3y2-ffffffffffffffff.jpg"


class FakeGet:
    def __init__(self, body: bytes = JPEG, error: Exception | None = None) -> None:
        self.body: bytes = body
        self.error: Exception | None = error
        self.urls: list[str] = []

    def __call__(self, url: str, *, timeout: float) -> bytes:
        del timeout
        self.urls.append(url)
        if self.error is not None:
            raise self.error
        return self.body


def test_gibs_request_uses_wms_1_3_axis_order_and_the_day() -> None:
    get = FakeGet()
    image = GibsClient(get).fetch_image(
        TRUE_COLOR, date(2026, 3, 5), (114.0, 1.0, 114.5, 1.5), 512, 256
    )
    assert image.content == JPEG
    query = parse_qs(urlparse(get.urls[0]).query)
    assert query["LAYERS"] == [TRUE_COLOR.value]
    assert query["BBOX"] == ["1.0,114.0,1.5,114.5"]
    assert query["CRS"] == ["EPSG:4326"]
    assert query["TIME"] == ["2026-03-05"]
    assert (query["WIDTH"], query["HEIGHT"]) == (["512"], ["256"])


def http_error(code: int, retry_after: str | None = None) -> HTTPError:
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return HTTPError("https://gibs.example", code, "error", headers, io.BytesIO())


@pytest.mark.parametrize(
    ("get", "expected"),
    [
        (FakeGet(b"<?xml version='1.0'?><ServiceExceptionReport/>"), RuntimeError),
        (FakeGet(error=http_error(500)), RuntimeError),
        (FakeGet(error=URLError("down")), RuntimeError),
        (FakeGet(error=http_error(429, "30")), RetryLater),
    ],
)
def test_gibs_failures_are_translated(get: FakeGet, expected: type[Exception]) -> None:
    with pytest.raises(expected):
        _ = GibsClient(get).fetch_image(TRUE_COLOR, TODAY, (0, 0, 1, 1), 8, 8)


def test_file_store_writes_once_and_leaves_no_partial_files(tmp_path: Path) -> None:
    store = FileSceneStore(tmp_path)
    store.put("layer/2026-03-05/x1y1-abc.jpg", b"first")
    store.put("layer/2026-03-05/x1y1-abc.jpg", b"second")
    path = tmp_path / "layer/2026-03-05/x1y1-abc.jpg"
    assert path.read_bytes() == b"first"
    assert [item.name for item in path.parent.iterdir()] == ["x1y1-abc.jpg"]


def test_layer_list_parses_to_enums_and_rejects_unknown_names() -> None:
    assert parse_layers(f"{FALSE_COLOR}, {TRUE_COLOR},{FALSE_COLOR}") == (FALSE_COLOR, TRUE_COLOR)
    assert parse_layers("") == ()
    with pytest.raises(ValueError, match="Unknown imagery layers"):
        _ = parse_layers("MODIS_Terra_Nope")


class ScriptedSource:
    """Answers each request from a per-day script; records the requests."""

    def __init__(self, answers: dict[date, bytes | Exception]) -> None:
        self.answers: dict[date, bytes | Exception] = answers
        self.requests: list[tuple[ImageryLayer, date]] = []

    def fetch_image(
        self,
        layer: ImageryLayer,
        day: date,
        bounds: tuple[float, float, float, float],
        width: int,
        height: int,
    ) -> FetchedImage:
        del bounds, width, height
        self.requests.append((layer, day))
        answer = self.answers[day]
        if isinstance(answer, Exception):
            raise answer
        return FetchedImage(f"https://gibs.example/{day}", answer)


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


def producer(
    source: ScriptedSource,
    incidents: list[FireIncident],
    publisher: RecordingPublisher,
    store: MemoryStore,
    *,
    acquired: set[uuid.UUID] | None = None,
    max_scenes: int = 100,
) -> ImageryProducer:
    return ImageryProducer(
        incidents=lambda: incidents,
        source=source,
        store=store,
        publisher=publisher.publisher(),
        acquired=set() if acquired is None else acquired,
        layers=(TRUE_COLOR,),
        days=5,
        max_scenes_per_run=max_scenes,
        clock=lambda: NOW,
    )


def test_producer_stores_then_publishes_scene_metadata_once() -> None:
    yesterday = TODAY - timedelta(days=1)
    source = ScriptedSource({TODAY: JPEG, yesterday: JPEG})
    published, store = RecordingPublisher(), MemoryStore()
    job = producer(source, [incident(days=[yesterday, TODAY])], published, store)

    assert job.run_once().startswith("acquired 2 scenes")
    scene = published.events[0]
    assert (scene.acquisition_date, scene.tile, scene.layer) == (TODAY, "x228y2", TRUE_COLOR)
    assert (scene.west, scene.south, scene.east, scene.north) == (114.0, 1.0, 114.5, 1.5)
    assert (scene.width, scene.height) == (TILE_PIXELS, TILE_PIXELS)
    assert scene.sha256 == hashlib.sha256(JPEG).hexdigest()
    assert scene.byte_size == len(JPEG)
    assert store.files[scene.storage_key] == JPEG
    assert scene.source_url == f"https://gibs.example/{TODAY}"
    assert scene.retrieved_at == NOW

    # Already acquired scenes are not requested again.
    assert job.run_once().startswith("acquired 0 scenes")
    assert len(source.requests) == 2


def test_producer_retries_empty_and_failed_scenes_later_and_respects_the_budget() -> None:
    days = [TODAY - timedelta(days=offset) for offset in range(3)]
    source = ScriptedSource({days[0]: BLANK_JPEG, days[1]: RuntimeError("HTTP 500"), days[2]: JPEG})
    published, store = RecordingPublisher(), MemoryStore()
    job = producer(source, [incident(days=days)], published, store, max_scenes=2)

    assert job.run_once() == (
        "acquired 0 scenes, 1 not available yet, 1 failed, 1 deferred to the next run"
    )
    assert store.files == {}
    # The empty and failed scenes wait, so the deferred one gets its turn.
    assert job.run_once().startswith("acquired 1 scenes")
    assert [event.acquisition_date for event in published.events] == [days[2]]


def test_producer_run_fails_when_every_request_fails() -> None:
    source = ScriptedSource({TODAY: RuntimeError("GIBS down")})
    job = producer(source, [incident(days=[TODAY])], RecordingPublisher(), MemoryStore())
    with pytest.raises(RuntimeError, match="all 1 imagery requests failed"):
        _ = job.run_once()


def test_producer_lets_rate_limits_end_the_run() -> None:
    source = ScriptedSource({TODAY: RetryLater("slow down", 30)})
    job = producer(source, [incident(days=[TODAY])], RecordingPublisher(), MemoryStore())
    with pytest.raises(RetryLater):
        _ = job.run_once()
