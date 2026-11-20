from datetime import UTC, datetime, timedelta
from typing import cast

import h3

from wildfire_events.cells import partition_cell
from wildfire_events.models import (
    FireIncident,
    FirmsDetection,
    FirmsProduct,
    IncidentStatus,
    Instrument,
    detection_event_id,
)
from wildfire_processor.incidents import CELL_RESOLUTION, IncidentTracker, incident_id

T0 = datetime(2026, 3, 5, 6, 0, tzinfo=UTC)
CENTER = h3.latlng_to_cell(1.0, 114.0, CELL_RESOLUTION)
RING = sorted(set(h3.grid_disk(CENTER, 1)) - {CENTER})
FAR = h3.latlng_to_cell(1.3, 114.3, CELL_RESOLUTION)  # ~45 km away, same resolution-2 cell


class DictState:
    def __init__(self) -> None:
        self.data: dict[str, object] = {}

    def get(self, key: str, default: object = None) -> object:
        return self.data.get(key, default)

    def set(self, key: str, value: object) -> None:
        self.data[key] = value

    def delete(self, key: str) -> None:
        _ = self.data.pop(key, None)


def detection(
    cell: str,
    hours: float,
    *,
    satellite: str = "N20",
    frp: float | None = 5.0,
    ingested_hours: float | None = None,
) -> FirmsDetection:
    latitude, longitude = h3.cell_to_latlng(cell)
    observed = T0 + timedelta(hours=hours)
    ingested = observed + timedelta(hours=3 if ingested_hours is None else ingested_hours)
    assert partition_cell(latitude, longitude) == partition_cell(1.0, 114.0)
    return FirmsDetection(
        event_id=detection_event_id(
            FirmsProduct.VIIRS_NOAA20_NRT,
            satellite,
            observed,
            f"{latitude:.6f}",
            f"{longitude:.6f}",
        ),
        product=FirmsProduct.VIIRS_NOAA20_NRT,
        instrument=Instrument.VIIRS,
        satellite=satellite,
        observed_at=observed,
        latitude=latitude,
        longitude=longitude,
        h3_cell=partition_cell(latitude, longitude),
        frp_mw=frp,
        attributes={},
        ingested_at=ingested,
    )


def run(tracker: IncidentTracker, *detections: FirmsDetection) -> list[FireIncident]:
    return [incident for item in detections for incident in tracker.process(item)]


def latest(outputs: list[FireIncident]) -> dict[str, FireIncident]:
    return {str(item.incident_id): item for item in outputs}


def test_first_detection_starts_a_new_incident() -> None:
    first = detection(CENTER, 0)
    [incident] = IncidentTracker(DictState()).process(first)

    assert incident.incident_id == incident_id(first.event_id)
    assert (incident.revision, incident.status, incident.previous_status) == (
        1,
        IncidentStatus.NEW,
        None,
    )
    assert (incident.detection_count, incident.footprint_cells) == (1, 1)
    assert incident.footprint_km2 > 5
    assert incident.h3_cell == first.h3_cell
    assert incident.daily[0].day.isoformat() == "2026-03-05"
    assert incident.daily[0].new_cells == 1


def test_adjacent_detections_join_and_distant_ones_do_not() -> None:
    tracker = IncidentTracker(DictState())
    outputs = run(
        tracker,
        detection(CENTER, 0),
        detection(RING[0], 0.1, frp=20.0),
        detection(CENTER, 0.2, satellite="NPP", frp=None),
        detection(FAR, 0.3),
    )
    incidents = latest(outputs)

    assert len(incidents) == 2
    main = incidents[str(outputs[0].incident_id)]
    assert (main.detection_count, main.footprint_cells, main.revision) == (3, 2, 3)
    assert (main.max_frp_mw, main.total_frp_mw) == (20.0, 25.0)
    assert main.west <= main.longitude <= main.east
    far = outputs[-1]
    assert far.incident_id != main.incident_id
    assert far.detection_count == 1


def test_detections_outside_the_link_window_start_a_new_incident() -> None:
    tracker = IncidentTracker(DictState())
    first = tracker.process(detection(CENTER, 0))[0]
    outputs = tracker.process(detection(CENTER, 60))  # more than 48 h later

    by_id = latest(outputs)
    assert by_id[str(first.incident_id)].status == IncidentStatus.INACTIVE
    assert by_id[str(first.incident_id)].previous_status == IncidentStatus.NEW
    assert len(by_id) == 2


def test_lifecycle_new_growing_stable_inactive_and_state_cleanup() -> None:
    state = DictState()
    tracker = IncidentTracker(state)
    [created] = tracker.process(detection(CENTER, 0))
    key = str(created.incident_id)

    # Another detection in the same cell during the first day: still new.
    assert tracker.process(detection(CENTER, 12))[0].status == IncidentStatus.NEW
    # A new cell 30 h after the first detection: growing.
    grown = latest(tracker.process(detection(RING[0], 30)))[key]
    assert (grown.status, grown.previous_status) == (IncidentStatus.GROWING, IncidentStatus.NEW)
    assert [day.new_cells for day in grown.daily] == [1, 1]

    # Time advances through detections elsewhere in the partition.
    stable = latest(tracker.process(detection(FAR, 55)))
    assert (stable[key].status, stable[key].previous_status) == (
        IncidentStatus.STABLE,
        IncidentStatus.GROWING,
    )
    assert stable[key].revision == grown.revision + 1

    inactive = latest(tracker.process(detection(FAR, 79)))
    assert inactive[key].status == IncidentStatus.INACTIVE
    assert inactive[key].last_observed_at == T0 + timedelta(hours=30)
    # The inactive incident's state is gone; only the far incident remains.
    assert f"inc:{key}" not in state.data
    assert not any(k.startswith("cell:") and k.endswith(CENTER) for k in state.data)
    timers = cast(dict[str, int], state.data["timers"])
    assert list(timers) == [outputs_far_id(stable, key)]


def outputs_far_id(outputs: dict[str, FireIncident], excluded: str) -> str:
    [other] = [item for item in outputs if item != excluded]
    return other


def test_a_bridging_detection_merges_incidents_into_the_oldest() -> None:
    center_neighbours = RING
    west = center_neighbours[0]
    east = max(center_neighbours, key=lambda cell: h3.grid_distance(west, cell))
    assert h3.grid_distance(west, east) == 2

    tracker = IncidentTracker(DictState())
    older = tracker.process(detection(west, 0))[0]
    newer = tracker.process(detection(east, 1))[0]
    assert older.incident_id != newer.incident_id

    outputs = latest(tracker.process(detection(CENTER, 2)))
    merged = outputs[str(newer.incident_id)]
    survivor = outputs[str(older.incident_id)]
    assert (merged.status, merged.merged_into, merged.previous_status) == (
        IncidentStatus.MERGED,
        older.incident_id,
        IncidentStatus.NEW,
    )
    assert (survivor.detection_count, survivor.footprint_cells) == (3, 3)

    # Cells of the absorbed incident now link to the survivor.
    later = tracker.process(detection(east, 3))
    assert [item.incident_id for item in later] == [older.incident_id]


def test_late_detection_for_an_inactive_incident_starts_a_new_one() -> None:
    tracker = IncidentTracker(DictState())
    first = tracker.process(detection(CENTER, 0))[0]
    _ = tracker.process(detection(FAR, 50))  # first incident becomes inactive
    straggler = detection(CENTER, 10)  # observed before, arrives after
    [late] = tracker.process(straggler)
    assert late.incident_id == incident_id(straggler.event_id) != first.incident_id
    # Judged at the stream clock (hour 50): past the 24 h new period, not growing.
    assert late.status == IncidentStatus.STABLE


def test_clock_is_capped_at_ingestion_time() -> None:
    tracker = IncidentTracker(DictState())
    first = tracker.process(detection(CENTER, 0))[0]
    # Claims to be observed 100 h later but was ingested at hour 1.
    outputs = latest(tracker.process(detection(FAR, 100, ingested_hours=-99)))
    assert str(first.incident_id) not in outputs
    assert outputs[next(iter(outputs))].updated_at == T0 + timedelta(hours=1)


def test_replay_reproduces_identical_revisions() -> None:
    sequence = [
        detection(CENTER, 0),
        detection(RING[1], 0.5),
        detection(FAR, 1),
        detection(RING[2], 30),
        detection(FAR, 60),
        detection(FAR, 90),
    ]
    first = run(IncidentTracker(DictState()), *sequence)
    second = run(IncidentTracker(DictState()), *sequence)
    assert [item.model_dump() for item in first] == [item.model_dump() for item in second]


def test_stored_state_keeps_its_json_shape() -> None:
    """The Quix changelog persists state: statuses and products stay plain strings."""
    state = DictState()
    [created] = IncidentTracker(state).process(detection(CENTER, 0))
    stored = cast(dict[str, object], state.data[f"inc:{created.incident_id}"])
    assert stored["status"] == "new"
    assert type(stored["status"]) is str
    assert stored["products"] == ["VIIRS_NOAA20_NRT"]
    assert state.data[f"cell:{CENTER}"] == [str(created.incident_id), *[_ms(0)] * 2]


def _ms(hours: float) -> int:
    return int((T0 + timedelta(hours=hours)).timestamp() * 1000)
