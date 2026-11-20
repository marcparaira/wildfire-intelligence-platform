"""Cluster FIRMS detections into fire incidents and track their lifecycle.

Clustering
    Each detection is placed in a resolution-7 H3 cell (about 5 km², 1.4 km edge).
    It joins the incident that owns its cell or one of the six neighbouring cells,
    if that cell was detected within ``LINK_WINDOW`` of the detection. With no
    match it starts a new incident; when it links two or more incidents, the
    oldest absorbs the others, which end as ``merged``. Incidents never cross a
    resolution-2 cell (the Kafka key), so a fire on such a boundary becomes two
    incidents.

Lifecycle, evaluated against the stream clock
    ``new``       first observed less than ``NEW_PERIOD`` ago
    ``growing``   gained footprint cells in the last ``GROWTH_WINDOW``
    ``stable``    still detected, but no longer gaining cells
    ``inactive``  no detection for ``INACTIVE_AFTER`` (final)
    ``merged``    absorbed by another incident (final)

The stream clock is the newest observation time the tracker has seen (capped at
each detection's ingestion time, so a bad timestamp cannot jump it forward).
Status changes that only depend on time passing are applied when a later
detection moves the clock, so results depend on event time alone and a replay
of the same detections reproduces the same incidents and revisions.

A late detection whose incident has already become inactive starts a new
incident. Cross-sensor observations of the same fire each count as detections.
"""

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid5

import h3
from pydantic import BaseModel

from wildfire_events.models import (
    FireIncident,
    FirmsDetection,
    FirmsProduct,
    IncidentDay,
    IncidentStatus,
)
from wildfire_processor.state import KeyValueState

CELL_RESOLUTION = 7
CELL_AREA_KM2 = h3.average_hexagon_area(CELL_RESOLUTION, "km^2")
# A detection links to incidents in its own cell and the ring of cells around it.
NEIGHBOUR_RING = 1
LINK_WINDOW = timedelta(hours=48)
NEW_PERIOD = timedelta(hours=24)
GROWTH_WINDOW = timedelta(hours=24)
INACTIVE_AFTER = timedelta(hours=48)
FOOTPRINT_DECIMALS = 2
FRP_DECIMALS = 3

_INCIDENT_NAMESPACE = UUID("1b6f0f8e-5a0c-4d4f-9f3e-8a1f5f0c2d71")
_MS = 1000

# State store keys; their layout is persisted in the Quix changelog, so keep it stable.
_CLOCK_KEY = "clock"
_TIMERS_KEY = "timers"


def _incident_key(incident: UUID) -> str:
    return f"inc:{incident}"


def _incident_cells_key(incident: UUID) -> str:
    return f"cells:{incident}"


def _cell_key(cell: str) -> str:
    return f"cell:{cell}"


def _ms(value: timedelta | datetime) -> int:
    if isinstance(value, timedelta):
        return int(value.total_seconds() * _MS)
    return int(value.timestamp() * _MS)


def _datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / _MS, UTC)


def incident_id(first_event_id: UUID) -> UUID:
    """Deterministic incident ID derived from the detection that started it."""
    return uuid5(_INCIDENT_NAMESPACE, str(first_event_id))


def _max_frp(a: float | None, b: float | None) -> float | None:
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


class _Day(BaseModel):
    detections: int = 0
    new_cells: int = 0
    max_frp_mw: float | None = None

    def absorb(self, other: "_Day") -> None:
        self.detections += other.detections
        self.new_cells += other.new_cells
        self.max_frp_mw = _max_frp(self.max_frp_mw, other.max_frp_mw)


class _Incident(BaseModel):
    """Stored incident state; JSON-serializable for the state store."""

    id: UUID
    revision: int = 0
    status: IncidentStatus = IncidentStatus.NEW
    h3_cell: str
    first_ms: int
    last_ms: int
    last_growth_ms: int | None = None
    detection_count: int = 0
    footprint_cells: int = 0
    latitude_sum: float = 0.0
    longitude_sum: float = 0.0
    west: float
    south: float
    east: float
    north: float
    max_frp_mw: float | None = None
    total_frp_mw: float = 0.0
    products: list[FirmsProduct] = []
    daily: dict[str, _Day] = {}

    @classmethod
    def start(cls, detection: FirmsDetection, at: int) -> "_Incident":
        return cls(
            id=incident_id(detection.event_id),
            h3_cell=detection.h3_cell,
            first_ms=at,
            last_ms=at,
            west=detection.longitude,
            south=detection.latitude,
            east=detection.longitude,
            north=detection.latitude,
        )

    def day(self, detection: FirmsDetection) -> _Day:
        return self.daily.setdefault(detection.observed_at.date().isoformat(), _Day())

    def add_detection(self, detection: FirmsDetection, at: int) -> None:
        """Accumulate one detection's totals (footprint is handled by the tracker)."""
        self.first_ms = min(self.first_ms, at)
        self.last_ms = max(self.last_ms, at)
        self.detection_count += 1
        self.latitude_sum += detection.latitude
        self.longitude_sum += detection.longitude
        self.west = min(self.west, detection.longitude)
        self.south = min(self.south, detection.latitude)
        self.east = max(self.east, detection.longitude)
        self.north = max(self.north, detection.latitude)
        self.max_frp_mw = _max_frp(self.max_frp_mw, detection.frp_mw)
        self.total_frp_mw += detection.frp_mw or 0.0
        self.products = sorted({*self.products, detection.product})
        day = self.day(detection)
        day.detections += 1
        day.max_frp_mw = _max_frp(day.max_frp_mw, detection.frp_mw)
        self.daily = dict(sorted(self.daily.items()))

    def absorb(self, other: "_Incident") -> None:
        """Add another incident's totals and history to this one."""
        self.first_ms = min(self.first_ms, other.first_ms)
        self.last_ms = max(self.last_ms, other.last_ms)
        if other.last_growth_ms is not None:
            self.last_growth_ms = max(self.last_growth_ms or 0, other.last_growth_ms)
        self.detection_count += other.detection_count
        self.footprint_cells += other.footprint_cells
        self.latitude_sum += other.latitude_sum
        self.longitude_sum += other.longitude_sum
        self.west = min(self.west, other.west)
        self.south = min(self.south, other.south)
        self.east = max(self.east, other.east)
        self.north = max(self.north, other.north)
        self.max_frp_mw = _max_frp(self.max_frp_mw, other.max_frp_mw)
        self.total_frp_mw += other.total_frp_mw
        self.products = sorted({*self.products, *other.products})
        for day, values in other.daily.items():
            self.daily.setdefault(day, _Day()).absorb(values)
        self.daily = dict(sorted(self.daily.items()))

    def status_at(self, clock: int) -> IncidentStatus:
        if clock - self.last_ms > _ms(INACTIVE_AFTER):
            return IncidentStatus.INACTIVE
        if clock - self.first_ms <= _ms(NEW_PERIOD):
            return IncidentStatus.NEW
        if self.last_growth_ms is not None and clock - self.last_growth_ms <= _ms(GROWTH_WINDOW):
            return IncidentStatus.GROWING
        return IncidentStatus.STABLE

    def next_change(self, clock: int) -> int:
        """The earliest clock value at which the status can change without new data.

        Each bound is exclusive (``>`` in ``status_at``), hence the extra millisecond.
        """
        candidates = [self.last_ms + _ms(INACTIVE_AFTER) + 1]
        if self.status is IncidentStatus.NEW:
            candidates.append(self.first_ms + _ms(NEW_PERIOD) + 1)
        if self.status is IncidentStatus.GROWING and self.last_growth_ms is not None:
            candidates.append(self.last_growth_ms + _ms(GROWTH_WINDOW) + 1)
        return min(candidate for candidate in candidates if candidate > clock)

    def revision_at(
        self, clock: int, previous: IncidentStatus | None, merged_into: UUID | None
    ) -> FireIncident:
        return FireIncident(
            incident_id=self.id,
            revision=self.revision,
            status=self.status,
            previous_status=previous if previous != self.status else None,
            merged_into=merged_into,
            h3_cell=self.h3_cell,
            first_observed_at=_datetime(self.first_ms),
            last_observed_at=_datetime(self.last_ms),
            updated_at=_datetime(clock),
            detection_count=self.detection_count,
            footprint_cells=self.footprint_cells,
            footprint_km2=round(self.footprint_cells * CELL_AREA_KM2, FOOTPRINT_DECIMALS),
            latitude=self.latitude_sum / self.detection_count,
            longitude=self.longitude_sum / self.detection_count,
            west=self.west,
            south=self.south,
            east=self.east,
            north=self.north,
            max_frp_mw=self.max_frp_mw,
            total_frp_mw=round(self.total_frp_mw, FRP_DECIMALS),
            products=self.products,
            daily=[
                IncidentDay(
                    day=datetime.fromisoformat(day).date(),
                    detections=values.detections,
                    new_cells=values.new_cells,
                    max_frp_mw=values.max_frp_mw,
                )
                for day, values in self.daily.items()
            ],
        )


@dataclass(slots=True)
class _Cell:
    """Ownership of one resolution-7 cell; stored as ``[owner, first_ms, last_ms]``."""

    owner: UUID
    first_ms: int
    last_ms: int

    @classmethod
    def load(cls, raw: object) -> "_Cell | None":
        if raw is None:
            return None
        owner, first_ms, last_ms = cast(list[object], raw)
        return cls(UUID(str(owner)), cast(int, first_ms), cast(int, last_ms))

    def dump(self) -> list[object]:
        return [str(self.owner), self.first_ms, self.last_ms]

    def touch(self, at: int) -> None:
        self.first_ms = min(self.first_ms, at)
        self.last_ms = max(self.last_ms, at)

    def links(self, at: int) -> bool:
        """Whether a detection at ``at`` is within ``LINK_WINDOW`` of this cell's activity."""
        return self.first_ms - _ms(LINK_WINDOW) <= at <= self.last_ms + _ms(LINK_WINDOW)


@dataclass(slots=True)
class _Change:
    incident: _Incident
    previous: IncidentStatus | None
    merged_into: UUID | None


@dataclass(slots=True)
class IncidentTracker:
    """Applies detections to one state (one Kafka key) and returns changed incidents.

    State keys: ``clock`` (ms), ``timers`` (active incident ID -> next ms at which
    its status may change), ``inc:<id>`` (incident), ``cells:<id>`` (its cells),
    and ``cell:<h3>`` (owner, first and last detection ms).
    """

    state: KeyValueState
    _changed: dict[UUID, _Change] = field(default_factory=dict)

    # --- entry point ----------------------------------------------------------------

    def process(self, detection: FirmsDetection) -> list[FireIncident]:
        """Apply one detection; return a new revision of every incident it changed."""
        self._changed = {}
        at = _ms(detection.observed_at)
        clock = self._advance_clock(detection, at)
        timers = self._timers()

        cell = h3.latlng_to_cell(detection.latitude, detection.longitude, CELL_RESOLUTION)
        incident = self._incident_for(detection, cell, at, timers)
        self._claim(incident, detection, cell, at)
        incident.add_detection(detection, at)
        self._mark(incident, incident.status)
        self._refresh(incident, clock, timers)
        self._apply_due(clock, timers)
        self.state.set(_TIMERS_KEY, timers)
        return list(self._publish(clock))

    def _advance_clock(self, detection: FirmsDetection, at: int) -> int:
        """Move the stream clock; ingestion time caps it so a bad timestamp cannot jump it."""
        clock = max(
            cast(int, self.state.get(_CLOCK_KEY, 0)),
            min(at, _ms(detection.ingested_at)),
        )
        self.state.set(_CLOCK_KEY, clock)
        return clock

    def _apply_due(self, clock: int, timers: dict[str, int]) -> None:
        """Apply status changes that are due now that the clock has moved."""
        due = sorted((due_at, key) for key, due_at in timers.items() if due_at <= clock)
        for _, key in due:
            incident = self._incident(UUID(key))
            if incident is None:
                _ = timers.pop(key, None)
                continue
            self._refresh(incident, clock, timers)

    def _publish(self, clock: int) -> Iterable[FireIncident]:
        for change in self._changed.values():
            incident = change.incident
            incident.revision += 1
            if not incident.status.is_final:
                self._save(incident)
            yield incident.revision_at(clock, change.previous, change.merged_into)

    # --- state access ---------------------------------------------------------------

    def _incident(self, incident: UUID) -> _Incident | None:
        raw = self.state.get(_incident_key(incident))
        return None if raw is None else _Incident.model_validate(raw)

    def _save(self, incident: _Incident) -> None:
        self.state.set(_incident_key(incident.id), incident.model_dump(mode="json"))

    def _cells(self, incident: UUID) -> list[str]:
        return cast(list[str], self.state.get(_incident_cells_key(incident), []))

    def _cell(self, cell: str) -> _Cell | None:
        return _Cell.load(self.state.get(_cell_key(cell)))

    def _timers(self) -> dict[str, int]:
        return cast(dict[str, int], self.state.get(_TIMERS_KEY, {}))

    def _forget(self, incident: UUID, timers: dict[str, int]) -> None:
        self.state.delete(_incident_cells_key(incident))
        self.state.delete(_incident_key(incident))
        _ = timers.pop(str(incident), None)

    def _mark(
        self,
        incident: _Incident,
        previous: IncidentStatus | None,
        merged_into: UUID | None = None,
    ) -> None:
        """Record that ``incident`` changed; keep the earliest previous status."""
        earlier = self._changed.get(incident.id)
        self._changed[incident.id] = _Change(
            incident, earlier.previous if earlier else previous, merged_into
        )

    # --- lifecycle ------------------------------------------------------------------

    def _refresh(self, incident: _Incident, clock: int, timers: dict[str, int]) -> None:
        """Re-evaluate the status, then reschedule or retire the incident."""
        status = incident.status_at(clock)
        if status != incident.status:
            self._mark(incident, incident.status)
            incident.status = status
        if status is IncidentStatus.INACTIVE:
            self._retire(incident, timers)
            return
        timers[str(incident.id)] = incident.next_change(clock)
        self._save(incident)

    def _retire(self, incident: _Incident, timers: dict[str, int]) -> None:
        """Drop a final incident from state; ``fire.incidents`` keeps its last revision."""
        for cell in self._cells(incident.id):
            owner = self._cell(cell)
            if owner is not None and owner.owner == incident.id:
                self.state.delete(_cell_key(cell))
        self._forget(incident.id, timers)

    # --- clustering -----------------------------------------------------------------

    def _incident_for(
        self, detection: FirmsDetection, cell: str, at: int, timers: dict[str, int]
    ) -> _Incident:
        """The incident a detection joins: the oldest linked one, absorbing the others."""
        linked = self._linked(detection, cell, at)
        if not linked:
            incident = _Incident.start(detection, at)
            self._mark(incident, None)
            return incident
        survivor, *absorbed = linked
        for other in absorbed:
            self._merge(survivor, other, timers)
        return survivor

    def _linked(self, detection: FirmsDetection, cell: str, at: int) -> list[_Incident]:
        found: dict[UUID, _Incident] = {}
        for neighbour in sorted(h3.grid_disk(cell, NEIGHBOUR_RING)):
            record = self._cell(neighbour)
            if record is None or record.owner in found or not record.links(at):
                continue
            incident = self._incident(record.owner)
            if incident is not None and incident.h3_cell == detection.h3_cell:
                found[incident.id] = incident
        return sorted(found.values(), key=lambda item: (item.first_ms, str(item.id)))

    def _merge(self, survivor: _Incident, absorbed: _Incident, timers: dict[str, int]) -> None:
        cells = self._cells(absorbed.id)
        for cell in cells:
            record = self._cell(cell)
            if record is not None and record.owner == absorbed.id:
                record.owner = survivor.id
                self.state.set(_cell_key(cell), record.dump())
        self.state.set(_incident_cells_key(survivor.id), [*self._cells(survivor.id), *cells])
        survivor.absorb(absorbed)
        self._mark(absorbed, absorbed.status, merged_into=survivor.id)
        absorbed.status = IncidentStatus.MERGED
        self._forget(absorbed.id, timers)
        self._mark(survivor, survivor.status)

    def _claim(self, incident: _Incident, detection: FirmsDetection, cell: str, at: int) -> None:
        """Record the detection's cell for ``incident``, growing its footprint if new to it."""
        record = self._cell(cell)
        if record is None or record.owner != incident.id:
            # A cell previously owned by an unlinked incident moves to this one.
            record = _Cell(incident.id, at, at)
            incident.footprint_cells += 1
            incident.day(detection).new_cells += 1
            self.state.set(_incident_cells_key(incident.id), [*self._cells(incident.id), cell])
            if incident.detection_count and at > incident.first_ms + _ms(NEW_PERIOD):
                incident.last_growth_ms = max(incident.last_growth_ms or at, at)
        record.touch(at)
        self.state.set(_cell_key(cell), record.dump())
