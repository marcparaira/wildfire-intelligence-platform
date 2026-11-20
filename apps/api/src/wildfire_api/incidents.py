"""Fire incidents from the PostGIS projection of ``fire.incidents``.

The stream processor clusters detections into incidents and publishes a revision on
every change; the sink worker keeps the latest revision per incident in
``fire_incidents``. Weather context comes from the ``weather_hourly`` projection of
the incident's resolution-2 H3 cell, which is also its weather-grid cell.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, LiteralString
from uuid import UUID

import psycopg
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from wildfire_api.bounds import Bounds, BoundsDep
from wildfire_api.conditions import RiskSummary
from wildfire_api.db import connect, fetch_all
from wildfire_api.settings import SettingsDep
from wildfire_events.models import FirmsProduct, IncidentStatus, RiskLevel

LOGGER = logging.getLogger(__name__)
router = APIRouter(tags=["incidents"])

DEFAULT_LIMIT = 500
MAX_LIMIT = 2000
ACTIVE_STATUSES = (IncidentStatus.NEW, IncidentStatus.GROWING, IncidentStatus.STABLE)
# Peak risk looks at detections from the incident's last day of activity...
PEAK_RISK_WINDOW = timedelta(hours=24)
# ...inside its bounding box grown by this margin, since detections are not linked to
# incidents and a box edge passes through the outermost detections.
PEAK_RISK_MARGIN_DEGREES = 0.01
WEATHER_CONTEXT = timedelta(hours=24)


class IncidentSummary(BaseModel):
    incident_id: UUID
    revision: int
    status: IncidentStatus
    previous_status: IncidentStatus | None
    merged_into: UUID | None
    first_observed_at: datetime
    last_observed_at: datetime
    updated_at: datetime
    detection_count: int
    footprint_cells: int
    footprint_km2: float
    latitude: float
    longitude: float
    bbox: tuple[float, float, float, float]  # west, south, east, north
    max_frp_mw: float | None
    total_frp_mw: float
    products: list[FirmsProduct]
    peak_risk: RiskSummary | None


class IncidentResults(BaseModel):
    items: list[IncidentSummary]
    truncated: bool


class IncidentDay(BaseModel):
    """One UTC day of activity, with the footprint accumulated up to and including it."""

    day: date
    detections: int
    new_cells: int
    max_frp_mw: float | None
    footprint_cells: int
    footprint_km2: float


class WeatherHour(BaseModel):
    valid_at: datetime
    temperature_c: float | None
    precipitation_mm: float | None
    wind_speed_kmh: float | None
    wind_direction_degrees: float | None


class IncidentWeather(BaseModel):
    cell: str
    generated_at: datetime
    hours: list[WeatherHour]


class IncidentDetail(IncidentSummary):
    daily: list[IncidentDay]
    weather: IncidentWeather | None


class IncidentNotFound(LookupError):
    """No incident with this ID has been stored."""


class RecordedDay(BaseModel):
    """An entry of ``FireIncident.daily`` as stored in the ``daily`` JSON column."""

    day: date
    detections: int
    new_cells: int
    max_frp_mw: float | None = None


def growth(
    days: Sequence[RecordedDay], footprint_cells: int, footprint_km2: float
) -> list[IncidentDay]:
    """Daily activity, oldest first, with the cumulative footprint after each day.

    The area per cell is the incident's own mean (``footprint_km2 / footprint_cells``),
    so the last day's cumulative area matches the incident's footprint when every
    footprint cell was counted as new on some day.
    """
    km2_per_cell = footprint_km2 / footprint_cells if footprint_cells else 0.0
    cumulative = 0
    result: list[IncidentDay] = []
    for recorded in sorted(days, key=lambda item: item.day):
        cumulative += recorded.new_cells
        result.append(
            IncidentDay(
                day=recorded.day,
                detections=recorded.detections,
                new_cells=recorded.new_cells,
                max_frp_mw=recorded.max_frp_mw,
                footprint_cells=cumulative,
                footprint_km2=cumulative * km2_per_cell,
            )
        )
    return result


def weather_window(now: datetime) -> tuple[datetime, datetime]:
    """From the start of the current UTC hour through ``WEATHER_CONTEXT`` later (inclusive)."""
    start = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    return start, start + WEATHER_CONTEXT


@dataclass(frozen=True, slots=True)
class IncidentFilter:
    bounds: Bounds | None = None
    statuses: tuple[IncidentStatus, ...] = ACTIVE_STATUSES
    limit: int = DEFAULT_LIMIT


class _IncidentRow(BaseModel):
    incident_id: UUID
    revision: int
    status: IncidentStatus
    previous_status: IncidentStatus | None
    merged_into: UUID | None
    h3_cell: str
    first_observed_at: datetime
    last_observed_at: datetime
    updated_at: datetime
    detection_count: int
    footprint_cells: int
    footprint_km2: float
    latitude: float
    longitude: float
    west: float
    south: float
    east: float
    north: float
    max_frp_mw: float | None
    total_frp_mw: float
    products: list[FirmsProduct]
    daily: list[RecordedDay]
    peak_risk_score: int | None
    peak_risk_level: RiskLevel | None

    def summary(self) -> IncidentSummary:
        return IncidentSummary(
            incident_id=self.incident_id,
            revision=self.revision,
            status=self.status,
            previous_status=self.previous_status,
            merged_into=self.merged_into,
            first_observed_at=self.first_observed_at,
            last_observed_at=self.last_observed_at,
            updated_at=self.updated_at,
            detection_count=self.detection_count,
            footprint_cells=self.footprint_cells,
            footprint_km2=self.footprint_km2,
            latitude=self.latitude,
            longitude=self.longitude,
            bbox=(self.west, self.south, self.east, self.north),
            max_frp_mw=self.max_frp_mw,
            total_frp_mw=self.total_frp_mw,
            products=self.products,
            peak_risk=None
            if self.peak_risk_score is None or self.peak_risk_level is None
            else RiskSummary(score=self.peak_risk_score, level=self.peak_risk_level),
        )


class _WeatherCell(BaseModel):
    cell: str
    generated_at: datetime


# The incident row plus its peak risk; callers append WHERE/ORDER/LIMIT. Parameters:
# the peak-risk window (interval), then the bounding-box margin (degrees) four times.
_INCIDENTS: LiteralString = """
    SELECT i.incident_id, i.revision, i.status, i.previous_status, i.merged_into, i.h3_cell,
           i.first_observed_at, i.last_observed_at, i.updated_at, i.detection_count,
           i.footprint_cells, i.footprint_km2, i.latitude, i.longitude, i.west, i.south,
           i.east, i.north, i.max_frp_mw, i.total_frp_mw, i.products, i.daily,
           peak.risk_score AS peak_risk_score, peak.risk_level AS peak_risk_level
    FROM fire_incidents AS i
    LEFT JOIN LATERAL (
        SELECT c.risk_score, c.risk_level
        FROM detection_conditions AS c JOIN fire_detections AS d USING (event_id)
        WHERE c.h3_cell = i.h3_cell
          AND c.risk_score IS NOT NULL
          AND c.observed_at BETWEEN i.last_observed_at - %s AND i.last_observed_at
          AND ST_Intersects(
              d.location,
              ST_MakeEnvelope(i.west - %s, i.south - %s, i.east + %s, i.north + %s, 4326)
          )
        ORDER BY c.risk_score DESC
        LIMIT 1
    ) AS peak ON true
"""


def _peak_risk_parameters() -> list[object]:
    return [PEAK_RISK_WINDOW, *([PEAK_RISK_MARGIN_DEGREES] * 4)]


def find_incidents(connection: psycopg.Connection, query: IncidentFilter) -> IncidentResults:
    """Incidents in the given statuses (and bounds), most recently active first."""
    clauses: list[LiteralString] = ["i.status = ANY(%s)"]
    parameters: list[object] = [*_peak_risk_parameters(), [str(item) for item in query.statuses]]
    if query.bounds is not None:
        clauses.append("ST_Intersects(i.location, ST_MakeEnvelope(%s, %s, %s, %s, 4326))")
        parameters.extend(
            (query.bounds.west, query.bounds.south, query.bounds.east, query.bounds.north)
        )
    # One extra row tells whether the result was truncated.
    parameters.append(query.limit + 1)
    rows = fetch_all(
        connection,
        _IncidentRow,
        f"""{_INCIDENTS}
        WHERE {" AND ".join(clauses)}
        ORDER BY i.last_observed_at DESC, i.incident_id
        LIMIT %s
        """,
        parameters,
    )
    return IncidentResults(
        items=[row.summary() for row in rows[: query.limit]], truncated=len(rows) > query.limit
    )


def load_weather(
    connection: psycopg.Connection, cell: str, now: datetime
) -> IncidentWeather | None:
    """The cell's forecast over the weather window, or None if the cell has no forecast."""
    cells = fetch_all(
        connection,
        _WeatherCell,
        "SELECT cell, generated_at FROM weather_cells WHERE cell = %s",
        [cell],
    )
    if not cells:
        return None
    start, end = weather_window(now)
    hours = fetch_all(
        connection,
        WeatherHour,
        """
        SELECT valid_at, temperature_c, precipitation_mm, wind_speed_kmh, wind_direction_degrees
        FROM weather_hourly
        WHERE cell = %s AND valid_at BETWEEN %s AND %s
        ORDER BY valid_at
        """,
        [cell, start, end],
    )
    return IncidentWeather(cell=cells[0].cell, generated_at=cells[0].generated_at, hours=hours)


def load_incident(
    connection: psycopg.Connection, incident_id: UUID, now: datetime
) -> IncidentDetail:
    """One incident in any status; raises ``IncidentNotFound``."""
    rows = fetch_all(
        connection,
        _IncidentRow,
        f"{_INCIDENTS} WHERE i.incident_id = %s",
        [*_peak_risk_parameters(), incident_id],
    )
    if not rows:
        raise IncidentNotFound(incident_id)
    row = rows[0]
    return IncidentDetail.model_validate(
        {
            **row.summary().model_dump(),
            "daily": growth(row.daily, row.footprint_cells, row.footprint_km2),
            "weather": load_weather(connection, row.h3_cell, now),
        }
    )


def _unavailable() -> HTTPException:
    return HTTPException(
        status.HTTP_503_SERVICE_UNAVAILABLE, "Incident data is currently unavailable"
    )


@router.get("/incidents", response_model=IncidentResults)
def incidents(
    *,
    settings: SettingsDep,
    bounds: BoundsDep,
    status_: Annotated[list[IncidentStatus] | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
) -> IncidentResults:
    """Fire incidents, most recently active first.

    Only active incidents (new, growing, stable) unless ``status`` is given (repeatable).
    With bounds, an incident is included when its mean position is inside them.
    ``peak_risk`` is the highest risk among detections from the incident's last
    24 hours of activity within its bounding box: an approximation, because
    detections are not linked to incidents.
    """
    query = IncidentFilter(
        bounds=bounds, statuses=tuple(dict.fromkeys(status_ or ACTIVE_STATUSES)), limit=limit
    )
    try:
        with connect(settings.database_url) as connection:
            return find_incidents(connection, query)
    except psycopg.Error as exc:
        LOGGER.exception("Unable to query fire incidents")
        raise _unavailable() from exc


@router.get("/incidents/{incident_id}", response_model=IncidentDetail)
def incident(settings: SettingsDep, incident_id: UUID) -> IncidentDetail:
    """One incident with its daily growth and the next 24 hours of weather for its cell.

    ``daily`` is oldest first, with the cumulative footprint after each day.
    ``weather`` covers the current UTC hour through 24 hours later from the latest
    stored forecast for the incident's H3 cell, and is null when that cell has none.
    """
    try:
        with connect(settings.database_url) as connection:
            return load_incident(connection, incident_id, datetime.now(UTC))
    except IncidentNotFound as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Incident not found") from exc
    except psycopg.Error as exc:
        LOGGER.exception("Unable to query fire incidents")
        raise _unavailable() from exc
