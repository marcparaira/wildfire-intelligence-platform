"""Fire detections from the PostGIS projection of ``firms.detections``.

Each detection carries its country, risk, and weather at observation time from the
``detection_conditions`` projection; they are null until the detection is enriched.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Annotated, LiteralString
from uuid import UUID

import psycopg
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from wildfire_api.bounds import Bounds, BoundsDep
from wildfire_api.conditions import (
    CONDITION_COLUMNS,
    ConditionColumns,
    DetectionRisk,
    DetectionWeather,
)
from wildfire_api.db import connect, fetch_all
from wildfire_api.settings import SettingsDep
from wildfire_events.models import FirmsProduct

LOGGER = logging.getLogger(__name__)
router = APIRouter(tags=["hotspots"])

DEFAULT_LIMIT = 5000
MAX_LIMIT = 10_000


class Hotspot(BaseModel):
    id: int
    event_id: UUID
    source: str
    instrument: str
    satellite: str
    observed_at: datetime
    latitude: float
    longitude: float
    attributes: dict[str, str]
    duplicate_of: UUID | None
    country: str | None
    risk: DetectionRisk | None
    weather: DetectionWeather | None


class _HotspotRow(ConditionColumns):
    id: int
    event_id: UUID
    source: str
    instrument: str
    satellite: str
    observed_at: datetime
    latitude: float
    longitude: float
    attributes: dict[str, str]
    duplicate_of: UUID | None

    def hotspot(self) -> Hotspot:
        return Hotspot(
            id=self.id,
            event_id=self.event_id,
            source=self.source,
            instrument=self.instrument,
            satellite=self.satellite,
            observed_at=self.observed_at,
            latitude=self.latitude,
            longitude=self.longitude,
            attributes=self.attributes,
            duplicate_of=self.duplicate_of,
            country=self.country,
            risk=self.risk(),
            weather=self.weather(),
        )


class HotspotResults(BaseModel):
    items: list[Hotspot]
    truncated: bool


@dataclass(frozen=True, slots=True)
class HotspotFilter:
    bounds: Bounds | None = None
    date_from: date | None = None
    date_to: date | None = None
    products: tuple[FirmsProduct, ...] = ()
    include_duplicates: bool = False
    limit: int = DEFAULT_LIMIT


def find_hotspots(connection: psycopg.Connection, query: HotspotFilter) -> HotspotResults:
    """Matching detections, newest first; ``truncated`` when more than ``limit`` match."""
    clauses: list[LiteralString] = []
    parameters: list[object] = []
    if query.bounds is not None:
        clauses.append("ST_Intersects(d.location, ST_MakeEnvelope(%s, %s, %s, %s, 4326))")
        parameters.extend(
            (query.bounds.west, query.bounds.south, query.bounds.east, query.bounds.north)
        )
    if query.date_from:
        clauses.append("d.observed_at >= %s")
        parameters.append(datetime.combine(query.date_from, time.min, tzinfo=UTC))
    if query.date_to:
        clauses.append("d.observed_at < ((%s::date + 1)::timestamp AT TIME ZONE 'UTC')")
        parameters.append(query.date_to)
    if query.products:
        clauses.append("d.product = ANY(%s)")
        parameters.append([product.value for product in query.products])
    if not query.include_duplicates:
        clauses.append("d.duplicate_of IS NULL")
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    # One extra row tells whether the result was truncated.
    parameters.append(query.limit + 1)
    sql = f"""
        SELECT d.id, d.event_id, d.product AS source, d.instrument, d.satellite, d.observed_at,
               d.latitude, d.longitude, d.attributes, d.duplicate_of, {CONDITION_COLUMNS}
        FROM fire_detections AS d LEFT JOIN detection_conditions AS c USING (event_id){where}
        ORDER BY d.observed_at DESC, d.id DESC LIMIT %s
    """
    rows = fetch_all(connection, _HotspotRow, sql, parameters)
    return HotspotResults(
        items=[row.hotspot() for row in rows[: query.limit]], truncated=len(rows) > query.limit
    )


@router.get("/hotspots", response_model=HotspotResults)
def hotspots(
    *,
    settings: SettingsDep,
    bounds: BoundsDep,
    date_from: date | None = None,
    date_to: date | None = None,
    product: Annotated[list[FirmsProduct] | None, Query()] = None,
    include_duplicates: bool = False,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
) -> HotspotResults:
    """Detections from the PostGIS projection of ``firms.detections``, newest first.

    Cross-sensor duplicates (the same fire seen by another satellite within minutes)
    are hidden unless ``include_duplicates`` is set.
    """
    if date_from and date_to and date_from > date_to:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "date_from must not be later than date_to"
        )
    query = HotspotFilter(
        bounds=bounds,
        date_from=date_from,
        date_to=date_to,
        products=tuple(product or ()),
        include_duplicates=include_duplicates,
        limit=limit,
    )
    try:
        with connect(settings.database_url) as connection:
            return find_hotspots(connection, query)
    except psycopg.Error as exc:
        LOGGER.exception("Unable to query hotspot detections")
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "Hotspot data is currently unavailable"
        ) from exc
