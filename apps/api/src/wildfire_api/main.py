import logging
import os
from datetime import UTC, date, datetime, time
from typing import Annotated

import psycopg
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row
from pydantic import BaseModel

app = FastAPI(title="Wildfire Intelligence Platform API", version="0.1.0")
LOGGER = logging.getLogger(__name__)


class Hotspot(BaseModel):
    id: int
    source: str
    satellite: str
    observed_at: datetime
    latitude: float
    longitude: float
    attributes: dict[str, str]


class HotspotResults(BaseModel):
    items: list[Hotspot]
    truncated: bool


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/hotspots", response_model=HotspotResults, tags=["hotspots"])
def hotspots(
    west: Annotated[float | None, Query(ge=-180, le=180)] = None,
    south: Annotated[float | None, Query(ge=-90, le=90)] = None,
    east: Annotated[float | None, Query(ge=-180, le=180)] = None,
    north: Annotated[float | None, Query(ge=-90, le=90)] = None,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: Annotated[int, Query(ge=1, le=10000)] = 5000,
) -> HotspotResults:
    bounds = (west, south, east, north)
    if any(value is not None for value in bounds):
        if west is None or south is None or east is None or north is None:
            raise HTTPException(422, "All four geographic bounds are required")
        if west >= east or south >= north:
            raise HTTPException(422, "West must be less than east and south less than north")
    if date_from and date_to and date_from > date_to:
        raise HTTPException(422, "date_from must not be later than date_to")

    clauses = []
    parameters: list[object] = []
    if west is not None and south is not None and east is not None and north is not None:
        clauses.append("ST_Intersects(location, ST_MakeEnvelope(%s, %s, %s, %s, 4326))")
        parameters.extend((west, south, east, north))
    if date_from:
        clauses.append("observed_at >= %s")
        parameters.append(datetime.combine(date_from, time.min, tzinfo=UTC))
    if date_to:
        clauses.append("observed_at < ((%s::date + 1)::timestamp AT TIME ZONE 'UTC')")
        parameters.append(date_to)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    parameters.append(limit + 1)
    query = (
        "SELECT id, source, satellite, observed_at, latitude, longitude, attributes "
        f"FROM firms_detections{where} ORDER BY observed_at DESC, id DESC LIMIT %s"
    )
    database_url = os.environ.get(
        "DATABASE_URL", "postgresql://wildfire:wildfire_dev@localhost:55432/wildfire"
    )
    try:
        with psycopg.connect(database_url, row_factory=dict_row, connect_timeout=5) as connection:
            rows = connection.execute(query, parameters).fetchall()
    except psycopg.Error as exc:
        LOGGER.exception("Unable to query hotspot detections")
        raise HTTPException(503, "Hotspot data is currently unavailable") from exc
    return HotspotResults(
        items=[Hotspot.model_validate(row) for row in rows[:limit]],
        truncated=len(rows) > limit,
    )
