"""Hourly per-country detection statistics from the projection of ``stats.hourly``.

The stream processor counts detections and FRP per country and UTC hour of
observation; the sink worker keeps the latest count per country and hour in
``detection_stats_hourly``.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, LiteralString

import psycopg
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from wildfire_api.db import connect, fetch_all
from wildfire_api.settings import SettingsDep
from wildfire_events.countries import Country

LOGGER = logging.getLogger(__name__)
router = APIRouter(tags=["statistics"])

DEFAULT_WINDOW = timedelta(hours=48)
MAX_WINDOW = timedelta(days=31)


class HourlyStats(BaseModel):
    country: Country
    hour_start: datetime
    detections: int
    detections_with_frp: int
    total_frp_mw: float
    max_frp_mw: float | None


class HourlyStatsResults(BaseModel):
    items: list[HourlyStats]


class InvalidWindow(ValueError):
    """The requested time range cannot be served."""


def stats_window(
    since: datetime | None, until: datetime | None, now: datetime
) -> tuple[datetime, datetime]:
    """The ``hour_start`` range to return: by default the ``DEFAULT_WINDOW`` before ``until``.

    ``until`` defaults to ``now``; ``since`` defaults to ``DEFAULT_WINDOW`` before ``until``.
    Raises ``InvalidWindow`` for naive times, a reversed range, or more than ``MAX_WINDOW``.
    """
    if (since is not None and since.tzinfo is None) or (until is not None and until.tzinfo is None):
        raise InvalidWindow("since and until must include a timezone")
    end = (until or now).astimezone(UTC)
    start = since.astimezone(UTC) if since is not None else end - DEFAULT_WINDOW
    if start > end:
        raise InvalidWindow("since must not be later than until")
    if end - start > MAX_WINDOW:
        raise InvalidWindow(f"the range must not exceed {MAX_WINDOW.days} days")
    return start, end


@dataclass(frozen=True, slots=True)
class StatsFilter:
    since: datetime
    until: datetime
    countries: tuple[Country, ...] = ()


def find_hourly_stats(connection: psycopg.Connection, query: StatsFilter) -> HourlyStatsResults:
    """Rows with ``since <= hour_start <= until``, by hour then country."""
    clauses: list[LiteralString] = ["hour_start BETWEEN %s AND %s"]
    parameters: list[object] = [query.since, query.until]
    if query.countries:
        clauses.append("country = ANY(%s)")
        parameters.append([str(country) for country in query.countries])
    rows = fetch_all(
        connection,
        HourlyStats,
        f"""
        SELECT country, hour_start, detections, detections_with_frp, total_frp_mw, max_frp_mw
        FROM detection_stats_hourly
        WHERE {" AND ".join(clauses)}
        ORDER BY hour_start, country
        """,
        parameters,
    )
    return HourlyStatsResults(items=rows)


@router.get("/stats/hourly", response_model=HourlyStatsResults)
def hourly_stats(
    *,
    settings: SettingsDep,
    country: Annotated[list[Country] | None, Query()] = None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> HourlyStatsResults:
    """Detections and FRP per country (ISO 3166-1 alpha-3) and UTC hour of observation.

    Returns hours whose start is within [since, until]; by default the last 48 hours.
    ``country`` is repeatable; without it every country is returned. Counts include
    each sensor's observation of the same fire. Recent hours can still grow while
    late FIRMS data arrive.
    """
    try:
        start, end = stats_window(since, until, datetime.now(UTC))
    except InvalidWindow as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
    query = StatsFilter(since=start, until=end, countries=tuple(dict.fromkeys(country or ())))
    try:
        with connect(settings.database_url) as connection:
            return find_hourly_stats(connection, query)
    except psycopg.Error as exc:
        LOGGER.exception("Unable to query hourly statistics")
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "Statistics are currently unavailable"
        ) from exc
