"""HTTP clients for the external providers (NASA FIRMS and Open-Meteo).

Each client takes an ``HttpGet`` so tests can substitute canned responses. Clients
translate transport failures into ``RuntimeError`` (or ``RetryLater`` for rate
limits) and return raw payloads; validating them is the caller's job.
"""

import math
from collections.abc import Sequence
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http import HTTPStatus
from http.client import HTTPResponse
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import urlopen

from pydantic import TypeAdapter, ValidationError

from wildfire_events.countries import REGION_BOUNDS
from wildfire_events.models import FirmsProduct
from wildfire_ingestor.scheduler import RetryLater
from wildfire_ingestor.weather import FORECAST_DAYS, HOURLY_VARIABLES, MODEL, ForecastCell

FIRMS_AREA_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
FIRMS_TIMEOUT_SECONDS = 120
MAX_DAY_RANGE = 5  # The FIRMS area API serves at most five days per request.
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_TIMEOUT_SECONDS = 60
DEFAULT_RETRY_AFTER_SECONDS = 60

# One location is returned as an object, several as a list of objects.
_OPEN_METEO_PAYLOAD: TypeAdapter[list[object] | dict[str, object]] = TypeAdapter(
    list[object] | dict[str, object]
)


class HttpGet(Protocol):
    """GET a URL and return the body; raises ``urllib`` errors like ``urlopen``."""

    def __call__(self, url: str, *, timeout: float) -> bytes: ...


def http_get(url: str, *, timeout: float) -> bytes:
    with cast(HTTPResponse, urlopen(url, timeout=timeout)) as response:
        return response.read()


class FirmsClient:
    """FIRMS area API for the configured region."""

    def __init__(self, map_key: str, get: HttpGet = http_get) -> None:
        if not map_key.strip():
            raise ValueError("FIRMS_MAP_KEY is required for a live FIRMS request")
        self._map_key: str = map_key
        self._get: HttpGet = get

    def fetch_csv(self, product: FirmsProduct, day_range: int) -> str:
        """The product's detections over the last ``day_range`` days, as FIRMS CSV."""
        if not 1 <= day_range <= MAX_DAY_RANGE:
            raise ValueError(f"FIRMS day range must be between 1 and {MAX_DAY_RANGE}")
        bounds = ",".join(str(int(value)) for value in REGION_BOUNDS)
        url = f"{FIRMS_AREA_URL}/{quote(self._map_key, safe='')}/{product}/{bounds}/{day_range}"
        # Documented exception to "raise ... from ...": the urllib errors carry the
        # request URL, which contains the FIRMS map key (a secret). Chaining them
        # would put the key into logged tracebacks, so the cause is dropped.
        try:
            return self._get(url, timeout=FIRMS_TIMEOUT_SECONDS).decode("utf-8-sig")
        except HTTPError as exc:
            raise RuntimeError(f"FIRMS request failed with HTTP {exc.code}") from None
        except (URLError, TimeoutError, OSError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"FIRMS request failed: {type(exc).__name__}") from None


class OpenMeteoClient:
    """Open-Meteo hourly forecasts, several locations per request."""

    def __init__(self, get: HttpGet = http_get) -> None:
        self._get: HttpGet = get

    def fetch_locations(self, cells: Sequence[ForecastCell]) -> list[object]:
        """One unvalidated location payload per cell, in request order.

        Raises ``RetryLater`` when rate limited and ``RuntimeError`` on other failures.
        """
        parameters = {
            "latitude": ",".join(str(cell.latitude) for cell in cells),
            "longitude": ",".join(str(cell.longitude) for cell in cells),
            "hourly": ",".join(HOURLY_VARIABLES),
            "forecast_days": str(FORECAST_DAYS),
            "models": MODEL,
            "timezone": "GMT",
            "wind_speed_unit": "kmh",
            "precipitation_unit": "mm",
        }
        url = f"{OPEN_METEO_URL}?{urlencode(parameters)}"
        try:
            payload = _OPEN_METEO_PAYLOAD.validate_json(
                self._get(url, timeout=OPEN_METEO_TIMEOUT_SECONDS)
            )
        except HTTPError as exc:
            if exc.code == HTTPStatus.TOO_MANY_REQUESTS:
                seconds = retry_after_seconds(exc.headers.get("Retry-After"))
                raise RetryLater("Open-Meteo rate limit reached", seconds) from exc
            raise RuntimeError(f"Open-Meteo request failed with HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError, UnicodeDecodeError, ValidationError) as exc:
            raise RuntimeError(f"Open-Meteo request failed: {type(exc).__name__}") from exc
        return payload if isinstance(payload, list) else [payload]


def retry_after_seconds(value: str | None) -> int:
    """Seconds to wait from a ``Retry-After`` header (delta seconds or an HTTP date)."""
    if value is None:
        return DEFAULT_RETRY_AFTER_SECONDS
    try:
        seconds = int(value)
    except ValueError:
        return _seconds_until(value)
    return max(1, seconds)


def _seconds_until(http_date: str) -> int:
    try:
        retry_at = parsedate_to_datetime(http_date)
    except (TypeError, ValueError, OverflowError):
        return DEFAULT_RETRY_AFTER_SECONDS
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=UTC)
    return max(1, math.ceil((retry_at - datetime.now(UTC)).total_seconds()))
