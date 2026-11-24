"""HTTP clients for the external providers.

NASA FIRMS, Open-Meteo, NASA GIBS, and Microsoft Planetary Computer (Sentinel-2). Each
client takes an ``HttpGet`` (and ``HttpPost``) so tests can substitute canned responses. Clients
translate transport failures into ``RuntimeError`` (or ``RetryLater`` for rate
limits) and return raw payloads; validating them is the caller's job.
"""

import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from http import HTTPStatus
from http.client import HTTPResponse
from typing import Annotated, Final, Literal, Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from pydantic import AwareDatetime, BaseModel, Field, TypeAdapter, ValidationError

from wildfire_events.countries import REGION_BOUNDS
from wildfire_events.models import FirmsProduct, ImageryLayer
from wildfire_ingestor.scheduler import RetryLater
from wildfire_ingestor.weather import FORECAST_DAYS, HOURLY_VARIABLES, MODEL, ForecastCell

FIRMS_AREA_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
FIRMS_TIMEOUT_SECONDS = 120
MAX_DAY_RANGE = 5  # The FIRMS area API serves at most five days per request.
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_TIMEOUT_SECONDS = 60
DEFAULT_RETRY_AFTER_SECONDS = 60
GIBS_WMS_URL = "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi"
GIBS_TIMEOUT_SECONDS = 60
GIBS_MEDIA_TYPE: Final = "image/jpeg"
JPEG_SIGNATURE = b"\xff\xd8\xff"
# Enough of a WMS service-exception document to say what went wrong.
ERROR_EXCERPT_CHARS = 300
PLANETARY_COMPUTER_STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
PLANETARY_COMPUTER_RENDER_URL = "https://planetarycomputer.microsoft.com/api/data/v1/item/bbox"
PLANETARY_COMPUTER_TIMEOUT_SECONDS = 120
SENTINEL2_COLLECTION = "sentinel-2-l2a"
# Short-wave infrared fire composite: B12 (2.19 um) as red, B8A (865 nm) as green and
# B04 (665 nm) as blue. Hot flames saturate B12, burn scars lose B8A, smoke is thin at
# these wavelengths.
SWIR_BANDS = ("B12", "B8A", "B04")
# L2A values are reflectance x 10000 plus the 1000 offset of processing baseline 04.00
# (all data since 2022): reflectance 0-0.4 is stretched over the output range.
SWIR_RESCALE = "1000,5000"
# The rendered images keep the granule's no-data area transparent, for compositing.
RENDER_FORMAT = "webp"
WEBP_SIGNATURE = (b"RIFF", b"WEBP")
SEARCH_PAGE_SIZE = 500
MAX_SEARCH_PAGES = 20

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


class HttpPost(Protocol):
    """POST a JSON body and return the response body; raises ``urllib`` errors."""

    def __call__(self, url: str, body: bytes, *, timeout: float) -> bytes: ...


def http_post(url: str, body: bytes, *, timeout: float) -> bytes:
    request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with cast(HTTPResponse, urlopen(request, timeout=timeout)) as response:
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


@dataclass(frozen=True, slots=True)
class FetchedImage:
    url: str
    content: bytes


class GibsClient:
    """NASA GIBS (Global Imagery Browse Services) WMS in geographic coordinates.

    GIBS needs no API key. It answers errors such as an unknown layer with HTTP 200
    and an XML service exception, so the body is checked to be a JPEG.
    """

    def __init__(self, get: HttpGet = http_get) -> None:
        self._get: HttpGet = get

    def fetch_image(
        self,
        layer: ImageryLayer,
        day: date,
        bounds: tuple[float, float, float, float],
        width: int,
        height: int,
    ) -> FetchedImage:
        """The layer's daily composite over ``bounds`` (west, south, east, north) as JPEG.

        Raises ``RetryLater`` when rate limited and ``RuntimeError`` on other failures.
        """
        west, south, east, north = bounds
        parameters = {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "VERSION": "1.3.0",
            "LAYERS": layer.value,
            "STYLES": "",
            "FORMAT": GIBS_MEDIA_TYPE,
            "CRS": "EPSG:4326",
            # WMS 1.3.0 orders EPSG:4326 axes latitude first.
            "BBOX": f"{south},{west},{north},{east}",
            "WIDTH": str(width),
            "HEIGHT": str(height),
            "TIME": day.isoformat(),
        }
        url = f"{GIBS_WMS_URL}?{urlencode(parameters)}"
        try:
            content = self._get(url, timeout=GIBS_TIMEOUT_SECONDS)
        except HTTPError as exc:
            if exc.code == HTTPStatus.TOO_MANY_REQUESTS:
                seconds = retry_after_seconds(exc.headers.get("Retry-After"))
                raise RetryLater("GIBS rate limit reached", seconds) from exc
            raise RuntimeError(f"GIBS request failed with HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise RuntimeError(f"GIBS request failed: {type(exc).__name__}") from exc
        if not content.startswith(JPEG_SIGNATURE):
            excerpt = content[:ERROR_EXCERPT_CHARS].decode("utf-8", "replace")
            raise RuntimeError(f"GIBS returned no image: {excerpt}")
        return FetchedImage(url, content)


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


Bounds = tuple[float, float, float, float]
Ring = tuple[tuple[float, float], ...]


@dataclass(frozen=True, slots=True)
class SentinelGranule:
    """One Sentinel-2 L2A granule (a 110 km MGRS tile of one pass)."""

    item_id: str
    platform: str
    acquired_at: datetime
    cloud_cover: float
    bounds: Bounds  # west, south, east, north
    # Outer rings of the area with data. At the edge of a swath it is a thin wedge of
    # the bounding box. Empty when the provider gave no footprint: use the bounds.
    footprint: tuple[Ring, ...] = ()


class _StacProperties(BaseModel):
    datetime: AwareDatetime
    platform: str = Field(min_length=1, max_length=32)
    cloud_cover: float = Field(alias="eo:cloud_cover", ge=0, le=100)


# A GeoJSON position: longitude, latitude, and possibly an elevation.
_Position = Annotated[list[float], Field(min_length=2)]
_LinearRing = list[_Position]


class _StacPolygon(BaseModel):
    type: Literal["Polygon"]
    coordinates: list[_LinearRing]

    def outer_rings(self) -> tuple[Ring, ...]:
        return _outer_rings([self.coordinates])


class _StacMultiPolygon(BaseModel):
    type: Literal["MultiPolygon"]
    coordinates: list[list[_LinearRing]]

    def outer_rings(self) -> tuple[Ring, ...]:
        return _outer_rings(self.coordinates)


def _outer_rings(polygons: list[list[_LinearRing]]) -> tuple[Ring, ...]:
    return tuple(
        tuple((point[0], point[1]) for point in polygon[0]) for polygon in polygons if polygon
    )


class _StacItem(BaseModel):
    id: str = Field(min_length=1)
    geometry: _StacPolygon | _StacMultiPolygon | None = Field(default=None, discriminator="type")
    # 2D, or 3D with the minimum and maximum elevations after the coordinates.
    bbox: tuple[float, float, float, float] | tuple[float, float, float, float, float, float]
    properties: _StacProperties

    def granule(self) -> SentinelGranule:
        if len(self.bbox) == 4:
            west, south, east, north = self.bbox
        else:
            west, south, _, east, north, _ = self.bbox
        return SentinelGranule(
            item_id=self.id,
            platform=self.properties.platform.title(),
            acquired_at=self.properties.datetime.astimezone(UTC),
            cloud_cover=self.properties.cloud_cover,
            bounds=(west, south, east, north),
            footprint=() if self.geometry is None else self.geometry.outer_rings(),
        )


class _StacLink(BaseModel):
    rel: str
    href: str
    method: str = "GET"
    body: dict[str, object] | None = None


class _StacPage(BaseModel):
    features: list[_StacItem]
    links: list[_StacLink] = Field(default_factory=list)

    def next_request(self) -> tuple[str, dict[str, object]] | None:
        for link in self.links:
            if link.rel == "next" and link.method.upper() == "POST" and link.body is not None:
                return link.href, link.body
        return None


class PlanetaryComputerClient:
    """Sentinel-2 L2A from Microsoft Planetary Computer: STAC search and image rendering.

    Neither needs an API key. Rendering clips one granule to a bounding box and
    returns it as an image whose area outside the granule is transparent.
    """

    def __init__(self, get: HttpGet = http_get, post: HttpPost = http_post) -> None:
        self._get: HttpGet = get
        self._post: HttpPost = post

    def search(
        self, areas: Sequence[Bounds], start: date, end: date, max_cloud_cover: float
    ) -> list[SentinelGranule]:
        """Granules intersecting any of ``areas`` sensed on the UTC days ``start``-``end``.

        Granules cloudier than ``max_cloud_cover`` percent are left out. Raises
        ``RetryLater`` when rate limited and ``RuntimeError`` on other failures.
        """
        if not areas:
            return []
        url = PLANETARY_COMPUTER_STAC_URL
        body: dict[str, object] = {
            "collections": [SENTINEL2_COLLECTION],
            "intersects": {
                "type": "MultiPolygon",
                "coordinates": [[[[w, s], [e, s], [e, n], [w, n], [w, s]]] for w, s, e, n in areas],
            },
            "datetime": f"{start.isoformat()}T00:00:00Z/{end.isoformat()}T23:59:59Z",
            "query": {"eo:cloud_cover": {"lte": max_cloud_cover}},
            "fields": {
                "include": [
                    "id",
                    "bbox",
                    "geometry",
                    "properties.datetime",
                    "properties.platform",
                    "properties.eo:cloud_cover",
                ],
                "exclude": ["assets", "links"],
            },
            "limit": SEARCH_PAGE_SIZE,
        }
        granules: list[SentinelGranule] = []
        for _ in range(MAX_SEARCH_PAGES):
            page = self._search_page(url, body)
            granules.extend(item.granule() for item in page.features)
            following = page.next_request()
            if following is None:
                return granules
            url, body = following
        raise RuntimeError(f"Sentinel-2 search returned more than {MAX_SEARCH_PAGES} pages")

    def _search_page(self, url: str, body: dict[str, object]) -> _StacPage:
        try:
            content = self._post(
                url, json.dumps(body).encode(), timeout=PLANETARY_COMPUTER_TIMEOUT_SECONDS
            )
            return _StacPage.model_validate_json(content)
        except HTTPError as exc:
            if exc.code == HTTPStatus.TOO_MANY_REQUESTS:
                seconds = retry_after_seconds(exc.headers.get("Retry-After"))
                raise RetryLater("Planetary Computer rate limit reached", seconds) from exc
            raise RuntimeError(f"Sentinel-2 search failed with HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError, ValidationError) as exc:
            raise RuntimeError(f"Sentinel-2 search failed: {type(exc).__name__}") from exc

    def render(self, item_id: str, bounds: Bounds, width: int, height: int) -> FetchedImage:
        """The SWIR composite of one granule over ``bounds`` (WebP with transparency)."""
        west, south, east, north = bounds
        parameters = [
            ("collection", SENTINEL2_COLLECTION),
            ("item", item_id),
            *(("assets", band) for band in SWIR_BANDS),
            *(("rescale", SWIR_RESCALE) for _ in SWIR_BANDS),
            ("nodata", "0"),
        ]
        url = (
            f"{PLANETARY_COMPUTER_RENDER_URL}/{west},{south},{east},{north}/"
            f"{width}x{height}.{RENDER_FORMAT}?{urlencode(parameters)}"
        )
        try:
            content = self._get(url, timeout=PLANETARY_COMPUTER_TIMEOUT_SECONDS)
        except HTTPError as exc:
            if exc.code == HTTPStatus.TOO_MANY_REQUESTS:
                seconds = retry_after_seconds(exc.headers.get("Retry-After"))
                raise RetryLater("Planetary Computer rate limit reached", seconds) from exc
            raise RuntimeError(f"Sentinel-2 render failed with HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise RuntimeError(f"Sentinel-2 render failed: {type(exc).__name__}") from exc
        if not (content[:4] == WEBP_SIGNATURE[0] and content[8:12] == WEBP_SIGNATURE[1]):
            excerpt = content[:ERROR_EXCERPT_CHARS].decode("utf-8", "replace")
            raise RuntimeError(f"Sentinel-2 render returned no image: {excerpt}")
        return FetchedImage(url, content)
