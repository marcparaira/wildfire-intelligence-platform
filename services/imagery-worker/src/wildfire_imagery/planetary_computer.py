"""Sentinel-2 L2A from Microsoft Planetary Computer: STAC search and image rendering.

Neither needs an API key. See ``wildfire_imagery.sentinel`` for how the granules
become scenes.
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from http import HTTPStatus
from typing import Annotated, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

from pydantic import AwareDatetime, BaseModel, Field, ValidationError

from wildfire_imagery.scenes import FetchedImage
from wildfire_imagery.tiles import Bounds
from wildfire_producers.http import (
    HttpGet,
    HttpPost,
    excerpt,
    http_get,
    http_post,
    retry_after_seconds,
)
from wildfire_producers.scheduler import RetryLater

STAC_SEARCH_URL = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
RENDER_URL = "https://planetarycomputer.microsoft.com/api/data/v1/item/bbox"
TIMEOUT_SECONDS = 120
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
    """Sentinel-2 L2A search and rendering.

    Rendering clips one granule to a bounding box and
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
        url = STAC_SEARCH_URL
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
            content = self._post(url, json.dumps(body).encode(), timeout=TIMEOUT_SECONDS)
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
            f"{RENDER_URL}/{west},{south},{east},{north}/"
            f"{width}x{height}.{RENDER_FORMAT}?{urlencode(parameters)}"
        )
        try:
            content = self._get(url, timeout=TIMEOUT_SECONDS)
        except HTTPError as exc:
            if exc.code == HTTPStatus.TOO_MANY_REQUESTS:
                seconds = retry_after_seconds(exc.headers.get("Retry-After"))
                raise RetryLater("Planetary Computer rate limit reached", seconds) from exc
            raise RuntimeError(f"Sentinel-2 render failed with HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise RuntimeError(f"Sentinel-2 render failed: {type(exc).__name__}") from exc
        if not (content[:4] == WEBP_SIGNATURE[0] and content[8:12] == WEBP_SIGNATURE[1]):
            raise RuntimeError(f"Sentinel-2 render returned no image: {excerpt(content)}")
        return FetchedImage(url, content)
