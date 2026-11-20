"""Countries covered by the platform and point-in-country lookup.

Uses the bundled Natural Earth 1:10m boundaries (public domain, version 5.1.1), so
services can assign countries without PostGIS.
"""

from collections.abc import Sequence
from enum import StrEnum
from functools import cache
from importlib.resources import files
from typing import Literal, TypedDict, cast

import numpy as np
import numpy.typing as npt
import shapely
from pydantic import TypeAdapter
from shapely.geometry import MultiPolygon, shape
from shapely.geometry.base import BaseGeometry

# (west, south, east, north) of the selected countries, in degrees.
REGION_BOUNDS = (92.0, -12.0, 156.0, 29.0)


class Country(StrEnum):
    """Selected countries; values are ISO 3166-1 alpha-3 codes."""

    BRN = "BRN"
    IDN = "IDN"
    KHM = "KHM"
    LAO = "LAO"
    MMR = "MMR"
    MYS = "MYS"
    PHL = "PHL"
    PNG = "PNG"
    SGP = "SGP"
    THA = "THA"
    TLS = "TLS"
    VNM = "VNM"


# Event fields use a Literal of every member; see the note in ``wildfire_events.models``.
CountryLiteral = Literal[
    Country.BRN,
    Country.IDN,
    Country.KHM,
    Country.LAO,
    Country.MMR,
    Country.MYS,
    Country.PHL,
    Country.PNG,
    Country.SGP,
    Country.THA,
    Country.TLS,
    Country.VNM,
]


class FeatureProperties(TypedDict):
    code: str


class Feature(TypedDict):
    properties: FeatureProperties
    geometry: dict[str, object]


class FeatureCollection(TypedDict):
    features: list[Feature]


_FEATURE_COLLECTION = TypeAdapter(FeatureCollection)


class CountryLocator:
    """Finds the selected country that contains a point (boundaries included)."""

    def __init__(self, geojson: FeatureCollection) -> None:
        parts: list[BaseGeometry] = []
        countries: list[Country] = []
        for feature in geojson["features"]:
            country = Country(feature["properties"]["code"])
            geometry = shape(feature["geometry"])
            polygons = list(geometry.geoms) if isinstance(geometry, MultiPolygon) else [geometry]
            parts.extend(polygons)
            countries.extend([country] * len(polygons))
        self._parts: npt.NDArray[np.object_] = np.array(parts, dtype=object)
        self._countries: tuple[Country, ...] = tuple(countries)
        shapely.prepare(self._parts)
        self._tree: shapely.STRtree = shapely.STRtree(self._parts)

    def locate(self, points: Sequence[tuple[float, float]]) -> list[Country | None]:
        """For each (latitude, longitude), the selected country containing it, if any.

        A point on a shared border gets the first matching country in file order.
        """
        found: list[Country | None] = [None] * len(points)
        if not points:
            return found
        geometries = shapely.points(
            [longitude for _, longitude in points], [latitude for latitude, _ in points]
        )
        pairs = self._tree.query(geometries)
        point_index = cast(npt.NDArray[np.int64], pairs[0])
        part_index = cast(npt.NDArray[np.int64], pairs[1])
        matches = shapely.covers(self._parts[part_index], geometries[point_index])
        hits = zip(
            cast(list[int], point_index[matches].tolist()),
            cast(list[int], part_index[matches].tolist()),
            strict=True,
        )
        for point, part in sorted(hits):
            if found[point] is None:
                found[point] = self._countries[part]
        return found

    def covers(self, points: Sequence[tuple[float, float]]) -> list[bool]:
        """For each (latitude, longitude), whether it lies in or on a selected country."""
        return [country is not None for country in self.locate(points)]


@cache
def selected_countries() -> CountryLocator:
    """The locator for the bundled boundaries, loaded once per process."""
    data = files("wildfire_events").joinpath("data/countries.geojson").read_text("utf-8")
    return CountryLocator(_FEATURE_COLLECTION.validate_json(data))
