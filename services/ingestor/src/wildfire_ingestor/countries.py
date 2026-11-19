"""Point-in-country filter for the selected Southeast Asian countries.

Uses the bundled Natural Earth 1:10m boundaries (public domain, version 5.1.1) so
the ingestor depends only on external APIs and Kafka, not on PostGIS.
"""

from collections.abc import Sequence
from functools import cache
from importlib.resources import files
from typing import TypedDict, cast

import numpy as np
import numpy.typing as npt
import shapely
from pydantic import TypeAdapter
from shapely.geometry import MultiPolygon, shape
from shapely.geometry.base import BaseGeometry

REGION_BOUNDS = (92.0, -12.0, 156.0, 29.0)


class Feature(TypedDict):
    geometry: dict[str, object]


class FeatureCollection(TypedDict):
    features: list[Feature]


_FEATURE_COLLECTION = TypeAdapter(FeatureCollection)


class CountryFilter:
    def __init__(self, geojson: FeatureCollection) -> None:
        parts: list[BaseGeometry] = []
        for feature in geojson["features"]:
            geometry = shape(feature["geometry"])
            if isinstance(geometry, MultiPolygon):
                parts.extend(geometry.geoms)
            else:
                parts.append(geometry)
        self._parts: npt.NDArray[np.object_] = np.array(parts, dtype=object)
        shapely.prepare(self._parts)
        self._tree: shapely.STRtree = shapely.STRtree(self._parts)

    def covers(self, points: Sequence[tuple[float, float]]) -> list[bool]:
        """For each (latitude, longitude), whether it lies in or on a selected country."""
        if not points:
            return []
        geometries = shapely.points(
            [longitude for _, longitude in points], [latitude for latitude, _ in points]
        )
        pairs = self._tree.query(geometries)
        point_index = cast(npt.NDArray[np.int64], pairs[0])
        part_index = cast(npt.NDArray[np.int64], pairs[1])
        matches = shapely.covers(self._parts[part_index], geometries[point_index])
        inside = np.zeros(len(points), dtype=bool)
        inside[point_index[matches]] = True
        return cast(list[bool], inside.tolist())


@cache
def selected_countries() -> CountryFilter:
    data = files("wildfire_ingestor").joinpath("data/countries.geojson").read_text("utf-8")
    return CountryFilter(_FEATURE_COLLECTION.validate_json(data))
