"""Point-in-country filter for the selected Southeast Asian countries.

Uses the bundled Natural Earth 1:10m boundaries (public domain, version 5.1.1) so
the ingestor depends only on external APIs and Kafka, not on PostGIS.
"""

import json
from collections.abc import Sequence
from functools import cache
from importlib.resources import files

import numpy as np
import shapely
from shapely.geometry import shape

REGION_BOUNDS = (92.0, -12.0, 156.0, 29.0)


class CountryFilter:
    def __init__(self, geojson: dict) -> None:
        parts = []
        for feature in geojson["features"]:
            geometry = shape(feature["geometry"])
            parts.extend(getattr(geometry, "geoms", [geometry]))
        self._parts = np.array(parts, dtype=object)
        shapely.prepare(self._parts)
        self._tree = shapely.STRtree(self._parts)

    def covers(self, points: Sequence[tuple[float, float]]) -> list[bool]:
        """For each (latitude, longitude), whether it lies in or on a selected country."""
        if not points:
            return []
        latitudes, longitudes = zip(*points, strict=True)
        geometries = shapely.points(np.array(longitudes), np.array(latitudes))
        point_index, part_index = self._tree.query(geometries)
        matches = shapely.covers(self._parts[part_index], geometries[point_index])
        inside = np.zeros(len(points), dtype=bool)
        inside[point_index[matches]] = True
        return inside.tolist()


@cache
def selected_countries() -> CountryFilter:
    data = files("wildfire_ingestor").joinpath("data/countries.geojson").read_text("utf-8")
    return CountryFilter(json.loads(data))
