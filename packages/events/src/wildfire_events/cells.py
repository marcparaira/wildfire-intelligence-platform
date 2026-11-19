"""H3 cells used as Kafka keys.

Every spatial event is keyed by the resolution-2 H3 cell (average area about
87,000 km2) that contains it. Weather forecasts are produced for exactly these
cells, so a detection's key is also the key of the forecast that covers it, and
both live on the same partition number of their co-partitioned topics.
"""

import h3

PARTITION_RESOLUTION = 2


def partition_cell(latitude: float, longitude: float) -> str:
    return h3.latlng_to_cell(latitude, longitude, PARTITION_RESOLUTION)


def is_partition_cell(cell: str) -> bool:
    return h3.is_valid_cell(cell) and h3.get_resolution(cell) == PARTITION_RESOLUTION


def cell_boundary(cell: str) -> list[tuple[float, float]]:
    """Closed boundary ring as (longitude, latitude) pairs."""
    ring = [(lng, lat) for lat, lng in h3.cell_to_boundary(cell)]
    return [*ring, ring[0]]


def cells_covering(west: float, south: float, east: float, north: float) -> list[str]:
    """Partition cells that overlap a bounding box, in a stable order."""
    shape = h3.LatLngPoly([(south, west), (south, east), (north, east), (north, west)])
    return sorted(h3.h3shape_to_cells_experimental(shape, PARTITION_RESOLUTION, contain="overlap"))
