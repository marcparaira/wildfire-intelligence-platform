"""The fixed grid that imagery is fetched in.

Images are fetched per tile of a ``TILE_DEGREES`` grid rather than per incident:
neighbouring incidents share tiles, a growing incident only adds the tiles it newly
reaches, and a tile of one layer on one day (or pass) is a stable scene that is
acquired once.
"""

import math
from dataclasses import dataclass

TILE_DEGREES = 0.5
# Context around the outermost detections of an incident, for the daily imagery.
INCIDENT_MARGIN_DEGREES = 0.05

Bounds = tuple[float, float, float, float]  # west, south, east, north in degrees


@dataclass(frozen=True, slots=True, order=True)
class Tile:
    """A ``TILE_DEGREES`` square; column and row count from longitude 0 and latitude 0."""

    column: int
    row: int

    @property
    def name(self) -> str:
        return f"x{self.column}y{self.row}"

    @property
    def bounds(self) -> Bounds:
        """West, south, east, north in degrees."""
        west = self.column * TILE_DEGREES
        south = self.row * TILE_DEGREES
        return west, south, west + TILE_DEGREES, south + TILE_DEGREES


def tiles_covering(
    west: float,
    south: float,
    east: float,
    north: float,
    margin: float = INCIDENT_MARGIN_DEGREES,
) -> list[Tile]:
    """Tiles intersecting the bounds grown by ``margin``, row by row from the south-west."""
    max_column = math.ceil(180 / TILE_DEGREES) - 1
    max_row = math.ceil(90 / TILE_DEGREES) - 1

    def index(value: float, upper: int) -> int:
        return max(-upper - 1, min(upper, math.floor(value / TILE_DEGREES)))

    columns = range(index(west - margin, max_column), index(east + margin, max_column) + 1)
    rows = range(index(south - margin, max_row), index(north + margin, max_row) + 1)
    return [Tile(column, row) for row in rows for column in columns]
