"""Optional geographic bounding-box filter shared by the map endpoints."""

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, HTTPException, Query, status


@dataclass(frozen=True, slots=True)
class Bounds:
    west: float
    south: float
    east: float
    north: float

    def contains(self, latitude: float, longitude: float) -> bool:
        return self.west <= longitude <= self.east and self.south <= latitude <= self.north


def optional_bounds(
    west: Annotated[float | None, Query(ge=-180, le=180)] = None,
    south: Annotated[float | None, Query(ge=-90, le=90)] = None,
    east: Annotated[float | None, Query(ge=-180, le=180)] = None,
    north: Annotated[float | None, Query(ge=-90, le=90)] = None,
) -> Bounds | None:
    """No bounds, or all four forming a non-empty box (422 otherwise)."""
    if west is None and south is None and east is None and north is None:
        return None
    if west is None or south is None or east is None or north is None:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "All four geographic bounds are required"
        )
    if west >= east or south >= north:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "West must be less than east and south less than north",
        )
    return Bounds(west, south, east, north)


BoundsDep = Annotated[Bounds | None, Depends(optional_bounds)]
