"""Scenes, the imagery store, and the outcome of acquiring a scene.

The store is a claim check: each image is written there first, and only then is its
``SatelliteScene`` metadata published to ``imagery.scenes`` (the image never
travels through Kafka). Keys contain the content hash, so stored files never change.
"""

import uuid
from dataclasses import dataclass
from datetime import date
from enum import StrEnum, auto
from pathlib import Path
from typing import Protocol

from wildfire_events.models import ImageryLayer, scene_id
from wildfire_imagery.tiles import Tile

SHORT_HASH_CHARS = 16


@dataclass(frozen=True, slots=True)
class FetchedImage:
    url: str
    content: bytes


@dataclass(frozen=True, slots=True)
class SceneRequest:
    layer: ImageryLayer
    day: date
    tile: Tile

    @property
    def scene_id(self) -> uuid.UUID:
        return scene_id(self.layer, self.day, self.tile.name)


def storage_key(request: SceneRequest, sha256: str, extension: str = "jpg") -> str:
    """Store path of an image; the content hash makes every stored file immutable."""
    return (
        f"{request.layer.value}/{request.day.isoformat()}/"
        f"{request.tile.name}-{sha256[:SHORT_HASH_CHARS]}.{extension}"
    )


class SceneStore(Protocol):
    """Where image bytes are kept; ``FileSceneStore`` implements it."""

    def put(self, key: str, content: bytes) -> None: ...


class FileSceneStore:
    """Images as files under a root directory shared with the API (read-only there).

    Files are written to a temporary name and renamed, so a reader never sees a
    partial image. Keys are content-addressed, so an existing file is left alone.
    """

    def __init__(self, root: Path) -> None:
        self.root: Path = root

    def put(self, key: str, content: bytes) -> None:
        path = self.root / key
        if path.exists():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
        _ = partial.write_bytes(content)
        _ = partial.replace(path)


class Outcome(StrEnum):
    ACQUIRED = auto()
    EMPTY = auto()
    FAILED = auto()
