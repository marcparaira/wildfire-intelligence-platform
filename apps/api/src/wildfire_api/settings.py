"""Runtime configuration from environment variables, provided to routes as a dependency."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

from fastapi import Depends

DEFAULT_DATABASE_URL = "postgresql://wildfire:wildfire_dev@localhost:55432/wildfire"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:18081"
# Shared with the ingestor, which writes the images (see compose.yaml).
DEFAULT_IMAGERY_DIR = "data/imagery"


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    kafka_bootstrap_servers: str
    schema_registry_url: str
    imagery_dir: Path

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
            kafka_bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS
            ),
            schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY_URL),
            imagery_dir=Path(os.environ.get("IMAGERY_DIR", DEFAULT_IMAGERY_DIR)),
        )


def get_settings() -> Settings:
    """Read per request: it is a few environment lookups, and tests can change them."""
    return Settings.from_env()


SettingsDep = Annotated[Settings, Depends(get_settings)]
