"""Stream-processor jobs and their settings."""

import os
from dataclasses import dataclass
from enum import StrEnum, auto

from wildfire_events.topics import ENRICHMENT_GROUP, INCIDENTS_GROUP, STATISTICS_GROUP

DEFAULT_KAFKA = "localhost:19092"
DEFAULT_REGISTRY_URL = "http://localhost:18081"
DEFAULT_STATE_DIR = "state"


class Job(StrEnum):
    """The processor's jobs; each runs as its own Quix application and consumer group."""

    INCIDENTS = auto()
    ENRICHMENT = auto()
    STATISTICS = auto()

    @property
    def default_group(self) -> str:
        match self:
            case Job.INCIDENTS:
                return INCIDENTS_GROUP
            case Job.ENRICHMENT:
                return ENRICHMENT_GROUP
            case Job.STATISTICS:
                return STATISTICS_GROUP

    @property
    def group_variable(self) -> str:
        """Environment variable that overrides the job's consumer group."""
        return f"{self.upper()}_CONSUMER_GROUP"


@dataclass(frozen=True, slots=True)
class Settings:
    """Connection settings for one job.

    Quix keeps each job's state under ``<state_dir>/<consumer_group>``, so jobs can
    share ``state_dir``.
    """

    kafka: str
    registry_url: str
    state_dir: str
    consumer_group: str = INCIDENTS_GROUP

    @classmethod
    def from_env(cls, job: Job = Job.INCIDENTS) -> "Settings":
        return cls(
            kafka=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA),
            registry_url=os.environ.get("SCHEMA_REGISTRY_URL", DEFAULT_REGISTRY_URL),
            state_dir=os.environ.get("STATE_DIR", DEFAULT_STATE_DIR),
            consumer_group=os.environ.get(job.group_variable, job.default_group),
        )
