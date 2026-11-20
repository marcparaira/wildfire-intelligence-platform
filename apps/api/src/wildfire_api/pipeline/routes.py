"""``GET /pipeline/status``."""

from functools import cache
from typing import Annotated

from fastapi import APIRouter, Depends

from wildfire_api.pipeline.kafka_offsets import KafkaOffsets
from wildfire_api.pipeline.models import PipelineStatus
from wildfire_api.pipeline.sources import PostgisSources
from wildfire_api.pipeline.status import PipelineStatusCollector, StatusCache
from wildfire_api.settings import SettingsDep

router = APIRouter(tags=["pipeline"])


def status_collector(settings: SettingsDep) -> PipelineStatusCollector:
    return PipelineStatusCollector(
        read_sources=PostgisSources(settings.database_url).read,
        read_kafka=KafkaOffsets(settings.kafka_bootstrap_servers).read,
    )


@cache
def status_cache() -> StatusCache:
    """The process-wide cache shared by every request."""
    return StatusCache()


@router.get("/pipeline/status", response_model=PipelineStatus)
def pipeline_status(
    collector: Annotated[PipelineStatusCollector, Depends(status_collector)],
    cached: Annotated[StatusCache, Depends(status_cache)],
) -> PipelineStatus:
    """Freshness, throughput, consumer lag, and dead-letter counts for each source.

    A source is ``fresh`` while its newest observation (or forecast retrieval) is
    younger than ``fresh_within_hours``, ``delayed`` until ``stale_after_hours``,
    then ``stale``; ``no_data`` means nothing was ingested and ``unknown`` that
    PostGIS could not be read. Kafka sections are null when Kafka is unavailable.
    Results are cached for a few seconds.
    """
    return cached.get(collector.collect)
