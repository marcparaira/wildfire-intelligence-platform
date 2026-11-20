"""Assembles the pipeline status from its two independent halves, with a short cache."""

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from threading import Lock

import psycopg

from wildfire_api.pipeline.models import SOURCES, KafkaSection, PipelineStatus, SourceStatus
from wildfire_api.pipeline.sources import unknown_source

LOGGER = logging.getLogger(__name__)

# Collecting the status costs a full scan of fire_detections and a round of Kafka
# metadata requests, so dashboards polling it share one result for a few seconds.
CACHE_SECONDS = 10.0

POSTGIS_UNAVAILABLE = "PostGIS is unavailable; source freshness is unknown"
KAFKA_UNAVAILABLE = "Kafka is unavailable; throughput, lag, and dead letters are unknown"


def utc_now() -> datetime:
    return datetime.now(UTC)


class PipelineStatusCollector:
    """Collects both halves; a failing half is reported in ``errors``, not raised.

    ``read_sources`` signals an unavailable database with ``psycopg.Error``;
    ``read_kafka`` may raise anything the Kafka clients raise.
    """

    def __init__(
        self,
        read_sources: Callable[[datetime], list[SourceStatus]],
        read_kafka: Callable[[datetime], KafkaSection],
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.read_sources: Callable[[datetime], list[SourceStatus]] = read_sources
        self.read_kafka: Callable[[datetime], KafkaSection] = read_kafka
        self.clock: Callable[[], datetime] = clock

    def collect(self) -> PipelineStatus:
        now = self.clock()
        errors: list[str] = []
        try:
            sources = self.read_sources(now)
        except psycopg.Error:
            LOGGER.exception("Pipeline status: PostGIS is unavailable")
            errors.append(POSTGIS_UNAVAILABLE)
            sources = [unknown_source(spec) for spec in SOURCES]
        kafka: KafkaSection | None = None
        try:
            kafka = self.read_kafka(now)
        except Exception:
            # Degrade-gracefully boundary: kafka-python raises many unrelated types
            # (KafkaError, timeouts, assertion errors), and a status endpoint must
            # still report the PostGIS half. The failure is logged with its traceback.
            LOGGER.exception("Pipeline status: Kafka is unavailable")
            errors.append(KAFKA_UNAVAILABLE)
        return PipelineStatus(
            checked_at=now,
            sources=sources,
            topics=kafka.topics if kafka else None,
            consumers=kafka.consumers if kafka else None,
            dead_letters=kafka.dead_letters if kafka else None,
            errors=errors,
        )


class StatusCache:
    """Keeps the latest status for ``ttl_seconds``; concurrent callers wait for one fetch."""

    def __init__(
        self,
        ttl_seconds: float = CACHE_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.ttl_seconds: float = ttl_seconds
        self.monotonic: Callable[[], float] = monotonic
        self._lock: Lock = Lock()
        self._cached: tuple[float, PipelineStatus] | None = None

    def get(self, fetch: Callable[[], PipelineStatus]) -> PipelineStatus:
        with self._lock:
            if self._cached is not None and self.monotonic() - self._cached[0] < self.ttl_seconds:
                return self._cached[1]
            status = fetch()
            self._cached = (self.monotonic(), status)
            return status

    def clear(self) -> None:
        with self._lock:
            self._cached = None
