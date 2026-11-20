"""Enrichment job: joins ``firms.detections`` with ``weather.forecasts`` per H3 cell.

Pipeline::

    firms.detections  ─ decode ─┐
                                ├─ concat ─ Enricher (stateful, per cell) ─ detections.enriched
    weather.forecasts ─ decode ─┘
    (invalid records → stream-processor.dlq; forecast tombstones clear the cell)

Both topics are keyed by the resolution-2 H3 cell and co-partitioned, so the
concatenated stream keeps one state per cell. Quix consumes the two topics in
record-timestamp order, so a detection sees the forecasts published before it.
Output records keep the detection's key, and Quix's murmur2 partitioner matches
the ingestor's, so ``detections.enriched`` is co-partitioned with its sources.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

from quixstreams import Application, State

from wildfire_events.codec import EventCodec
from wildfire_events.countries import selected_countries
from wildfire_events.kafka import ensure_topics
from wildfire_events.models import EnrichedDetection, FirmsDetection, WeatherForecast
from wildfire_events.registry import SubjectRegistry
from wildfire_events.topics import (
    DETECTIONS_ENRICHED,
    FIRMS_DETECTIONS,
    STREAM_PROCESSOR_DLQ,
    WEATHER_FORECASTS,
    TopicSpec,
    value_subject,
)
from wildfire_processor.adapters import (
    DeadLetters,
    Headers,
    QuixState,
    RecordDecoder,
    RecordProducer,
    is_present,
)
from wildfire_processor.enrichment import CountryLocator, Enricher, ForecastUpdate
from wildfire_processor.quix import application, output_topic
from wildfire_processor.settings import Settings


@dataclass(frozen=True, slots=True)
class EnrichmentTopics:
    """Topic definitions; tests substitute their own."""

    detections: TopicSpec = FIRMS_DETECTIONS
    forecasts: TopicSpec = WEATHER_FORECASTS
    enriched: TopicSpec = DETECTIONS_ENRICHED
    dead_letters: TopicSpec = STREAM_PROCESSOR_DLQ


@dataclass(frozen=True, slots=True)
class ForecastDecoder:
    """Decodes forecasts; a null value is a tombstone, not an invalid record."""

    decoder: RecordDecoder[WeatherForecast]

    def __call__(
        self, value: bytes | None, key: bytes | None, timestamp: int, headers: Headers | None
    ) -> ForecastUpdate | None:
        if value is None and key is not None:
            return ForecastUpdate(cell=key.decode(), forecast=None)
        forecast = self.decoder(value, key, timestamp, headers)
        return None if forecast is None else ForecastUpdate(cell=forecast.cell, forecast=forecast)


def enrich_step(
    enricher: Enricher,
) -> Callable[[ForecastUpdate | FirmsDetection, State[str, object]], list[EnrichedDetection]]:
    """The Quix stateful step for ``enricher``."""

    def step(
        record: ForecastUpdate | FirmsDetection, state: State[str, object]
    ) -> list[EnrichedDetection]:
        return enricher.process(record, QuixState(state))

    return step


def enriched_key(enriched: EnrichedDetection) -> str:
    return enriched.detection.h3_cell


def build_enrichment_application(
    settings: Settings,
    registry: SubjectRegistry,
    topics: EnrichmentTopics | None = None,
    countries: CountryLocator | None = None,
) -> Application:
    """Create the topics and the Quix application with its pipeline.

    Side effects: creates missing topics and registers the ``EnrichedDetection``
    schema, so an incompatible schema fails here.
    """
    topics = topics or EnrichmentTopics()
    ensure_topics(
        settings.kafka, [topics.detections, topics.forecasts, topics.enriched, topics.dead_letters]
    )
    app = application(settings)
    enriched = output_topic(
        app,
        topics.enriched,
        EventCodec(EnrichedDetection, value_subject(topics.enriched.name), registry),
    )
    dead_letters = DeadLetters(
        cast(RecordProducer, app.get_producer()),
        topic=topics.dead_letters.name,
        group_id=settings.consumer_group,
    )
    detection_decoder = RecordDecoder(
        EventCodec(FirmsDetection, value_subject(topics.detections.name), registry), dead_letters
    )
    forecast_decoder = ForecastDecoder(
        RecordDecoder(
            EventCodec(WeatherForecast, value_subject(topics.forecasts.name), registry),
            dead_letters,
        )
    )
    detections = app.dataframe(app.topic(topics.detections.name, value_deserializer="bytes"))
    detections = detections.apply(detection_decoder, metadata=True).filter(is_present)  # pyright: ignore[reportUnknownMemberType] -- partially typed in Quix
    forecasts = app.dataframe(app.topic(topics.forecasts.name, value_deserializer="bytes"))
    forecasts = forecasts.apply(forecast_decoder, metadata=True).filter(is_present)  # pyright: ignore[reportUnknownMemberType] -- partially typed in Quix
    enricher = Enricher(countries or selected_countries())
    sdf = detections.concat(forecasts)
    sdf = sdf.apply(enrich_step(enricher), stateful=True, expand=True)  # pyright: ignore[reportUnknownMemberType] -- partially typed in Quix
    _ = sdf.to_topic(enriched, key=enriched_key)
    return app
