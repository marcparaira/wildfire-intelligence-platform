"""Versioned JSON Schemas for topic values.

The checked-in ``schemas/<Model>.json`` files are the registered contracts. Regenerate
them after a model change with ``python -m wildfire_events.schemas``; review the
diff, and the registry's BACKWARD compatibility check rejects breaking changes.
"""

import json
import logging
from importlib.resources import files
from pathlib import Path

from pydantic import BaseModel

from wildfire_events.models import (
    EnrichedDetection,
    FireIncident,
    FirmsDetection,
    HourlyDetectionStats,
    SatelliteScene,
    WeatherForecast,
)
from wildfire_events.topics import (
    DETECTIONS_ENRICHED,
    FIRE_INCIDENTS,
    FIRMS_DETECTIONS,
    SATELLITE_SCENES,
    STATS_HOURLY,
    WEATHER_FORECASTS,
    value_subject,
)

LOGGER = logging.getLogger(__name__)

SUBJECT_MODELS: dict[str, type[BaseModel]] = {
    value_subject(FIRMS_DETECTIONS.name): FirmsDetection,
    value_subject(WEATHER_FORECASTS.name): WeatherForecast,
    value_subject(FIRE_INCIDENTS.name): FireIncident,
    value_subject(DETECTIONS_ENRICHED.name): EnrichedDetection,
    value_subject(STATS_HOURLY.name): HourlyDetectionStats,
    value_subject(SATELLITE_SCENES.name): SatelliteScene,
}


def generate(model: type[BaseModel]) -> str:
    return json.dumps(model.model_json_schema(), indent=2, sort_keys=True) + "\n"


def load(model: type[BaseModel]) -> str:
    """The checked-in (registered) JSON Schema for a model."""
    return files("wildfire_events").joinpath(f"schemas/{model.__name__}.json").read_text("utf-8")


def main() -> None:
    """Regenerate the checked-in schemas; only meaningful from an editable install."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    root = Path(str(files("wildfire_events").joinpath("schemas")))
    for model in SUBJECT_MODELS.values():
        path = root / f"{model.__name__}.json"
        _ = path.write_text(generate(model), encoding="utf-8")
        LOGGER.info("wrote %s", path)


if __name__ == "__main__":
    main()
