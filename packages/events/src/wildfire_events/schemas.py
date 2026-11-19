"""Versioned JSON Schemas for topic values.

The checked-in ``schemas/<Model>.json`` files are the registered contracts. Regenerate
them after a model change with ``python -m wildfire_events.schemas``; review the
diff, and the registry's BACKWARD compatibility check rejects breaking changes.
"""

import json
import sys
from importlib.resources import files

from pydantic import BaseModel

from wildfire_events.models import FirmsDetection, WeatherForecast
from wildfire_events.topics import FIRMS_DETECTIONS, WEATHER_FORECASTS, value_subject

SUBJECT_MODELS: dict[str, type[BaseModel]] = {
    value_subject(FIRMS_DETECTIONS.name): FirmsDetection,
    value_subject(WEATHER_FORECASTS.name): WeatherForecast,
}


def generate(model: type[BaseModel]) -> str:
    return json.dumps(model.model_json_schema(), indent=2, sort_keys=True) + "\n"


def load(model: type[BaseModel]) -> str:
    """The checked-in (registered) JSON Schema for a model."""
    return files("wildfire_events").joinpath(f"schemas/{model.__name__}.json").read_text("utf-8")


def main() -> None:
    root = files("wildfire_events").joinpath("schemas")
    for model in SUBJECT_MODELS.values():
        path = root.joinpath(f"{model.__name__}.json")
        with open(str(path), "w", encoding="utf-8") as handle:
            _ = handle.write(generate(model))
        print(f"wrote {path}", file=sys.stderr)


if __name__ == "__main__":
    main()
