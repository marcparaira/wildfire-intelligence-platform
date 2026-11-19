"""NASA FIRMS NRT active-fire producer."""

import argparse
import csv
import logging
import math
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http.client import HTTPResponse
from pathlib import Path
from typing import cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import urlopen

from pydantic import ValidationError

from wildfire_events.cells import partition_cell
from wildfire_events.models import (
    FIRMS_PRODUCTS,
    FirmsDetection,
    detection_event_id,
    is_firms_product,
)
from wildfire_ingestor.config import MAX_DAY_RANGE, Settings, detection_publisher, read_map_key
from wildfire_ingestor.countries import REGION_BOUNDS, CountryFilter, selected_countries
from wildfire_ingestor.publisher import Publisher

LOGGER = logging.getLogger(__name__)
REQUIRED_FIELDS = {"latitude", "longitude", "acq_date", "acq_time", "satellite"}
FIRMS_AREA_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"


@dataclass
class ParsedCsv:
    detections: list[FirmsDetection] = field(default_factory=list)
    rejected: list[str] = field(default_factory=list)


def _frp(value: str | None) -> float | None:
    try:
        result = float(value) if value not in (None, "") else None
    except ValueError:
        return None
    return result if result is not None and math.isfinite(result) else None


def parse_csv(text: str, product: str, ingested_at: datetime | None = None) -> ParsedCsv:
    """Normalize a FIRMS CSV. Malformed responses raise; malformed rows are rejected."""
    if not is_firms_product(product):
        raise ValueError(f"Unknown FIRMS product {product}")
    ingested_at = ingested_at or datetime.now(UTC)
    reader = csv.DictReader(text.splitlines())
    if not reader.fieldnames or not REQUIRED_FIELDS.issubset(reader.fieldnames):
        raise ValueError("FIRMS response is missing required CSV headers")
    if len(reader.fieldnames) != len(set(reader.fieldnames)):
        raise ValueError("FIRMS response has duplicate CSV headers")
    result = ParsedCsv()
    for line, row in enumerate(reader, start=2):
        try:
            if None in row or any(value is None for value in row.values()):
                raise ValueError("incorrect number of columns")
            time = row["acq_time"]
            if not time.isascii() or not time.isdigit() or len(time) > 4:
                raise ValueError("acquisition time must be HHMM")
            observed_at = datetime.strptime(
                f"{row['acq_date']} {time.zfill(4)}", "%Y-%m-%d %H%M"
            ).replace(tzinfo=UTC)
            latitude, longitude = float(row["latitude"]), float(row["longitude"])
            if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                raise ValueError("coordinates out of range")
            result.detections.append(
                FirmsDetection(
                    event_id=detection_event_id(
                        product, row["satellite"], observed_at, row["latitude"], row["longitude"]
                    ),
                    product=product,
                    instrument=FIRMS_PRODUCTS[product],
                    satellite=row["satellite"],
                    observed_at=observed_at,
                    latitude=latitude,
                    longitude=longitude,
                    h3_cell=partition_cell(latitude, longitude),
                    frp_mw=_frp(row.get("frp")),
                    attributes=row,
                    ingested_at=ingested_at,
                )
            )
        except (ValueError, ValidationError) as exc:
            result.rejected.append(f"line {line}: {exc}")
    return result


def fetch_csv(map_key: str, product: str, day_range: int) -> str:
    if not map_key.strip():
        raise ValueError("FIRMS_MAP_KEY is required for a live FIRMS request")
    if product not in FIRMS_PRODUCTS:
        raise ValueError(f"Unknown FIRMS product {product}")
    if not 1 <= day_range <= MAX_DAY_RANGE:
        raise ValueError(f"FIRMS day range must be between 1 and {MAX_DAY_RANGE}")
    bounds = ",".join(str(int(value)) for value in REGION_BOUNDS)
    url = f"{FIRMS_AREA_URL}/{quote(map_key, safe='')}/{product}/{bounds}/{day_range}"
    try:
        with cast(HTTPResponse, urlopen(url, timeout=120)) as response:
            return response.read().decode("utf-8-sig")
    except HTTPError as exc:
        raise RuntimeError(f"FIRMS request failed with HTTP {exc.code}") from None
    except (URLError, TimeoutError, OSError, UnicodeDecodeError) as exc:
        raise RuntimeError(f"FIRMS request failed: {type(exc).__name__}") from None


def select_countries(
    detections: list[FirmsDetection], countries: CountryFilter | None = None
) -> list[FirmsDetection]:
    west, south, east, north = REGION_BOUNDS
    candidates = [
        item
        for item in detections
        if west <= item.longitude <= east and south <= item.latitude <= north
    ]
    covered = (countries or selected_countries()).covers(
        [(item.latitude, item.longitude) for item in candidates]
    )
    return [item for item, inside in zip(candidates, covered, strict=True) if inside]


@dataclass
class FirmsProducer:
    """One scheduled producer per FIRMS product."""

    product: str
    map_key: str
    day_range: int
    publisher: Publisher[FirmsDetection]

    def run_once(self) -> str:
        return publish_csv(fetch_csv(self.map_key, self.product, self.day_range), self.product,
                           self.publisher)


def publish_csv(text: str, product: str, publisher: Publisher[FirmsDetection]) -> str:
    parsed = parse_csv(text, product)
    for reason in parsed.rejected[:10]:
        LOGGER.warning("%s: rejected FIRMS row %s", product, reason)
    selected = select_countries(parsed.detections)
    published, skipped = publisher.publish(selected)
    return (
        f"{product}: {len(parsed.detections)} rows, {len(parsed.rejected)} rejected, "
        f"{len(selected)} in selected countries, {published} published, "
        f"{skipped} already published"
    )


class Arguments(argparse.Namespace):
    product: str = "VIIRS_NOAA20_NRT"
    days: int = 1
    csv: str | None = None
    key_stdin: bool = False


def main() -> None:
    """One-off publish of a FIRMS product from the API or a local CSV."""
    parser = argparse.ArgumentParser(description="Publish FIRMS detections once")
    _ = parser.add_argument(
        "--product", choices=sorted(FIRMS_PRODUCTS), default="VIIRS_NOAA20_NRT"
    )
    _ = parser.add_argument("--days", type=int, default=1, help="FIRMS day range (1-5)")
    _ = parser.add_argument(
        "--csv", help="Use a local FIRMS CSV, or - for stdin, instead of the API"
    )
    _ = parser.add_argument(
        "--key-stdin", action="store_true", help="Read the map key from stdin"
    )
    args = parser.parse_args(namespace=Arguments())
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        if args.csv and args.key_stdin:
            raise ValueError("--key-stdin cannot be combined with --csv")
        if args.csv == "-":
            text = sys.stdin.read()
        elif args.csv:
            text = Path(args.csv).read_text(encoding="utf-8-sig")
        else:
            key = sys.stdin.readline().strip() if args.key_stdin else read_map_key()
            text = fetch_csv(key, args.product, args.days)
        publisher, producer = detection_publisher(Settings.from_env())
        try:
            LOGGER.info("%s", publish_csv(text, args.product, publisher))
        finally:
            producer.close()
    except (OSError, ValueError, RuntimeError) as exc:
        LOGGER.error("Ingestion failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
