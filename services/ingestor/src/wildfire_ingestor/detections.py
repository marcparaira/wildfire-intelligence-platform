"""FIRMS active-fire detections: CSV normalization, country selection, and publishing."""

import csv
import logging
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from pydantic import ValidationError

from wildfire_events.cells import partition_cell
from wildfire_events.countries import REGION_BOUNDS, CountryLocator
from wildfire_events.models import FirmsDetection, FirmsProduct, detection_event_id
from wildfire_producers.publisher import Publisher

LOGGER = logging.getLogger(__name__)
REQUIRED_FIELDS = frozenset({"latitude", "longitude", "acq_date", "acq_time", "satellite"})
MAX_LOGGED_REJECTIONS = 10
FIRST_DATA_LINE = 2  # Line 1 is the CSV header.
ACQUISITION_TIME_DIGITS = 4  # HHMM, with leading zeros sometimes dropped.


@dataclass(slots=True)
class ParsedCsv:
    detections: list[FirmsDetection] = field(default_factory=list)
    rejected: list[str] = field(default_factory=list)


class FirmsCsvSource(Protocol):
    """FIRMS CSV for a product; ``FirmsClient`` implements it."""

    def fetch_csv(self, product: FirmsProduct, day_range: int) -> str: ...


def _frp(value: str | None) -> float | None:
    if not value:
        return None
    try:
        result = float(value)
    except ValueError:
        return None
    return result if math.isfinite(result) else None


def _observed_at(date: str, time: str) -> datetime:
    if not time.isascii() or not time.isdigit() or len(time) > ACQUISITION_TIME_DIGITS:
        raise ValueError("acquisition time must be HHMM")
    padded = time.zfill(ACQUISITION_TIME_DIGITS)
    return datetime.strptime(f"{date} {padded}", "%Y-%m-%d %H%M").replace(tzinfo=UTC)


def _parse_row(row: dict[str, str], product: FirmsProduct, ingested_at: datetime) -> FirmsDetection:
    """One CSV row as a detection; raises ``ValueError`` for a malformed row."""
    observed_at = _observed_at(row["acq_date"], row["acq_time"])
    latitude, longitude = float(row["latitude"]), float(row["longitude"])
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        raise ValueError("coordinates out of range")
    return FirmsDetection(
        event_id=detection_event_id(
            product, row["satellite"], observed_at, row["latitude"], row["longitude"]
        ),
        product=product,
        instrument=product.instrument,
        satellite=row["satellite"],
        observed_at=observed_at,
        latitude=latitude,
        longitude=longitude,
        h3_cell=partition_cell(latitude, longitude),
        frp_mw=_frp(row.get("frp")),
        attributes=row,
        ingested_at=ingested_at,
    )


def parse_csv(text: str, product: FirmsProduct, ingested_at: datetime | None = None) -> ParsedCsv:
    """Normalize a FIRMS CSV. Malformed responses raise; malformed rows are rejected."""
    ingested_at = ingested_at or datetime.now(UTC)
    reader = csv.DictReader(text.splitlines())
    if not reader.fieldnames or not REQUIRED_FIELDS.issubset(reader.fieldnames):
        raise ValueError("FIRMS response is missing required CSV headers")
    if len(reader.fieldnames) != len(set(reader.fieldnames)):
        raise ValueError("FIRMS response has duplicate CSV headers")
    result = ParsedCsv()
    for line, row in enumerate(reader, start=FIRST_DATA_LINE):
        # DictReader puts extra values under None and fills missing ones with None.
        if None in row or any(value is None for value in row.values()):
            result.rejected.append(f"line {line}: incorrect number of columns")
            continue
        try:
            result.detections.append(_parse_row(row, product, ingested_at))
        except (ValueError, ValidationError) as exc:
            result.rejected.append(f"line {line}: {exc}")
    return result


def select_countries(
    detections: list[FirmsDetection], countries: CountryLocator
) -> list[FirmsDetection]:
    """Detections inside the selected countries."""
    west, south, east, north = REGION_BOUNDS
    candidates = [
        item
        for item in detections
        if west <= item.longitude <= east and south <= item.latitude <= north
    ]
    covered = countries.covers([(item.latitude, item.longitude) for item in candidates])
    return [item for item, inside in zip(candidates, covered, strict=True) if inside]


def publish_csv(
    text: str,
    product: FirmsProduct,
    publisher: Publisher[FirmsDetection],
    countries: CountryLocator,
) -> str:
    """Publish a FIRMS CSV's detections in the selected countries; return a summary."""
    parsed = parse_csv(text, product)
    for reason in parsed.rejected[:MAX_LOGGED_REJECTIONS]:
        LOGGER.warning("%s: rejected FIRMS row %s", product, reason)
    selected = select_countries(parsed.detections, countries)
    published, skipped = publisher.publish(selected)
    return (
        f"{product}: {len(parsed.detections)} rows, {len(parsed.rejected)} rejected, "
        f"{len(selected)} in selected countries, {published} published, "
        f"{skipped} already published"
    )


@dataclass(frozen=True, slots=True)
class FirmsProducer:
    """One scheduled producer per FIRMS product."""

    product: FirmsProduct
    day_range: int
    source: FirmsCsvSource
    publisher: Publisher[FirmsDetection]
    countries: CountryLocator

    def run_once(self) -> str:
        text = self.source.fetch_csv(self.product, self.day_range)
        return publish_csv(text, self.product, self.publisher, self.countries)
