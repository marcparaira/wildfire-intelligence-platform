"""One-off FIRMS publish: ``python -m wildfire_ingestor.firms``.

Publishes one FIRMS product from the API or from a local CSV, skipping detections
already in firms.detections. The scheduled service lives in ``service``.
"""

import argparse
import logging
import sys
from pathlib import Path

from wildfire_events.countries import selected_countries
from wildfire_events.kafka import idempotent_producer
from wildfire_events.models import FirmsProduct
from wildfire_ingestor.config import (
    PRODUCER_CLIENT_ID,
    Settings,
    configure_logging,
    connect_registry,
    detection_publisher,
    load_published_detections,
    read_map_key,
)
from wildfire_ingestor.detections import publish_csv
from wildfire_ingestor.sources import FirmsClient

LOGGER = logging.getLogger(__name__)
STDIN = "-"


class Arguments(argparse.Namespace):
    product: FirmsProduct = FirmsProduct.VIIRS_NOAA20_NRT
    days: int = 1
    csv: str | None = None
    key_stdin: bool = False


def _parse_arguments() -> Arguments:
    parser = argparse.ArgumentParser(description="Publish FIRMS detections once")
    _ = parser.add_argument(
        "--product", type=FirmsProduct, choices=list(FirmsProduct), default=Arguments.product
    )
    _ = parser.add_argument("--days", type=int, default=1, help="FIRMS day range (1-5)")
    _ = parser.add_argument(
        "--csv", help="Use a local FIRMS CSV, or - for stdin, instead of the API"
    )
    _ = parser.add_argument("--key-stdin", action="store_true", help="Read the map key from stdin")
    return parser.parse_args(namespace=Arguments())


def _read_csv(args: Arguments) -> str:
    if args.csv and args.key_stdin:
        raise ValueError("--key-stdin cannot be combined with --csv")
    if args.csv == STDIN:
        return sys.stdin.read()
    if args.csv:
        return Path(args.csv).read_text(encoding="utf-8-sig")
    key = sys.stdin.readline().strip() if args.key_stdin else read_map_key()
    return FirmsClient(key).fetch_csv(args.product, args.days)


def _publish(text: str, product: FirmsProduct) -> str:
    settings = Settings.from_env()
    published = load_published_detections(settings)
    producer = idempotent_producer(settings.kafka, PRODUCER_CLIENT_ID)
    try:
        publisher = detection_publisher(producer, connect_registry(settings), published)
        return publish_csv(text, product, publisher, selected_countries())
    finally:
        producer.close()


def main() -> None:
    args = _parse_arguments()
    configure_logging()
    try:
        LOGGER.info("%s", _publish(_read_csv(args), args.product))
    # Expected operator errors (bad flags, files, settings, provider failures) are
    # reported as one line without a traceback.
    except (OSError, ValueError, RuntimeError) as exc:
        LOGGER.error("Ingestion failed: %s", exc)  # noqa: TRY400 -- no traceback for operator errors
        sys.exit(1)


if __name__ == "__main__":
    main()
