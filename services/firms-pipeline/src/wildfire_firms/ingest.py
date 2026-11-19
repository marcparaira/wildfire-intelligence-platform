import argparse
import csv
import json
import logging
import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import urlopen

import psycopg
from kafka import KafkaProducer
from psycopg.types.json import Jsonb

from wildfire_firms.events import SOURCE, TOPIC, Detection

LOGGER = logging.getLogger(__name__)
REGION_BOUNDS = (92, -12, 156, 29)
REQUIRED_FIELDS = {"latitude", "longitude", "acq_date", "acq_time", "satellite"}
FIRMS_AREA_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"


def parse_csv(text: str) -> Iterator[Detection]:
    reader = csv.DictReader(text.splitlines())
    if not reader.fieldnames or not REQUIRED_FIELDS.issubset(reader.fieldnames):
        raise ValueError("FIRMS response is missing required CSV headers")
    if len(reader.fieldnames) != len(set(reader.fieldnames)):
        raise ValueError("FIRMS response has duplicate CSV headers")
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
            yield Detection.model_validate(
                {
                    "schema_version": 1,
                    "source": SOURCE,
                    "satellite": row["satellite"],
                    "observed_at": observed_at,
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                    "attributes": row,
                }
            )
        except ValueError as exc:
            raise ValueError(f"Invalid FIRMS detection on CSV line {line}: {exc}") from exc


def fetch_csv(map_key: str, day_range: int) -> str:
    if not map_key.strip():
        raise ValueError("FIRMS_MAP_KEY is required for a live FIRMS request")
    if not 1 <= day_range <= 5:
        raise ValueError("FIRMS day range must be between 1 and 5")
    bounds = ",".join(str(value) for value in REGION_BOUNDS)
    url = f"{FIRMS_AREA_URL}/{quote(map_key, safe='')}/{SOURCE}/{bounds}/{day_range}"
    try:
        with urlopen(url, timeout=60) as response:
            return response.read().decode("utf-8-sig")
    except HTTPError as exc:
        raise RuntimeError(f"FIRMS request failed with HTTP {exc.code}") from None
    except (URLError, TimeoutError, UnicodeDecodeError) as exc:
        raise RuntimeError(f"FIRMS request failed: {type(exc).__name__}") from None


def filter_countries(detections: list[Detection], database_url: str) -> list[Detection]:
    west, south, east, north = REGION_BOUNDS
    candidates = [
        detection
        for detection in detections
        if west <= detection.longitude <= east and south <= detection.latitude <= north
    ]
    if not candidates:
        return []

    boundary_data = json.loads(
        files("wildfire_firms").joinpath("data/countries.geojson").read_text(encoding="utf-8")
    )
    with psycopg.connect(database_url) as connection:
        connection.execute("CREATE TEMP TABLE country_parts (geom geometry(Geometry, 4326))")
        connection.execute(
            """
            INSERT INTO country_parts (geom)
            SELECT ST_Subdivide((ST_Dump(ST_SetSRID(
                ST_GeomFromGeoJSON(feature->'geometry'), 4326
            ))).geom, 256)
            FROM jsonb_array_elements(%s::jsonb->'features') AS feature
            """,
            (Jsonb(boundary_data),),
        )
        connection.execute("CREATE INDEX ON country_parts USING gist (geom)")
        connection.execute("ANALYZE country_parts")
        rows = connection.execute(
            """
            SELECT point.ordinality
            FROM unnest(%s::float8[], %s::float8[]) WITH ORDINALITY AS point(lon, lat, ordinality)
            WHERE EXISTS (
                SELECT 1 FROM country_parts
                WHERE ST_Covers(geom, ST_SetSRID(ST_Point(point.lon, point.lat), 4326))
            )
            ORDER BY point.ordinality
            """,
            (
                [float(item.longitude) for item in candidates],
                [float(item.latitude) for item in candidates],
            ),
        ).fetchall()
    return [candidates[ordinality - 1] for (ordinality,) in rows]


def publish(detections: list[Detection], bootstrap_servers: str, topic: str = TOPIC) -> int:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        request_timeout_ms=30000,
    )
    try:
        for detection in detections:
            producer.send(topic, detection.model_dump_json().encode("utf-8")).get(timeout=60)
    finally:
        producer.close()
    return len(detections)


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish FIRMS detections for Southeast Asia")
    parser.add_argument("--days", type=int, default=1, help="FIRMS day range (1-5)")
    parser.add_argument("--csv", help="Use a local FIRMS CSV, or - for stdin, instead of the API")
    parser.add_argument(
        "--key-stdin", action="store_true", help="Read the FIRMS map key from stdin"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.csv and args.key_stdin:
            raise ValueError("--key-stdin cannot be combined with --csv")
        if args.csv == "-":
            text = sys.stdin.read()
        elif args.csv:
            text = Path(args.csv).read_text(encoding="utf-8-sig")
        else:
            key = (
                sys.stdin.readline().strip()
                if args.key_stdin
                else os.environ.get("FIRMS_MAP_KEY", "")
            )
            text = fetch_csv(key, args.days)
        detections = list(parse_csv(text))
        database_url = os.environ.get(
            "DATABASE_URL", "postgresql://wildfire:wildfire_dev@localhost:55432/wildfire"
        )
        selected = filter_countries(detections, database_url)
        count = publish(selected, os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
        LOGGER.info(
            "Published %d of %d FIRMS detections within selected countries to %s",
            count,
            len(detections),
            TOPIC,
        )
    except (OSError, ValueError, RuntimeError, psycopg.Error) as exc:
        LOGGER.error("Ingestion failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
