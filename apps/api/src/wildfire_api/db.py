"""Read access to the PostGIS projections maintained by the sink worker."""

import os
from collections.abc import Sequence
from typing import LiteralString

import psycopg
from psycopg.rows import class_row

DEFAULT_DATABASE_URL = "postgresql://wildfire:wildfire_dev@localhost:55432/wildfire"


def connect() -> psycopg.Connection:
    return psycopg.connect(os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL), connect_timeout=5)


def fetch_all[R](
    connection: psycopg.Connection,
    row_type: type[R],
    query: LiteralString,
    parameters: Sequence[object] = (),
) -> list[R]:
    """Run a query and build one ``row_type`` per row from the selected columns."""
    with connection.cursor(row_factory=class_row(row_type)) as cursor:
        return cursor.execute(query, parameters).fetchall()
