"""Read access to the PostGIS projections maintained by the sink worker."""

from collections.abc import Sequence
from typing import LiteralString

import psycopg
from psycopg.rows import class_row

CONNECT_TIMEOUT_SECONDS = 5


def connect(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url, connect_timeout=CONNECT_TIMEOUT_SECONDS)


def fetch_all[R](
    connection: psycopg.Connection,
    row_type: type[R],
    query: LiteralString,
    parameters: Sequence[object] = (),
) -> list[R]:
    """Run a query and build one ``row_type`` per row from the selected columns."""
    with connection.cursor(row_factory=class_row(row_type)) as cursor:
        return cursor.execute(query, parameters).fetchall()
