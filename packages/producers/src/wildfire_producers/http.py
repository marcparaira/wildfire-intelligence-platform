"""HTTP transport shared by the provider clients.

Clients take an ``HttpGet`` (and ``HttpPost``) so tests can substitute canned
responses. They translate transport failures into ``RuntimeError``, or into
``RetryLater`` when the provider rate-limits them, and return raw payloads;
validating those is the caller's job.
"""

import math
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http.client import HTTPResponse
from typing import Protocol, cast
from urllib.request import Request, urlopen

DEFAULT_RETRY_AFTER_SECONDS = 60
# Enough of an error document to say what went wrong.
ERROR_EXCERPT_CHARS = 300


class HttpGet(Protocol):
    """GET a URL and return the body; raises ``urllib`` errors like ``urlopen``."""

    def __call__(self, url: str, *, timeout: float) -> bytes: ...


def http_get(url: str, *, timeout: float) -> bytes:
    with cast(HTTPResponse, urlopen(url, timeout=timeout)) as response:
        return response.read()


class HttpPost(Protocol):
    """POST a JSON body and return the response body; raises ``urllib`` errors."""

    def __call__(self, url: str, body: bytes, *, timeout: float) -> bytes: ...


def http_post(url: str, body: bytes, *, timeout: float) -> bytes:
    request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with cast(HTTPResponse, urlopen(request, timeout=timeout)) as response:
        return response.read()


def excerpt(content: bytes) -> str:
    """The start of an unexpected response body, for an error message."""
    return content[:ERROR_EXCERPT_CHARS].decode("utf-8", "replace")


def retry_after_seconds(value: str | None) -> int:
    """Seconds to wait from a ``Retry-After`` header (delta seconds or an HTTP date)."""
    if value is None:
        return DEFAULT_RETRY_AFTER_SECONDS
    try:
        seconds = int(value)
    except ValueError:
        return _seconds_until(value)
    return max(1, seconds)


def _seconds_until(http_date: str) -> int:
    try:
        retry_at = parsedate_to_datetime(http_date)
    except (TypeError, ValueError, OverflowError):
        return DEFAULT_RETRY_AFTER_SECONDS
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=UTC)
    return max(1, math.ceil((retry_at - datetime.now(UTC)).total_seconds()))
