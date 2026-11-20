"""Minimal client for the Confluent-compatible schema-registry REST API (Karapace)."""

import json
import logging
import time
from http.client import HTTPResponse
from threading import Lock
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from pydantic import BaseModel, TypeAdapter, ValidationError

LOGGER = logging.getLogger(__name__)
CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class RegistryUnavailable(RuntimeError):
    """Transient failure: the registry could not be reached or returned a server error."""


class SchemaRejected(RuntimeError):
    """The registry refused a schema, typically because it breaks compatibility."""


class SubjectRegistry(Protocol):
    """What codecs need from a schema registry; ``SchemaRegistry`` implements it."""

    def register(self, subject: str, schema: str) -> int: ...

    def subjects_for(self, schema_id: int) -> frozenset[str]: ...


class _Registered(BaseModel):
    id: int


class _SubjectVersion(BaseModel):
    subject: str


_REGISTERED = TypeAdapter(_Registered)
_SUBJECT_VERSIONS = TypeAdapter(list[_SubjectVersion])
_ANY_JSON = TypeAdapter(object)


class SchemaRegistry:
    def __init__(self, url: str, timeout: float = 10) -> None:
        self.url: str = url.rstrip("/")
        self.timeout: float = timeout
        self._lock: Lock = Lock()
        self._registered: dict[tuple[str, str], int] = {}
        self._subjects_by_id: dict[int, frozenset[str]] = {}

    def _request[R](
        self, method: str, path: str, response_type: TypeAdapter[R], body: object | None = None
    ) -> R:
        data = None if body is None else json.dumps(body).encode()
        request = Request(
            f"{self.url}{path}",
            data=data,
            method=method,
            headers={"Content-Type": CONTENT_TYPE, "Accept": CONTENT_TYPE},
        )
        try:
            with cast(HTTPResponse, urlopen(request, timeout=self.timeout)) as response:
                payload = response.read() or b"null"
        except HTTPError as exc:
            if exc.code >= 500:
                raise RegistryUnavailable(f"schema registry HTTP {exc.code}") from exc
            raise
        except (URLError, TimeoutError, OSError) as exc:
            raise RegistryUnavailable(f"schema registry unreachable: {exc}") from exc
        try:
            return response_type.validate_json(payload)
        except ValidationError as exc:
            raise RegistryUnavailable(f"unexpected schema registry response: {exc}") from exc

    def register(self, subject: str, schema: str) -> int:
        """Register (or look up) a JSON Schema under a subject and return its global ID."""
        with self._lock:
            if (subject, schema) in self._registered:
                return self._registered[(subject, schema)]
        try:
            result = self._request(
                "POST",
                f"/subjects/{quote(subject, safe='')}/versions",
                _REGISTERED,
                {"schemaType": "JSON", "schema": schema},
            )
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise SchemaRejected(f"{subject}: HTTP {exc.code}: {detail}") from exc
        schema_id = result.id
        with self._lock:
            self._registered[(subject, schema)] = schema_id
        return schema_id

    def subjects_for(self, schema_id: int) -> frozenset[str]:
        """Subjects that a schema ID is registered under (empty if the ID is unknown)."""
        with self._lock:
            if schema_id in self._subjects_by_id:
                return self._subjects_by_id[schema_id]
        try:
            versions = self._request("GET", f"/schemas/ids/{schema_id}/versions", _SUBJECT_VERSIONS)
            subjects = frozenset(version.subject for version in versions)
        except HTTPError as exc:
            if exc.code != 404:
                raise RegistryUnavailable(f"schema registry HTTP {exc.code}") from exc
            return frozenset()
        with self._lock:
            self._subjects_by_id[schema_id] = subjects
        return subjects

    def wait_until_ready(self, attempts: int = 60, delay: float = 5) -> None:
        for attempt in range(1, attempts + 1):
            try:
                _ = self._request("GET", "/subjects", _ANY_JSON)
            except RegistryUnavailable as exc:
                if attempt == attempts:
                    raise
                LOGGER.warning("Schema registry unavailable; retrying in %ss: %s", delay, exc)
                time.sleep(delay)
            else:
                return
