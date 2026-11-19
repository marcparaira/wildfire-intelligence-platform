"""Confluent wire format for JSON Schema events.

A value is ``0x00`` + a 4-byte big-endian schema ID + UTF-8 JSON. Producers register
the checked-in schema for the topic's subject; consumers reject values whose schema
ID is not registered under the expected subject or whose JSON fails validation.
"""

import struct
from typing import cast

from pydantic import BaseModel, ValidationError

from wildfire_events import schemas
from wildfire_events.registry import SubjectRegistry

MAGIC_BYTE = 0
HEADER = struct.Struct(">bI")


class InvalidEvent(ValueError):
    """A record that can never be processed; consumers dead-letter it and continue."""


class EventCodec[T: BaseModel]:
    def __init__(self, model: type[T], subject: str, registry: SubjectRegistry) -> None:
        self.model: type[T] = model
        self.subject: str = subject
        self.registry: SubjectRegistry = registry
        self._schema_id: int | None = None

    @property
    def schema_id(self) -> int:
        if self._schema_id is None:
            self._schema_id = self.registry.register(self.subject, schemas.load(self.model))
        return self._schema_id

    def encode(self, event: T) -> bytes:
        if not isinstance(event, self.model):
            raise TypeError(f"expected {self.model.__name__}, got {type(event).__name__}")
        return HEADER.pack(MAGIC_BYTE, self.schema_id) + event.model_dump_json().encode()

    def decode(self, value: bytes | None) -> T:
        if value is None:
            raise InvalidEvent("record has no value")
        if len(value) <= HEADER.size:
            raise InvalidEvent("record is too short for the schema-registry wire format")
        magic, schema_id = cast(tuple[int, int], HEADER.unpack_from(value))
        if magic != MAGIC_BYTE:
            raise InvalidEvent(f"unknown wire-format magic byte {magic}")
        if self.subject not in self.registry.subjects_for(schema_id):
            raise InvalidEvent(f"schema ID {schema_id} is not registered for {self.subject}")
        try:
            return self.model.model_validate_json(value[HEADER.size :])
        except ValidationError as exc:
            raise InvalidEvent(
                f"value does not match {self.model.__name__}: "
                + "; ".join(
                    f"{'.'.join(map(str, err['loc'])) or '<root>'}: {err['msg']}"
                    for err in exc.errors()[:5]
                )
            ) from exc
