"""The key-value state interface the domain logic is written against."""

from typing import Protocol


class KeyValueState(Protocol):
    """Per-key state store; Quix Streams' ``State`` (via ``QuixState``) implements it.

    Values must be JSON-serializable: Quix persists them in its changelog topic.
    """

    def get(self, key: str, default: object = None) -> object: ...

    def set(self, key: str, value: object) -> None: ...

    def delete(self, key: str) -> None: ...
