"""Publishing events through the schema-registry codec with an idempotent producer."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from kafka import KafkaProducer
from pydantic import BaseModel

from wildfire_events.codec import EventCodec
from wildfire_ingestor.log import EVENT_ID_HEADER, PublishedEvents


@dataclass
class Publisher[T: BaseModel]:
    producer: KafkaProducer
    codec: EventCodec[T]
    topic: str
    key: Callable[[T], str]
    event_id: Callable[[T], object] | None = None
    published: PublishedEvents | None = None

    def publish(self, events: Iterable[T]) -> tuple[int, int]:
        """Publish events not already in the topic; return (published, skipped)."""
        pending = []
        skipped = 0
        batch_ids: set[str] = set()
        for event in events:
            event_id = None if self.event_id is None else str(self.event_id(event))
            if event_id is not None and (
                event_id in batch_ids or (self.published is not None and event_id in self.published)
            ):
                skipped += 1
                continue
            headers = [] if event_id is None else [(EVENT_ID_HEADER, event_id.encode())]
            future = self.producer.send(
                self.topic,
                key=self.key(event).encode(),
                value=self.codec.encode(event),
                headers=headers,
            )
            if event_id is not None:
                batch_ids.add(event_id)
            pending.append((event_id, future))
        self.producer.flush()
        for event_id, future in pending:
            metadata = future.get(timeout=60)
            if event_id is not None and self.published is not None:
                self.published.add(event_id, metadata.timestamp / 1000)
        return len(pending), skipped
