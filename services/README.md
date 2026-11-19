# Data services

Each service is an independently running container that talks to the rest of
the system through Kafka. Event contracts (topics, H3 keys, models, registered
JSON Schemas, and the wire-format codec) live in `packages/events` and are
shared by every service, so build service images from the repository root:

```sh
podman build -f services/ingestor/Containerfile .
```

- `ingestor/` runs one scheduled, idempotent producer per external source:
  each FIRMS NRT product and Open-Meteo. It reads only external APIs and Kafka.
- `sink-worker/` projects topics into PostGIS, with one consumer group per
  job, and routes invalid events to `sink-worker.dlq`.

Keep HTTP API concerns in `apps/api`. See
[the event pipeline operations guide](../docs/operations/event-pipeline.md).
