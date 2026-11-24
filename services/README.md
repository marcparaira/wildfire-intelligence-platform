# Data services

Each service is an independently running container that talks to the rest of
the system through Kafka. Event contracts (topics, H3 keys, models, registered
JSON Schemas, and the wire-format codec) live in `packages/events` and are
shared by every service. The two scheduled producer services also share
`packages/producers` (the scheduler, the deduplicating publisher, reading state
back from Kafka, HTTP helpers, and settings). Build service images from the
repository root:

```sh
podman build -f services/ingestor/Containerfile .
```

- `ingestor/` runs one scheduled, idempotent producer per external source:
  each FIRMS NRT product and Open-Meteo. It reads only external APIs and Kafka,
  and feeds the live map, so it stays small and is rarely restarted.
- `imagery-worker/` acquires satellite imagery of current fire incidents:
  Sentinel-2 passes from Microsoft Planetary Computer.
  It is downstream of the stream processor (it reads `fire.incidents`), writes
  the images to the imagery store volume, and publishes `imagery.scenes`. Its
  runs take minutes and use image processing (Pillow), so it runs apart from
  the ingestor.
- `sink-worker/` projects topics into PostGIS, with one consumer group per
  job, and routes invalid events to `sink-worker.dlq`.
- `stream-processor/` runs stateful Quix Streams jobs, one process each: it
  clusters `firms.detections` into `fire.incidents`, enriches detections with
  country, weather, and fire risk into `detections.enriched`, and aggregates
  hourly per-country statistics into `stats.hourly`. Invalid events go to
  `stream-processor.dlq`.

Keep HTTP API concerns in `apps/api`. See
[the event pipeline operations guide](../docs/operations/event-pipeline.md).
