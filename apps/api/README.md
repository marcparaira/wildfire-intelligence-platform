# API

FastAPI application package. Keep HTTP routes and API concerns here; independent
data ingestion and stream-processing workers belong under the repository's
`services/` directory.

The `/health` endpoint is an application liveness check. `GET /hotspots`
queries the sink worker's `fire_detections` projection and hides cross-sensor
duplicates unless `include_duplicates=true`. `GET /weather/forecast` returns
one forecast hour over the H3 forecast grid from the `weather_cells` and
`weather_hourly` projections, with the times when the forecasts were
retrieved. The API never calls FIRMS or Open-Meteo; the ingestor publishes
both to Kafka on a schedule.

`GET /events/detections` streams new FIRMS detections with Server-Sent Events,
optionally limited to a bounding box. Each connection reads every partition of
`firms.detections` with its own Kafka consumer (no consumer group), and each
message's ID is the stream position, so a reconnecting browser resumes from its
`Last-Event-ID` header (or the `last_event_id` query parameter). Live items have
a null `id` and are not checked for cross-sensor duplicates until they reach
PostGIS.

Configuration is provided through environment variables: `DATABASE_URL`,
`KAFKA_BOOTSTRAP_SERVERS`, and `SCHEMA_REGISTRY_URL`. The container image is
built from the repository root (`podman build -f apps/api/Containerfile .`)
because it includes the shared `packages/events` package.
