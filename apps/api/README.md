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
both to Kafka on a schedule. Each hotspot also carries its `country`, fire
`risk` (score, level, and factor points), and the `weather` at observation
time from the `detection_conditions` projection of `detections.enriched`; they
are null until the stream processor and sink worker have enriched it.

`GET /incidents` lists fire incidents from the `fire_incidents` projection of
`fire.incidents`, most recently active first: active ones (new, growing,
stable) by default, or the repeatable `status` values, optionally inside a
bounding box (by mean position). `peak_risk` is the highest risk among
detections from the incident's last 24 hours of activity inside its bounding
box plus 0.01°, an approximation because detections are not linked to
incidents. `GET /incidents/{incident_id}` adds the daily growth (with the
cumulative footprint) and the next 24 hours of the latest forecast for the
incident's H3 cell. `GET /stats/hourly` returns detections and FRP per country
and UTC hour from `detection_stats_hourly`, for the last 48 hours by default
(`since`, `until`, at most 31 days; repeatable `country`).

`GET /events/detections` streams new FIRMS detections with Server-Sent Events,
optionally limited to a bounding box. Each connection reads every partition of
`firms.detections` with its own Kafka consumer (no consumer group), and each
message's ID is the stream position, so a reconnecting browser resumes from its
`Last-Event-ID` header (or the `last_event_id` query parameter). Live items have
a null `id` and are not checked for cross-sensor duplicates until they reach
PostGIS.

`GET /pipeline/status` reports, per data source, the newest observation (or
forecast retrieval) with a fresh/delayed/stale verdict and ingest counts from
PostGIS, plus topic throughput, consumer lag (one row per consumer group and
topic it reads), and dead-letter counts from Kafka. If PostGIS or Kafka is unreachable, the other half is still
returned and `errors` names the failure. Results are cached for 10 seconds.

Configuration is provided through environment variables: `DATABASE_URL`,
`KAFKA_BOOTSTRAP_SERVERS`, and `SCHEMA_REGISTRY_URL`. The container image is
built from the repository root (`podman build -f apps/api/Containerfile .`)
because it includes the shared `packages/events` package.
