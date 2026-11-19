# Development Roadmap

## Phase 1 — Project Foundation

- [x] Scaffold React/Vite and Python/FastAPI applications.
- [x] Create multi-stage Podman Containerfiles with separate build and runtime stages.
- [x] Configure PostgreSQL/PostGIS and Kafka locally.
- [x] Set up linting, tests, and GitHub Actions CI.
- [x] Document local development commands.

**Deliverable:** One-command local environment with healthy services.

## Phase 2 — FIRMS Ingestion Vertical Slice

**Data flow:** NASA FIRMS → Python Ingestor → Kafka → Processor → PostGIS

- [x] Ingest FIRMS detections for one pilot region.
- [x] Publish normalized Kafka events.
- [x] Validate and persist detections as geospatial points.
- [x] Deduplicate records and preserve source timestamps.

**Deliverable:** Tested, repeatable satellite-data ingestion pipeline.

## Phase 3 — First Functional Dashboard

- [x] Build React dashboard with OpenLayers.
- [x] Expose hotspot queries via FastAPI.
- [x] Display hotspots on a map with geographic and date filters.
- [x] Show details for selected detections.

**Deliverable:** Working wildfire hotspot dashboard.

## Phase 4 — FIRMS for Southeast Asia

- [x] Expand FIRMS ingestion to the selected Southeast Asian countries and Papua New Guinea.
- [x] Store and display the regional detections.

**Deliverable:** Country-filtered regional satellite-data coverage.

## Phase 5 — Weather Integration

- [x] Integrate Open-Meteo forecasts and store hourly weather in PostGIS.
- [x] Add temperature, precipitation, and wind-speed/direction map overlays.
- [x] Provide a seven-day hourly forecast timeline.

**Deliverable:** Combined fire and weather monitoring dashboard.

## Phase 6 — Streaming Backbone

Make Kafka the system of record: every observation, forecast, and user action
is an event, and storage, alerts, and the live map are projections of the log.

- [x] Replace on-demand ingestion with scheduled producers for all FIRMS NRT
  products (VIIRS SNPP, NOAA-20, NOAA-21, and MODIS).
- [x] Publish Open-Meteo forecasts to Kafka on a schedule; keep the latest forecast
  per grid cell in a compacted topic instead of fetching at request time.
- [x] Key events by H3 cell so spatially related data shares a partition.
- [x] Add a schema registry (Apicurio or Karapace) and register event schemas.
- [x] Route invalid events to dead-letter topics instead of stopping consumers.
- [x] Enable idempotent producers and deduplicate detections across sensors.
- [x] Add a Kafka web UI to the local environment.
- [x] Document rebuilding PostGIS by replaying topics from the beginning.

**Deliverable:** Continuous, schema-governed, replayable multi-source pipeline.

## Phase 7 — Real-Time Dashboard

- Add an API consumer that pushes new detections and alerts to the browser
  using Server-Sent Events.
- Update the map live, without manual refresh.
- Show observation and forecast freshness for each data source.
- Add a pipeline status page with per-source freshness, throughput, consumer
  lag, and dead-letter counts.

**Deliverable:** Live dashboard with visible pipeline health.

## Phase 8 — Stream Processing

Use a Python stream-processing library (Quix Streams) for stateful and
windowed processing.

- Cluster detections into fire incidents by space and time, and track each
  incident's lifecycle (new, growing, stable, inactive) in `fire.incidents`.
- Enrich detections with the latest weather for their grid cell.
- Compute a simple, documented risk indicator from fire activity and weather.
- Produce hourly per-country detection and FRP statistics.
- Display incidents, their growth over time, and weather context in the
  dashboard.

**Deliverable:** Incident-level situational awareness derived from the stream.

## Phase 9 — Watch Areas, Alerts, and Field Reports

- Let users draw watch areas, stored as events in a compacted topic.
- Match incidents and risk conditions against watch areas and publish alerts.
- Deliver alerts to the browser and to at least one external channel (email or
  webhook), each as an independent consumer group.
- Let responders confirm or dismiss detections with field reports, and record
  alert acknowledgements as events.

**Deliverable:** Event-driven alerting and two-way operational workflow.

## Phase 10 — Air Quality, History, and Archive

- Ingest air-quality data (Open-Meteo Air Quality and OpenAQ) and add a PM2.5
  haze overlay.
- Archive topics to object storage from the sink worker.
- Add historical views and a time slider that replays the event history.
- Export selected datasets and incident summaries.
- Add a clearly labeled simulation producer that replays archived detections
  at accelerated speed for demonstrations.
- Stretch: ingest the JAXA Himawari-9 wildfire product (10-minute
  geostationary detections).
- Side task: add a satellite imagery basemap (for example, NASA GIBS) with
  layer and opacity controls.

**Deliverable:** First complete MVP.

## Phase 11 — AWS Deployment

- Provision infrastructure using AWS CDK.
- Choose a cost-appropriate Kafka deployment (Amazon MSK Serverless or a
  self-managed single broker) and document the trade-off.
- Build and publish production container images.
- Deploy the application and add monitoring, including consumer lag, and
  operational documentation.
- Publish a live demo and screenshots.

**Deliverable:** Publicly demonstrable portfolio project.
