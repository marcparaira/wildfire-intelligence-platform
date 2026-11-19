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

## Phase 4 — Weather Integration

- Integrate a weather forecast provider and store forecasts.
- Add temperature, precipitation, and wind overlays.
- Provide a forecast timeline.

**Deliverable:** Combined fire and weather monitoring dashboard.

## Phase 5 — Imagery and Operational Features

- Integrate satellite imagery with layer and opacity controls.
- Add historical hotspot views.
- Introduce basic alerts and reporting.

**Deliverable:** First functional MVP.

## Phase 6 — AWS Deployment

- Provision infrastructure using AWS CDK.
- Build and publish production container images.
- Deploy the application and add monitoring and operational documentation.
- Publish a live demo and screenshots.

**Deliverable:** Publicly demonstrable portfolio project.
