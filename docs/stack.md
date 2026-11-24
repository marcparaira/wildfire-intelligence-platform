# Wildfire Intelligence Platform
**Technology Stack**

**Version:** 0.2
**Status:** Phase 6 streaming backbone established; remaining decisions are subject to evaluation

## 1. Overview

This document summarizes the selected foundation and proposed technologies for
the Wildfire Intelligence Platform.

Selected Phase 1 technologies are implemented in the repository. Other
selections may be revised following technical evaluations and comparative
studies. System architecture is documented separately.

## 2. Proposed Technology Stack

| Component | Technology | Status |
|---|---|---|
| Frontend | React, TypeScript, Vite | Selected |
| Frontend Container Runtime | Nginx | Selected |
| Geospatial Visualization | OpenLayers | Selected |
| Backend | Python, FastAPI | Selected |
| Backend Container Runtime | Python on Alpine Linux | Selected |
| Event Streaming | Apache Kafka | Selected |
| Stream Processing | Quix Streams (Python) | Proposed |
| Schema Registry | Karapace (Confluent-compatible API, JSON Schema) | Selected |
| Kafka Web UI | Kafbat Kafka UI | Selected |
| Kafka Archiving | Python archiver in the sink worker | Proposed |
| Spatial Indexing | H3 (resolution-2 Kafka keys and weather grid) | Selected |
| Browser Push | Server-Sent Events | Proposed |
| Database | PostgreSQL, PostGIS | Selected |
| Cloud Provider | Amazon Web Services (AWS) | Selected |
| Infrastructure as Code | AWS CDK (TypeScript) | Selected |
| Object Storage | Amazon S3 | Proposed |
| Containerization | Podman | Selected |


## 3. Containerization

The local environment is defined in the repository-root `compose.yaml`; run
`podman compose up --build` to start the web application, API, ingestor,
imagery worker, sink worker, PostgreSQL/PostGIS, Kafka, the Karapace schema registry, and Kafka UI.

Application container definitions live alongside their source in `apps/web`,
`apps/api`, and `services/*`. The Python services are built from the repository
root so they can include the shared `packages/events` contracts and, for the ingestor and imagery worker, the `packages/producers` runtime. All
application services run in individual Podman containers.

- Separate build and runtime environments.
- Multi-stage container builds.
- Minimal production runtime images.
- Independent service deployment.
- Backend containers will use Python on Alpine Linux.
