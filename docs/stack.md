# Wildfire Intelligence Platform
**Technology Stack**

**Version:** 0.1
**Status:** Preliminary — Subject to Technical Evaluation

## 1. Overview

This document summarizes the proposed technologies for the Wildfire Intelligence Platform.

Technology selections are preliminary and may be revised following technical evaluations and comparative studies. System architecture will be documented separately.

## 2. Proposed Technology Stack

| Component | Technology | Status |
|---|---|---|
| Frontend | React, TypeScript, Vite | Selected |
| Frontend Container Runtime | Nginx | Selected |
| Geospatial Visualization | OpenLayers | Selected |
| Backend | Python, FastAPI | Proposed |
| Backend Container Runtime | Python on Alpine Linux | Selected |
| Event Streaming | Apache Kafka | Proposed |
| Database | PostgreSQL, PostGIS | Proposed |
| Cloud Provider | Amazon Web Services (AWS) | Selected |
| Infrastructure as Code | AWS CDK (TypeScript) | Selected |
| Object Storage | Amazon S3 | Proposed |
| Containerization | Podman | Selected |


## 3. Containerization

All application services will run in individual Podman containers.

- Separate build and runtime environments.
- Multi-stage container builds.
- Minimal production runtime images.
- Independent service deployment.
- Backend containers will use Python on Alpine Linux.
