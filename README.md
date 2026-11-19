# Wildfire Intelligence Platform

The repository is organized so the user-facing applications, data services,
infrastructure, and project documentation can grow independently:

```text
apps/
  api/                 FastAPI application
  web/                 React + TypeScript + Vite application
infra/
  compose/              Local PostgreSQL/PostGIS initialization
services/               Reserved for independent ingestion and processing workers
docs/                    Requirements, architecture, decisions, and roadmap
```

## Prerequisites

- Podman with `podman compose` support (or Docker Compose)
- Node.js 22+ and npm (for running frontend checks outside containers)
- Python 3.12+ (for running backend checks outside containers)

## Start the complete local environment

From the repository root, run:

```sh
podman compose up --build
```

Compose builds the web and API images, initializes PostgreSQL with PostGIS,
starts Kafka in single-node KRaft mode, and defines health checks for each
service.
The web application is available at <http://localhost:8080>; the API docs are
at <http://localhost:8000/docs>. Database and Kafka are published at ports
55432 and 19092 by default to avoid common host conflicts. Override these with
the `POSTGRES_PORT`, `KAFKA_PORT`, `API_PORT`, or `WEB_PORT` environment
variables. Stop the services with `Ctrl+C`; remove
containers and the local database volume with `podman compose down -v`.

The local database uses development-only credentials from `compose.yaml`.
Do not use these defaults outside a local development environment.

## Run checks locally

```sh
cd apps/web
npm ci
npm run lint
npm test
npm run build

cd ../api
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
ruff check .
pytest
```

To run an app against the local dependencies without building its container,
start the services with `podman compose up -d db kafka`, then run:

```sh
# Terminal 1
cd apps/api
. .venv/bin/activate
uvicorn wildfire_api.main:app --reload

# Terminal 2
cd apps/web
npm run dev
```

The Vite development server proxies `/api` requests to the API on port 8000.

## Local service endpoints

| Service | Address |
|---|---|
| Web | <http://localhost:8080> |
| API / OpenAPI docs | <http://localhost:8000/docs> |
| API health | <http://localhost:8000/health> |
| API health through the web proxy | <http://localhost:8080/api/health> |
| PostgreSQL/PostGIS | `localhost:55432` |
| Kafka | `localhost:19092` |
