# Wildfire Intelligence Platform

A live map of satellite fire detections and weather forecasts for Southeast
Asia and Papua New Guinea, built on a Kafka event pipeline.

![Wildfire hotspot dashboard](docs/architecture/dashboard-snapshot.png)

## Architecture

Kafka is the system of record. The ingestor publishes every observation and
forecast as a schema-validated event. The sink worker projects the topics into
PostGIS, and the projections can be rebuilt by replaying the log. The API
serves queries from PostGIS and streams new detections from Kafka to the
browser.

```mermaid
flowchart LR
    SRC["NASA FIRMS<br/>Open-Meteo"]
    ING["Ingestor<br/>Scheduled producers"]

    subgraph KAFKA["Kafka + schema registry"]
        direction TB
        T_SRC["firms.detections<br/>weather.forecasts ⟲"]
        T_DLQ["sink-worker.dlq"]
    end

    SINK["Sink worker<br/>PostGIS projections"]
    DB[("PostgreSQL<br/>+ PostGIS")]
    API["FastAPI<br/>REST + SSE"]
    WEB["React + OpenLayers<br/>dashboard"]

    SRC -->|HTTP polling| ING -->|produce| T_SRC
    T_SRC -->|consume| SINK
    SINK -.->|invalid events| T_DLQ
    SINK -->|upsert| DB
    API -->|queries| DB
    T_SRC -->|live detections| API
    WEB <-->|HTTPS + SSE| API

    classDef external fill:#e0f2fe,stroke:#0284c7,color:#075985
    classDef streaming fill:#fef3c7,stroke:#d97706,color:#92400e
    classDef dlq fill:#fee2e2,stroke:#dc2626,color:#991b1b
    classDef data fill:#dcfce7,stroke:#16a34a,color:#166534

    class SRC external
    class T_SRC streaming
    class T_DLQ dlq
    class DB data
```

⟲ marks a compacted topic. Arrows show the direction data flows.

The [target architecture](docs/architecture/high-level-architecture.md) adds
stream processing, watch areas, and alerting; see the [roadmap](docs/roadmap.md).
The [event pipeline guide](docs/operations/event-pipeline.md) covers topics,
schemas, dead letters, and replaying the log.

```text
apps/api/              FastAPI: PostGIS queries and the live SSE stream
apps/web/              React + TypeScript + OpenLayers dashboard
packages/events/       Shared topics, H3 keys, event models, schemas, and codec
services/ingestor/     Scheduled FIRMS and Open-Meteo producers
services/sink-worker/  Kafka-to-PostGIS projections and dead-letter routing
infra/compose/         Local PostgreSQL/PostGIS initialization
docs/                  Requirements, architecture, operations, and roadmap
```

## Quick start

Requires Podman with `podman compose` (or Docker Compose).

1. Optional: get a free [NASA FIRMS MAP_KEY](https://firms.modaps.eosdis.nasa.gov/api/area/)
   and save it in `.firms-map-key` (`chmod 600`; the file is Git-ignored, never
   commit it). Without a key, only weather forecasts are ingested.
2. Start the stack:

   ```sh
   export FIRMS_MAP_KEY="$(cat .firms-map-key)"
   podman compose up --build
   ```

3. Open the dashboard at <http://localhost:8080>.

| Service | Address |
|---|---|
| Dashboard | <http://localhost:8080> |
| API docs | <http://localhost:8000/docs> |
| Kafka UI | <http://localhost:8090> |
| Schema registry | <http://localhost:18081/subjects> |
| PostgreSQL / Kafka | `localhost:55432` / `localhost:19092` |

Override ports with `WEB_PORT`, `API_PORT`, `KAFKA_UI_PORT`,
`SCHEMA_REGISTRY_PORT`, `POSTGRES_PORT`, and `KAFKA_PORT`. Credentials in
`compose.yaml` are for local development only.

`podman compose down -v` deletes the Kafka volume, and with it the event
history, not just a cache. podman-compose 1.0.6 does not recreate containers
when `compose.yaml` changes: run `podman compose down` (without `-v`), then
`up` again.

## Dashboard

- **Hotspots** come from FIRMS VIIRS (SNPP, NOAA-20, NOAA-21) and MODIS
  near-real-time products. Filter by area and UTC date range; select a point
  for its details.
- **Live updates:** new detections appear as soon as they are published; the
  indicator above the map shows the connection state. See
  [known limitations](docs/limitations.md).
- **Weather layer:** temperature, precipitation, or wind from Open-Meteo's
  seven-day hourly forecast, on a grid of resolution-2 H3 cells (about
  87,000 km² each). Use the timeline to change the forecast hour.
- **Refresh data** re-queries the database; it does not trigger ingestion.

Marker size and color show fire radiative power (FRP) on a log scale capped at
100 MW. FRP is radiated power, not fire severity or danger, and detections are
satellite pixel centers, not fire perimeters. The weather overlay is coarse and
does not predict fire behavior. Basemap tiles come from OpenStreetMap and need
internet access.

## Ingestion

The ingestor polls each FIRMS product every 30 minutes for the last
`FIRMS_DAYS` days and keeps detections within Myanmar, Thailand, Laos,
Cambodia, Vietnam, Brunei, Malaysia, Singapore, the Philippines, Indonesia,
Papua New Guinea, and Timor-Leste. It uses
[Natural Earth 1:10m boundaries](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-countries/),
which are approximate, so some small-island and coastal detections may be
dropped. Forecasts are fetched every 3 hours.

| Variable | Default | Purpose |
|---|---|---|
| `FIRMS_PRODUCTS` | all four | Comma-separated FIRMS products |
| `FIRMS_DAYS` | `2` | Days per request (1–5) |
| `FIRMS_INTERVAL_MINUTES` | `30` | FIRMS polling interval |
| `WEATHER_INTERVAL_MINUTES` | `180` | Forecast polling interval |

Detections have deterministic event IDs, so overlapping polls are never
published twice. The same fire seen by two satellites within 1 km and
20 minutes is kept, and the lower-ranked observation is marked `duplicate_of`
the better one. VIIRS ranks above MODIS, then the earlier observation wins.
The dashboard hides duplicates.

To publish a CSV once, for example for an offline test (the events are
permanent, so use a disposable environment):

```sh
podman compose exec -T ingestor python -m wildfire_ingestor.firms \
  --product VIIRS_NOAA20_NRT --csv - < services/ingestor/tests/fixtures/viirs_noaa20.csv
```

## Development

Requires Node.js 22+, Python 3.12+, [uv](https://docs.astral.sh/uv/), and
[basedpyright](https://docs.basedpyright.com/) (`uv tool install basedpyright`).

```sh
# Web
cd apps/web && npm ci && npm run lint && npm test && npm run build && cd ../..

# Python (one .venv at the root for every project)
uv sync
uv run ruff check .
basedpyright
for project in apps/api packages/events services/ingestor services/sink-worker; do
  uv run pytest "$project/tests"
done
```

Run each project's tests separately, because test module names repeat across
projects. After changing an event model, run
`uv run python -m wildfire_events.schemas` and commit the regenerated schema.

**Integration tests** run against the local stack when these variables are
set. Each test creates and removes its own schemas, topics, and subjects.

| Project | Variables |
|---|---|
| API | `API_TEST_DATABASE_URL` |
| Ingestor | `INGESTOR_TEST_KAFKA`, `INGESTOR_TEST_SCHEMA_REGISTRY` |
| Sink worker | `SINK_TEST_DATABASE_URL`, `SINK_TEST_KAFKA`, `SINK_TEST_SCHEMA_REGISTRY` |

Use `postgresql://wildfire:wildfire_dev@localhost:55432/wildfire`,
`localhost:19092`, and `http://localhost:18081`.

**Running outside containers:** start the dependencies with
`podman compose up -d db kafka schema-registry`, then run
`uv run uvicorn wildfire_api.main:app --reload` from the root and `npm run dev`
in `apps/web`. Vite proxies `/api` to port 8000. The ingestor and sink worker
run with `uv run python -m wildfire_ingestor` and
`uv run python -m wildfire_sink`.
