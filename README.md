# Wildfire Intelligence Platform

A live map of satellite fire detections and weather forecasts for Southeast
Asia and Papua New Guinea, built on a Kafka event pipeline.

![Wildfire hotspot dashboard](docs/architecture/dashboard-snapshot.png)

## Architecture

Kafka is the system of record. The ingestor publishes every observation and
forecast as a schema-validated event. The sink worker projects the topics into
PostGIS, and the projections can be rebuilt by replaying the log. The stream
processor clusters detections into fire incidents, enriches each detection
with its country, the forecast weather at observation time, and a fire risk
indicator, and aggregates hourly per-country statistics. The API serves
queries from PostGIS and streams new detections from Kafka to the browser.

```mermaid
flowchart LR
    %% External Sources (Implemented)
    FIRMS["NASA FIRMS API<br/>(Fire detections)"]
    METEO["Open-Meteo API<br/>(Weather forecasts)"]

    %% External Sources (Planned)
    OPENAQ["OpenAQ API<br/>(Air quality)"]
    HIMAWARI["Himawari-8<br/>(Satellite imagery)"]

    %% Ingestor Service
    ING["Ingestor Service"]

    %% Apache Kafka (Compact)
    subgraph KAFKA["Apache Kafka + Schema Registry"]
        T_SRC["Source Topics<br/>(firms.detections,<br/>weather.forecasts)"]
        T_DER["Derived Topics<br/>(detections.enriched,<br/>fire.incidents,<br/>stats.hourly)"]
        T_USER["User Topics"]
        T_DLQ["Dead-Letter Topics"]
    end

    %% Stream Processor Service
    subgraph STREAM["Stream Processor"]
        S_ENRICH["Enrichment<br/>(weather + risk)"]
        S_INC["Incidents<br/>(clustering)"]
        S_STATS["Statistics<br/>(hourly aggregation)"]
    end

    %% Sink Worker Service
    subgraph WORKER["Sink Worker"]
        W_DB["PostGIS Projector"]
        W_S3["S3 Archiver"]
        W_ALERT["Alert Notifier"]
    end

    %% Data Storage & Destinations
    DB[("PostgreSQL<br/>+ PostGIS")]
    S3[("Amazon S3")]
    EMAIL["Email"]
    WEBHOOK["Webhooks"]

    %% API & Frontend
    API["API Server<br/>(FastAPI)"]
    UI["Web Dashboard<br/>(React)"]

    %% Main flows: Sources to Ingestor
    FIRMS --> ING
    METEO --> ING
    OPENAQ -.-> ING
    HIMAWARI -.-> ING

    %% Ingestor to Kafka Source Topics
    ING --> T_SRC

    %% Stream Processor: Source Topics to Jobs (both Enrichment and Incidents in parallel)
    T_SRC --> S_ENRICH
    T_SRC --> S_INC

    %% Stream Processor: Enrichment output to Derived Topics
    S_ENRICH --> T_DER

    %% Stream Processor: Incidents output to Derived Topics
    S_INC --> T_DER

    %% Stream Processor: Statistics reads from Derived (Enriched), outputs to Derived
    T_DER --> S_STATS
    S_STATS --> T_DER

    %% Error handling
    S_ENRICH -->|invalid| T_DLQ
    S_INC -->|invalid| T_DLQ
    S_STATS -->|invalid| T_DLQ

    %% Sink Worker: Derived Topics to Storage
    T_DER --> W_DB
    T_DER --> W_S3
    T_DER --> W_ALERT

    %% Storage destinations
    W_DB --> DB
    W_S3 -.-> S3
    W_ALERT -.-> EMAIL
    W_ALERT -.-> WEBHOOK

    %% API: Database and Kafka
    DB --> API
    T_SRC -->|live| API

    %% Planned: User topics and alerts
    API -.->|user actions| T_USER
    T_DER -.->|alerts| API

    %% Frontend
    API <-->|HTTPS + SSE| UI

    %% Styling
    classDef done fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534
    classDef planned fill:#f8fafc,stroke:#94a3b8,stroke-width:2px,stroke-dasharray:5 4,color:#64748b
    classDef external fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#075985
    classDef streaming fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#92400e
    classDef storage fill:#fce7f3,stroke:#db2777,stroke-width:2px,color:#831843

    class FIRMS,METEO external
    class ING done
    class T_SRC,T_DER,T_DLQ streaming
    class DB storage
    class S_ENRICH,S_INC,S_STATS done
    class W_DB done
    class API,UI done

    class OPENAQ,HIMAWARI planned
    class T_USER planned
    class S_ALERT planned
    class S3,EMAIL,WEBHOOK planned
    class W_S3,W_ALERT planned

    style KAFKA fill:#fffbeb,stroke:#d97706,stroke-width:2px,color:#92400e
    style STREAM fill:#f0f9ff,stroke:#0284c7,stroke-width:2px,color:#075985
    style WORKER fill:#faf5ff,stroke:#a855f7,stroke-width:2px,color:#581c87
```

Solid lines and boxes show implemented features; dotted lines and dashed boxes show planned features.

The [target architecture](docs/architecture/high-level-architecture.md) adds
watch areas, alerting, and the archive; see the
[roadmap](docs/roadmap.md).
The [event pipeline guide](docs/operations/event-pipeline.md) covers topics,
schemas, dead letters, and replaying the log.

```text
apps/api/              FastAPI: PostGIS queries and the live SSE stream
apps/web/              React + TypeScript + OpenLayers dashboard
packages/events/       Shared topics, H3 keys, event models, schemas, and codec
services/ingestor/     Scheduled FIRMS and Open-Meteo producers
services/sink-worker/  Kafka-to-PostGIS projections and dead-letter routing
services/stream-processor/  Quix Streams jobs: incidents, enrichment and risk, hourly statistics
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
- **Data freshness** in the filter panel shows each source's newest
  observation (or forecast retrieval) and whether it is fresh, delayed, or
  stale.
- **Pipeline status** (<http://localhost:8080/#/status>) adds per-source
  ingest counts, topic throughput, sink-worker consumer lag, and recent dead
  letters. See [pipeline monitoring](docs/operations/event-pipeline.md#pipeline-status).

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

## Fire incidents

The stream processor groups detections that are within about 2.5–4 km
(neighbouring resolution-7 H3 cells) and 48 hours of each other into
incidents, and publishes a new revision to `fire.incidents` whenever one
changes. Each incident is `new` for its first 24 hours, then `growing` while
it gains area or `stable` when it stops; it becomes `inactive` after 48 hours
without detections, or `merged` when it joins an older incident. Revisions
carry the footprint, FRP totals, bounding box, and daily growth history.

The dashboard lists active incidents for the selected area and draws them on
the map, coloured by status. Selecting one shows its growth over time (daily
detections and cumulative footprint) and the forecast for its area over the
next 24 hours. Each detection's details show its country, the forecast weather
at observation time, and a simple fire risk indicator (low to extreme) scored
from wind, temperature, 24-hour rainfall, and fire radiative power. The API
also serves hourly per-country detection and FRP statistics at
`/api/stats/hourly`. See the
[event pipeline guide](docs/operations/event-pipeline.md#stream-processor) for
the rules and how to reprocess.

## Development

Requires Node.js 22+, Python 3.12+, and [uv](https://docs.astral.sh/uv/).
Python code follows the [Python coding standard](docs/python-coding-principles.md).

```sh
# Web
cd apps/web && npm ci && npm run lint && npm test && npm run build && cd ../..

# Python (one .venv at the root for every project; same commands as CI)
uv sync
uv run ruff format --check .
uv run ruff check .
uv run basedpyright
uv run pytest
```

Ruff, basedpyright, and pytest are pinned in the root `pyproject.toml` and
configured only there. After changing an event model, run
`uv run python -m wildfire_events.schemas` and commit the regenerated schema.

**Integration tests** run against the local stack when these variables are
set. Each test creates and removes its own schemas, topics, and subjects.

| Project | Variables |
|---|---|
| API | `API_TEST_DATABASE_URL` |
| Ingestor | `INGESTOR_TEST_KAFKA`, `INGESTOR_TEST_SCHEMA_REGISTRY` |
| Sink worker | `SINK_TEST_DATABASE_URL`, `SINK_TEST_KAFKA`, `SINK_TEST_SCHEMA_REGISTRY` |
| Stream processor | `PROCESSOR_TEST_KAFKA`, `PROCESSOR_TEST_SCHEMA_REGISTRY` |

Use `postgresql://wildfire:wildfire_dev@localhost:55432/wildfire`,
`localhost:19092`, and `http://localhost:18081`.

**Running outside containers:** start the dependencies with
`podman compose up -d db kafka schema-registry`, then run
`uv run uvicorn wildfire_api.main:app --reload` from the root and `npm run dev`
in `apps/web`. Vite proxies `/api` to port 8000. The ingestor, sink worker,
and stream processor run with `uv run python -m wildfire_ingestor`,
`uv run python -m wildfire_sink`, and `uv run python -m wildfire_processor`.

The stream processor pins `pydantic<2.14` through Quix Streams. The uv
workspace shares one resolution, so every project gets that version locally;
container images and CI install each service separately.
