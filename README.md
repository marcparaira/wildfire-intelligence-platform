# Wildfire Intelligence Platform

The repository is organized so the user-facing applications, data services,
shared event contracts, infrastructure, and project documentation can grow
independently:

```text
apps/
  api/                 FastAPI application (queries PostGIS projections)
  web/                 React + TypeScript + Vite application
packages/
  events/              Shared topics, H3 keys, event models, JSON Schemas, and codec
services/
  ingestor/            Scheduled FIRMS and Open-Meteo producers
  sink-worker/         Kafka-to-PostGIS projections and dead-letter routing
infra/
  compose/             Local PostgreSQL/PostGIS initialization
docs/                  Requirements, architecture, operations, and roadmap
```

## Dashboard snapshot

The dashboard displays satellite hotspot detections on a map, with geographic
and date filters and recent observation details.

![Wildfire hotspot dashboard](docs/architecture/dashboard-snapshot.png)

## Architecture

Kafka is the system of record. Scheduled producers publish every observation
and forecast as a schema-governed event; PostGIS holds projections of the log
that can be rebuilt by replaying the topics. The diagram shows what is
implemented today; the [target architecture](docs/architecture/high-level-architecture.md)
adds stream processing, live updates, and alerting.

```mermaid
flowchart TB
    subgraph External["External Services"]
        FIRMS["NASA FIRMS API<br/>VIIRS SNPP, NOAA-20, NOAA-21, MODIS"]
        WEATHER["Open-Meteo forecast API"]
        OSM["OpenStreetMap tile service"]
    end

    subgraph Ingestion["Ingestor"]
        INGEST["Scheduled producers<br/>Idempotent, keyed by H3 cell"]
    end

    subgraph Streaming["Event Streaming"]
        KAFKA["Apache Kafka<br/>firms.detections<br/>weather.forecasts (compacted)<br/>sink-worker.dlq"]
        REG["Schema registry<br/>Karapace, JSON Schema"]
        KUI["Kafka UI"]
    end

    subgraph Sink["Sink worker"]
        PROJ["PostGIS projections<br/>one consumer group per job"]
    end

    subgraph Storage["Data Storage"]
        DB[("PostgreSQL + PostGIS")]
    end

    subgraph Application["Dashboard and API"]
        WEB["Web container<br/>Nginx static hosting and /api proxy"]
        UI["Browser dashboard<br/>React + TypeScript + OpenLayers"]
        API["REST API<br/>Python + FastAPI"]
        WEB <--> UI
        WEB -->|API proxy| API
    end

    FIRMS --> INGEST
    WEATHER --> INGEST
    INGEST -->|register schemas| REG
    INGEST --> KAFKA
    KAFKA --> PROJ
    PROJ -->|validate schema IDs| REG
    PROJ -->|invalid events| KAFKA
    PROJ --> DB
    KUI --- KAFKA
    API -->|hotspot and forecast queries| DB
    UI -->|Map tile requests| OSM

    classDef external fill:#e0f2fe,stroke:#0284c7,color:#075985
    classDef streaming fill:#fef3c7,stroke:#d97706,color:#92400e
    classDef data fill:#dcfce7,stroke:#16a34a,color:#166534

    class FIRMS,WEATHER,OSM external
    class KAFKA,REG,KUI,PROJ streaming
    class DB data
```

[docs/operations/event-pipeline.md](docs/operations/event-pipeline.md)
describes the topics, keys, schemas, dead-letter handling, and how to rebuild
PostGIS by replaying the log.

## Prerequisites

- Podman with `podman compose` support (or Docker Compose)
- Node.js 22+ and npm (for running frontend checks outside containers)
- Python 3.12+ (for running backend checks outside containers)

## Start the complete local environment

From the repository root, run:

```sh
export FIRMS_MAP_KEY="$(cat .firms-map-key)"   # optional; see FIRMS ingestion
podman compose up --build
```

Compose builds the web, API, ingestor, and sink-worker images, initializes
PostgreSQL with PostGIS, and starts Kafka in single-node KRaft mode with the
Karapace schema registry and Kafka UI. The ingestor publishes FIRMS detections
every 30 minutes and Open-Meteo forecasts every 3 hours; the sink worker
continuously projects both topics into PostGIS. Without a FIRMS key, only the
weather producer runs.

The web application is available at <http://localhost:8080>; the API docs are
at <http://localhost:8000/docs>; Kafka UI is at <http://localhost:8090>.
Database, Kafka, and the schema registry are published at ports 55432, 19092,
and 18081 by default to avoid common host conflicts. Override these with the
`POSTGRES_PORT`, `KAFKA_PORT`, `SCHEMA_REGISTRY_PORT`, `KAFKA_UI_PORT`,
`API_PORT`, or `WEB_PORT` environment variables. Stop the services with
`Ctrl+C`; remove containers and both the local database and Kafka volumes with
`podman compose down -v`. Because Kafka is the system of record, `down -v`
deletes the event history, not only a cache.

podman-compose 1.0.6 does not recreate existing containers when
`compose.yaml` changes; run `podman compose down` (without `-v`) and `up`
again after pulling configuration changes.

The local database uses development-only credentials from `compose.yaml`.
Do not use these defaults outside a local development environment.

## Hotspot dashboard

The dashboard displays detections from the PostGIS projection. By default
there is no geographic filter; the map initially frames Southeast Asia. Choose
an optional regional area and an inclusive UTC date range, then select **Apply
filters**. A dashed map outline shows the applied area filter. Areas use
approximate, overlapping bounding boxes and can include nearby land and sea;
they are not country boundaries. Choose a map point or a recent observation to
see its acquisition time, satellite, instrument, product, confidence, and other
attributes. Use the arrows on the side panels to collapse or reopen them;
selecting a map point reopens observation details. **Refresh data** re-queries
the database for detections and the forecast; it does not trigger ingestion.
The map uses OpenStreetMap tiles and needs internet access for its basemap.

Marker color and size use a fixed logarithmic fire radiative power (FRP) scale
from 0 to 100 MW; values at or above 100 MW share the largest symbol, while
the detail panel retains the measured value. Gray symbols indicate unavailable
or invalid FRP. FRP is pixel-integrated radiated power, not a direct measure
of fire severity or danger; satellite retrievals have limitations. See
[NASA's VIIRS product documentation](https://www.earthdata.nasa.gov/data/instruments/viirs/viirs-i-band-375-m-active-fire-data).

The [hotspot API](http://localhost:8000/docs#/hotspots) accepts optional
`west`, `south`, `east`, and `north` bounding-box coordinates (all four
required together), `date_from` / `date_to` in `YYYY-MM-DD` UTC, and one or
more `product` values. `GET /hotspots` returns `items` in newest-first order
and a `truncated` flag. Cross-sensor duplicates (see below) are hidden unless
`include_duplicates=true`. The default limit is 5,000 (maximum 10,000); narrow
the filters if the dashboard reports that the limit was reached. Geographic
boxes do not cross the antimeridian.

## Weather forecasts

Choose **Temperature**, **Precipitation**, or **Wind speed** from the map's
weather-layer menu to show Open-Meteo's seven-day hourly forecast. The ingestor
requests forecasts for the centers of the 385 resolution-2 H3 cells (average
area about 87,000 km²) that cover the region, and publishes one event per cell
to the compacted `weather.forecasts` topic, which keeps the latest forecast per
cell. The sink worker stores them in PostGIS, and `GET /weather/forecast` reads
them; the API never calls Open-Meteo. The timeline selects the forecast hour,
the label shows when the forecast was retrieved, and wind arrows point
downwind. The overlay is a coarse visualization, not a local weather station
reading or a fire-behavior prediction. Temperatures are in °C, hourly
precipitation in mm, and wind speed in km/h. See
[Open-Meteo](https://open-meteo.com/) for provider details and attribution.

## FIRMS ingestion

The ingestor runs one scheduled producer for each FIRMS near-real-time product:
`VIIRS_SNPP_NRT`, `VIIRS_NOAA20_NRT`, `VIIRS_NOAA21_NRT`, and `MODIS_NRT`. Each
run requests the regional bounding box `92,-12,156,29` (west, south, east,
north) for the last `FIRMS_DAYS` days (default 2), then keeps detections whose
pixel centers fall within Myanmar, Thailand, Laos, Cambodia, Vietnam, Brunei,
Malaysia, Singapore, the Philippines, Indonesia, Papua New Guinea, and
Timor-Leste. The country filter uses the bundled public-domain
[Natural Earth 1:10m country boundaries, version 5.1.1](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-countries/),
which are approximate and use de facto boundaries; small islands and coastal
detections may be omitted. Detections are satellite pixel centers, not
confirmed fire perimeters.

1. Request a free [NASA FIRMS MAP_KEY](https://firms.modaps.eosdis.nasa.gov/api/area/).
2. Save the key in `.firms-map-key` in the project root, with restrictive
   permissions (`chmod 600 .firms-map-key`). For safety, this file is Git-ignored
   and is not included in the GitHub repository. Never commit or share your key.
3. Export it before starting the stack, as shown above. Compose passes it to the
   ingestor container as `FIRMS_MAP_KEY`.

Tune the producers with `FIRMS_PRODUCTS` (comma-separated),
`FIRMS_DAYS` (1-5), `FIRMS_INTERVAL_MINUTES`, and `WEATHER_INTERVAL_MINUTES`.
FIRMS reports each product's latest available date at its
[data-availability endpoint](https://firms.modaps.eosdis.nasa.gov/api/data_availability/);
a product with no recent data simply publishes nothing.

Re-polling overlapping days returns the same detections. Each detection has a
deterministic event ID, and the ingestor reads the IDs it published recently
back from `firms.detections` when it starts, so it never republishes them.
Producers are idempotent, so broker retries cannot duplicate records either.

To publish a CSV once, for example an offline smoke test without a key (the
fixture's detections become permanent events in the log, so prefer a
disposable environment):

```sh
podman compose exec -T ingestor python -m wildfire_ingestor.firms \
  --product VIIRS_NOAA20_NRT --csv - < services/ingestor/tests/fixtures/viirs_noaa20.csv
```

Inspect the services and resulting rows:

```sh
podman compose logs ingestor sink-worker
podman compose exec -T db psql -U wildfire -d wildfire \
  -c 'SELECT product, satellite, observed_at, duplicate_of FROM fire_detections ORDER BY observed_at DESC LIMIT 10;'
```

### Cross-sensor duplicates

Satellites in the same orbit can see the same fire minutes apart (for example
Aqua MODIS and Suomi-NPP VIIRS). The sink worker keeps every observation but
marks a detection as `duplicate_of` the best-ranked detection from another
satellite within 1 km and 20 minutes, preferring VIIRS (375 m) over MODIS
(1 km), then the earlier observation. Detections from the NOAA-20 and NOAA-21
passes, which are 40 minutes or more apart, remain separate observations.

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

cd ../..
python -m venv .venv
. .venv/bin/activate
pip install -e 'packages/events[dev]' -e 'services/ingestor[dev]' -e 'services/sink-worker[dev]'
ruff check packages/events services/ingestor services/sink-worker
pytest packages/events/tests services/ingestor/tests services/sink-worker/tests
```

After changing an event model in `packages/events`, regenerate its registered
JSON Schema with `python -m wildfire_events.schemas` and commit the result; a
test fails while the checked-in schema is stale.

Integration tests run when their environment variables point at the local
stack, and create and remove isolated database schemas, topics, consumer
groups, and registry subjects. From the project root, with the stack running:

```sh
API_TEST_DATABASE_URL=postgresql://wildfire:wildfire_dev@localhost:55432/wildfire \
  pytest apps/api/tests

INGESTOR_TEST_KAFKA=localhost:19092 \
INGESTOR_TEST_SCHEMA_REGISTRY=http://localhost:18081 \
  pytest services/ingestor/tests

SINK_TEST_DATABASE_URL=postgresql://wildfire:wildfire_dev@localhost:55432/wildfire \
SINK_TEST_KAFKA=localhost:19092 \
SINK_TEST_SCHEMA_REGISTRY=http://localhost:18081 \
  pytest services/sink-worker/tests
```

To run an app against the local dependencies without building its container,
start the services with `podman compose up -d db kafka schema-registry`, then
run:

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
The ingestor and sink worker default to the published local ports, so
`python -m wildfire_ingestor` and `python -m wildfire_sink` also run directly
from the root virtual environment.

## Local service endpoints

| Service | Address |
|---|---|
| Web | <http://localhost:8080> |
| API / OpenAPI docs | <http://localhost:8000/docs> |
| API health | <http://localhost:8000/health> |
| API health through the web proxy | <http://localhost:8080/api/health> |
| Hotspot API through the web proxy | <http://localhost:8080/api/hotspots> |
| Weather forecast API through the web proxy | <http://localhost:8080/api/weather/forecast> |
| Kafka UI | <http://localhost:8090> |
| Schema registry | <http://localhost:18081/subjects> |
| PostgreSQL/PostGIS | `localhost:55432` |
| Kafka | `localhost:19092` |
