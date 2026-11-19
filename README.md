# Wildfire Intelligence Platform

The repository is organized so the user-facing applications, data services,
infrastructure, and project documentation can grow independently:

```text
apps/
  api/                 FastAPI application
  web/                 React + TypeScript + Vite application
infra/
  compose/              Local PostgreSQL/PostGIS initialization
services/
  firms-pipeline/      FIRMS ingestion and Kafka-to-PostGIS processor
docs/                    Requirements, architecture, decisions, and roadmap
```

## Dashboard snapshot

The dashboard displays satellite hotspot detections on a map, with geographic
and date filters and recent observation details.

![Wildfire hotspot dashboard](docs/architecture/dashboard-snapshot.png)

## Architecture

The diagram shows the services currently implemented in this repository. FIRMS
ingestion is run on demand; weather is fetched and stored when requested and
refreshed after six hours; the processor continuously consumes Kafka events.

```mermaid
flowchart TB
    subgraph External["External Services"]
        FIRMS["NASA FIRMS API"]
        WEATHER["Open-Meteo forecast API"]
        OSM["OpenStreetMap tile service"]
    end

    subgraph Ingestion["On-Demand Ingestion"]
        INGEST["FIRMS ingestor<br/>Validate CSV and filter detections"]
    end

    subgraph Streaming["Event Streaming"]
        KAFKA["Apache Kafka<br/>firms.detections.v1"]
        PROC["FIRMS processor<br/>Validate and persist events"]
        KAFKA --> PROC
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
    WEATHER -->|7-day hourly forecast| API
    INGEST -->|Country boundary filter| DB
    INGEST --> KAFKA
    PROC --> DB
    API -->|Hotspot queries| DB
    API <-->|Weather forecast storage| DB
    UI -->|Map tile requests| OSM

    classDef external fill:#e0f2fe,stroke:#0284c7,color:#075985
    classDef streaming fill:#fef3c7,stroke:#d97706,color:#92400e
    classDef data fill:#dcfce7,stroke:#16a34a,color:#166534

    class FIRMS,WEATHER,OSM external
    class KAFKA,PROC streaming
    class DB data
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

Compose builds the web, API, and processor images, initializes PostgreSQL with
PostGIS, and starts Kafka in single-node KRaft mode. The processor runs
continuously; ingestion is run separately on demand.
The web application is available at <http://localhost:8080>; the API docs are
at <http://localhost:8000/docs>. Database and Kafka are published at ports
55432 and 19092 by default to avoid common host conflicts. Override these with
the `POSTGRES_PORT`, `KAFKA_PORT`, `API_PORT`, or `WEB_PORT` environment
variables. Stop the services with `Ctrl+C`; remove containers and both the
local database and Kafka volumes with `podman compose down -v`.

The local database uses development-only credentials from `compose.yaml`.
Do not use these defaults outside a local development environment.

## Hotspot dashboard

The dashboard displays detections already ingested into PostGIS. By default
there is no geographic filter; the map initially frames Southeast Asia. Choose
an optional regional area and an inclusive UTC date range, then select **Apply
filters**. A dashed map outline shows the applied area filter. Areas use
approximate, overlapping bounding boxes and can include nearby land and sea;
they are not country boundaries. Choose a map
point or a recent observation to see its acquisition time, source, confidence,
and other attributes. Use the arrows on the side panels to collapse or reopen
them; selecting a map point reopens observation details. **Refresh data**
re-queries the database; it does not run FIRMS
ingestion. The map uses OpenStreetMap tiles and needs internet access for its
basemap.

Marker color and size use a fixed logarithmic fire radiative power (FRP) scale
from 0 to 100 MW; values at or above 100 MW share the largest symbol, while
the detail panel retains the measured value. Gray symbols indicate unavailable
or invalid FRP. FRP is pixel-integrated radiated power, not a direct measure
of fire severity or danger; satellite retrievals have limitations. See
[NASA's VIIRS product documentation](https://www.earthdata.nasa.gov/data/instruments/viirs/viirs-i-band-375-m-active-fire-data).

## Weather forecasts

Choose **Temperature**, **Precipitation**, or **Wind speed** from the map's
weather-layer menu to load Open-Meteo's seven-day hourly forecast. The app
submits `POST /weather/forecast`; the API stores forecasts in the
`weather_forecasts` table on a 3-degree regional grid. Stored data is reused
for six hours; **Refresh forecast** requests a fresh forecast immediately. The
selected forecast hour is controlled by the timeline, and wind arrows point
downwind. The overlay is a coarse visualization, not a local weather station
reading or a fire-behavior prediction. Temperatures are in °C, hourly
precipitation in mm, and wind speed in km/h. See
[Open-Meteo](https://open-meteo.com/) for provider details and attribution.

The [hotspot API](http://localhost:8000/docs#/hotspots) accepts optional
`west`, `south`, `east`, and `north` bounding-box coordinates (all four
required together), and `date_from` / `date_to` in `YYYY-MM-DD` UTC.
`GET /hotspots` returns `items` in newest-first order and a `truncated` flag.
The default limit is 5,000 (maximum 10,000); narrow the filters if the
dashboard reports that the limit was reached. Geographic boxes do not cross
the antimeridian. The processor must have created the detections table before
hotspot queries can succeed.

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

cd ../../services/firms-pipeline
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
ruff check .
pytest
```

API integration tests run when `API_TEST_DATABASE_URL` points to a PostGIS
database, and create and remove an isolated test schema. For the default local
database, run from the project root with the API virtual environment active:

```sh
API_TEST_DATABASE_URL=postgresql://wildfire:wildfire_dev@localhost:55432/wildfire \
  pytest apps/api/tests
```

From the repository root, the pipeline integration test is skipped unless both
`PIPELINE_TEST_DATABASE_URL` and `PIPELINE_TEST_KAFKA` are set. With the local
stack running, execute it with:

```sh
PIPELINE_TEST_DATABASE_URL=postgresql://wildfire:wildfire_dev@localhost:55432/wildfire \
PIPELINE_TEST_KAFKA=localhost:19092 \
  pytest services/firms-pipeline/tests
```

The integration test creates and removes its own temporary database schema and
Kafka topic. It requires the pipeline dependencies to be installed in the
active Python environment.

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

## FIRMS ingestion

The pipeline uses the FIRMS `VIIRS_NOAA20_NRT` product for Myanmar, Thailand,
Laos, Cambodia, Vietnam, Brunei, Malaysia, Singapore, the Philippines,
Indonesia, Papua New Guinea, and Timor-Leste. It requests the regional
bounding box `92,-12,156,29` (west, south, east, north), then keeps detections
whose pixel centers fall within those countries. The same filter applies to
CSV input. The public-domain [Natural Earth 1:10m country boundaries, version
5.1.1](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-countries/)
are approximate and use de facto boundaries; small islands and coastal
detections may be omitted. Detections are satellite pixel centers, not
confirmed fire perimeters.

Existing records are not deleted or retroactively filtered, so earlier
offshore observations may still appear. The dashboard's area filters remain
bounding boxes, not country boundaries.

1. Request a free [NASA FIRMS MAP_KEY](https://firms.modaps.eosdis.nasa.gov/api/area/).
2. Save the key in `.firms-map-key` in the project root, with restrictive
   permissions (`chmod 600 .firms-map-key`). For safety, this file is Git-ignored
   and is not included in the GitHub repository. Create it locally and never
   commit or share your key.
3. Start the stack and run an ingestion:

   ```sh
   podman compose up -d --build
   podman compose exec -T processor python -m wildfire_firms.ingest --key-stdin < .firms-map-key
   ```

The default request covers the latest day. Add `--days 2` through `--days 5`
to request more recent days. Run the command again whenever a refresh is
needed; automatic scheduling is not configured. For an offline smoke test
without a key, replace the ingestion command with:

```sh
podman compose exec -T processor python -m wildfire_firms.ingest --csv - \
  < services/firms-pipeline/tests/fixtures/borneo.csv
```

Inspect the worker and resulting rows:

```sh
podman compose logs processor
podman compose exec -T db psql -U wildfire -d wildfire \
  -c 'SELECT source, satellite, observed_at, ST_AsText(location) FROM firms_detections ORDER BY observed_at DESC LIMIT 10;'
```

Events use the `firms.detections.v1` Kafka topic and preserve FIRMS attributes
and UTC acquisition time. The processor validates each event and commits its
Kafka offset only after the PostGIS transaction commits. Duplicate detections
with the same source, satellite, acquisition time, and coordinates are ignored;
the first stored attributes are retained. Failed or invalid events stop the
processor without committing that offset. Inspect its logs and repair the
problem before restarting it; an invalid Kafka event requires operator
intervention because published records cannot be edited in place. Kafka and
PostGIS data persist across ordinary Compose restarts, subject to Kafka's
retention policy, but `down -v` deletes both.

## Local service endpoints

| Service | Address |
|---|---|
| Web | <http://localhost:8080> |
| API / OpenAPI docs | <http://localhost:8000/docs> |
| API health | <http://localhost:8000/health> |
| API health through the web proxy | <http://localhost:8080/api/health> |
| Hotspot API through the web proxy | <http://localhost:8080/api/hotspots> |
| Weather forecast API through the web proxy | <http://localhost:8080/api/weather/forecast> |
| PostgreSQL/PostGIS | `localhost:55432` |
| Kafka | `localhost:19092` |
