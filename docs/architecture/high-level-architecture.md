# Wildfire Intelligence Platform

**High-Level Software Architecture**  
**Version:** 0.3  
**Status:** Proposed

This is the target architecture. The diagram marks which parts are already
implemented; [the roadmap](../roadmap.md) describes how the project gets to the
rest.

## Architecture Diagram

```mermaid
flowchart LR
    %% Layout: left to right in the order data flows. Nodes are declared next to
    %% the nodes they connect to, which keeps crossing edges down.

    %% External Sources (Implemented)
    FIRMS["NASA FIRMS API<br/>(Fire detections)"]
    METEO["Open-Meteo API<br/>(Weather forecasts)"]

    %% External Sources (Planned)
    OPENAQ["OpenAQ API<br/>(Air quality)"]

    %% Ingestor Service
    ING["Ingestor Service<br/>(FIRMS, weather)"]

    %% Apache Kafka (imagery.scenes counts as derived: it is built from fire.incidents)
    subgraph KAFKA["Apache Kafka + Schema Registry"]
        T_SRC["Source Topics<br/>(firms.detections,<br/>weather.forecasts)"]
        T_DER["Derived Topics<br/>(detections.enriched,<br/>fire.incidents,<br/>stats.hourly,<br/>imagery.scenes)"]
        T_USER["User Topics"]
        T_DLQ["Dead-Letter Topics"]
    end

    %% Stream Processor Service
    subgraph STREAM["Stream Processor"]
        S_ENRICH["Enrichment<br/>(weather + risk)"]
        S_INC["Incidents<br/>(clustering)"]
        S_STATS["Statistics<br/>(hourly aggregation)"]
    end

    %% Sink Worker Service (declared before the imagery worker, so it is drawn above it)
    subgraph WORKER["Sink Worker"]
        W_DB["PostGIS Projector"]
        W_S3["S3 Archiver"]
        W_ALERT["Alert Notifier"]
    end

    %% Imagery Worker and its sources (shares packages/producers with the ingestor)
    PC["Microsoft Planetary Computer<br/>(Sentinel-2 imagery)"]
    IMG["Imagery Worker<br/>(Sentinel-2)"]

    %% Data Storage & Destinations
    DB[("PostgreSQL<br/>+ PostGIS")]
    S3[("Amazon S3")]
    EMAIL["Email"]
    WEBHOOK["Webhooks"]
    STORE[("Imagery Store<br/>(volume)")]

    %% API & Frontend
    API["API Server<br/>(FastAPI)"]
    UI["Web Dashboard<br/>(React)"]

    %% Sources to Ingestor to Kafka
    FIRMS --> ING
    METEO --> ING
    OPENAQ -.-> ING
    ING --> T_SRC

    %% Stream Processor: source topics in, derived topics out
    T_SRC --> S_ENRICH
    T_SRC --> S_INC
    S_ENRICH --> T_DER
    S_INC --> T_DER
    T_DER --> S_STATS
    S_STATS --> T_DER
    S_ENRICH -->|invalid| T_DLQ
    S_INC -->|invalid| T_DLQ
    S_STATS -->|invalid| T_DLQ

    %% Sink Worker: derived topics to storage
    T_DER --> W_DB
    T_DER --> W_S3
    T_DER --> W_ALERT
    W_DB --> DB
    W_S3 -.-> S3
    W_ALERT -.-> EMAIL
    W_ALERT -.-> WEBHOOK

    %% Imagery Worker: incidents in; images to the store, scene metadata back to Kafka
    T_DER -->|fire.incidents| IMG
    PC --> IMG
    IMG -->|imagery.scenes| T_DER
    IMG --> STORE

    %% API: database, imagery store, and the live stream
    DB --> API
    STORE -->|images| API
    T_SRC -->|live| API
    T_DER -.->|alerts| API

    %% Frontend and planned user actions
    API <-->|HTTPS + SSE| UI
    API -.->|user actions| T_USER

    %% Styling
    classDef done fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534
    classDef planned fill:#f8fafc,stroke:#94a3b8,stroke-width:2px,stroke-dasharray:5 4,color:#64748b
    classDef external fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#075985
    classDef streaming fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#92400e
    classDef storage fill:#fce7f3,stroke:#db2777,stroke-width:2px,color:#831843

    class FIRMS,METEO,PC external
    class ING,IMG done
    class T_SRC,T_DER,T_DLQ streaming
    class DB,STORE storage
    class S_ENRICH,S_INC,S_STATS done
    class W_DB done
    class API,UI done

    class OPENAQ planned
    class T_USER planned
    class S3,EMAIL,WEBHOOK planned
    class W_S3,W_ALERT planned

    style KAFKA fill:#fffbeb,stroke:#d97706,stroke-width:2px,color:#92400e
    style STREAM fill:#f0f9ff,stroke:#0284c7,stroke-width:2px,color:#075985
    style WORKER fill:#faf5ff,stroke:#a855f7,stroke-width:2px,color:#581c87
```

Solid boxes and arrows are implemented; dashed grey boxes and dotted arrows
are planned. Arrows show the direction data flows.

## Services

| Service | Jobs | Responsibilities | Reads | Writes |
|---|---|---|---|---|
| **Ingestor** | — | Scheduled data producers for external sources | NASA FIRMS API, Open-Meteo API | `firms.detections`, `weather.forecasts` |
| **Imagery Worker** | Sentinel-2 | Scheduled satellite imagery of current fire incidents | `fire.incidents`, Microsoft Planetary Computer | `imagery.scenes`, the imagery store |
| **Stream Processor** | Enrichment, Incidents, Statistics, Alert (planned) | Add weather/country/risk to detections; cluster incidents; aggregate statistics; match watch areas | Source topics, compacted topics | Derived topics, `*.dlq` |
| **Sink Worker** | PostGIS Projector, S3 Archiver, Alert Notifier (planned) | Persist projections to database; archive to S3; send notifications | All topics | PostgreSQL, S3, email/webhook |
| **API Server** | — | Query incidents/hotspots/weather; stream live updates; handle user actions | PostgreSQL, Kafka (live stream) | User topics (`watch-areas`, `field-reports`, `alert-acks`) |
| **Web Dashboard** | — | Display live fire map; show incidents, weather, alerts; replay events | API Server | — |

## Topics

| Family | Topics | Status |
|---|---|---|
| **Source** | `firms.detections` | ✅ Implemented |
| | `weather.forecasts` (compacted) | ✅ Implemented |
| | `airquality.readings` | 🔲 Planned |
| **Derived** | `detections.enriched` | ✅ Implemented |
| | `fire.incidents` | ✅ Implemented |
| | `stats.hourly` | ✅ Implemented |
| | `alerts` | 🔲 Planned |
| **User** | `watch-areas` (compacted) | 🔲 Planned |
| | `field-reports` | 🔲 Planned |
| | `alert-acks` | 🔲 Planned |
| **Dead Letter** | `*.dlq` per consumer (e.g., `sink-worker.dlq`) | ✅ Implemented |

## External Systems

| System | Type | Purpose | Status |
|---|---|---|---|
| **NASA FIRMS API** | Data source | Satellite fire detections (SNPP, NOAA20) | ✅ Implemented |
| **Open-Meteo API** | Data source | Weather forecasts (temperature, wind, precipitation) | ✅ Implemented |
| **Microsoft Planetary Computer** | Data source | Sentinel-2 short-wave infrared imagery of incidents | ✅ Implemented |
| **OpenAQ API** | Data source | Air quality readings | 🔲 Planned |
| **PostgreSQL + PostGIS** | Data storage | Event projections, incidents, detection conditions | ✅ Implemented |
| **Amazon S3** | Data storage | Topic archival for long-term retention | 🔲 Planned |
| **Email / Webhooks** | Notifications | Alert delivery to users | 🔲 Planned |

## Architectural Principles

- **Event log as system of record:** Observations, forecasts, and user actions
  are Kafka events. PostGIS and S3 hold projections of the log and can be
  rebuilt by replaying topics.
- **Few services, many jobs:** Related jobs share a deployable service to keep
  the system small. Each job in the stream processor and the sink worker uses
  its own consumer group, so jobs track their offsets independently and can be
  split into separate services later without changing topics.
- **Producers:** The ingestor runs one scheduled producer per source.
  Producers are idempotent, and events are keyed by H3 cell so spatially
  related data shares a partition. Satellite imagery is acquired by a separate
  imagery worker: it depends on `fire.incidents`, its runs are slow and
  image-heavy, and restarting it must not interrupt live ingestion. Both share
  the producer runtime in `packages/producers`.
- **Schemas:** Event schemas are versioned in a schema registry. Invalid events
  go to dead-letter topics; consumers keep running.
- **Compacted topics as tables:** The latest forecast per grid cell and the
  current set of watch areas are compacted topics that stream processors join
  against.
- **Side effects at the edge:** The stream processor only reads and writes
  Kafka. Writes to PostGIS, S3, and notification channels happen in the sink
  worker, so reprocessing never sends duplicate notifications from the
  processor.
- **Backend API:** FastAPI serves queries from PostGIS, publishes user actions
  to Kafka, and subscribes to derived topics to stream live updates to the
  browser with Server-Sent Events. The API is a consumer but not a consumer
  group: each instance assigns itself all partitions manually, so every
  instance receives every event and filters it per connected client. Manual
  assignment also lets the API resume a reconnecting client from the Kafka
  offset in its `Last-Event-ID`.
- **Containers:** Each service runs in its own Podman container, with separate
  build and runtime stages.
- **Infrastructure:** AWS CDK manages infrastructure provisioning;
  infrastructure details are outside this code-level diagram.

Topic keys, schemas, dead-letter handling, and the replay procedure are
documented in [the event pipeline operations guide](../operations/event-pipeline.md).

## Open Design Questions

- Choose between Amazon MSK Serverless and a self-managed broker for the AWS
  deployment, based on cost and operational effort.
- Choose the incident clustering parameters (distance and time window) and
  validate them against historical FIRMS data.
- Decide whether alert delivery requires authentication and per-user
  subscriptions before the AWS deployment.
