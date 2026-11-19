# Wildfire Intelligence Platform

**High-Level Software Architecture**  
**Version:** 0.1  
**Status:** Proposed

## Architecture Diagram

```mermaid
flowchart TB
    subgraph External["External Data Sources"]
        FIRMS["NASA FIRMS API"]
        WEATHER["Open-Meteo Forecast API"]
        SAT["Satellite Imagery Providers"]
    end

    subgraph Ingestion["Data Ingestion"]
        FI["FIRMS Ingestor<br/>Python"]
        SI["Imagery Ingestor<br/>Python"]
    end

    subgraph Streaming["Event Streaming"]
        KAFKA["Apache Kafka"]
    end

    subgraph Processing["Data Processing"]
        PROC["Geospatial Processor<br/>Python"]
        ALERT["Alert Processor<br/>Python"]
    end

    subgraph Storage["Data Storage"]
        DB[("PostgreSQL + PostGIS<br/>Detections and weather forecasts")]
        S3[("Amazon S3")]
    end

    subgraph Backend["Backend"]
        API["REST API<br/>Python + FastAPI"]
    end

    subgraph Frontend["Frontend"]
        UI["React + TypeScript<br/>Vite"]
        MAP["OpenLayers<br/>Geospatial Visualization"]
        DASH["Dashboard Components"]
        UI --> MAP
        UI --> DASH
    end

    FIRMS --> FI
    SAT --> SI

    FI --> KAFKA
    SI --> KAFKA

    KAFKA --> PROC
    KAFKA --> ALERT

    PROC --> DB
    PROC --> S3
    ALERT --> DB

    API <--> DB
    API <--> S3
    WEATHER -->|"On-demand hourly forecasts"| API

    UI <-->|HTTPS / JSON| API

    classDef external fill:#e0f2fe,stroke:#0284c7,color:#075985
    classDef streaming fill:#fef3c7,stroke:#d97706,color:#92400e
    classDef data fill:#dcfce7,stroke:#16a34a,color:#166534

    class FIRMS,WEATHER,SAT external
    class KAFKA streaming
    class DB,S3 data
```

## Architectural Principles

- **Frontend:** React with OpenLayers for mapping, weather overlays, and satellite visualization.
- **Backend API:** FastAPI provides data access and business logic without coupling the frontend directly to storage.
- **Ingestion:** FIRMS data is published to Kafka; the API fetches and caches Open-Meteo forecasts on demand.
- **Processing:** Kafka consumers normalize, enrich, and persist geospatial information.
- **Storage:** PostGIS handles spatial queries, detections, and hourly forecasts; S3 stores large imagery and raster datasets.
- **Containers:** Each application service runs in its own Podman container, with separate build and runtime stages.
- **Infrastructure:** AWS CDK manages infrastructure provisioning; infrastructure details are outside this code-level diagram.

## Open Design Question

Determine whether the frontend should retrieve geospatial raster data directly from S3 using signed URLs or exclusively through the backend API.
