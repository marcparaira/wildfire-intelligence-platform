# Wildfire Intelligence Platform
**High-Level Requirements Document**

**Version:** 0.1
**Region:** Southeast Asia  
**Status:** Draft

## 1. Executive Summary

The objective is to develop a web-based geospatial dashboard to support firefighters and emergency response teams in monitoring and managing wildfires across Southeast Asia.

The platform will integrate NASA FIRMS satellite fire detections, weather forecasts, and geospatial imagery to provide near-real-time situational awareness, identify areas potentially at risk, and support operational decision-making.

## 2. Scope

The platform will provide:

- **Wildfire monitoring:** Visualization of active fire detections and historical fire activity.
- **Weather monitoring:** Current weather conditions and forecasts relevant to wildfire behavior.
- **Geospatial visualization:** Interactive maps, satellite imagery, and geographic overlays.
- **Risk awareness:** Identification of potentially affected areas based on fire activity, weather, and geographic context.
- **Operational support:** Alerts, filters, and reporting tools for emergency response teams.

The initial geographic scope covers Southeast Asia, with the possibility of starting in a selected pilot region.

## 3. Functional Requirements

| ID | Requirement | Description |
|---|---|---|
| FR-01 | Fire Detection | Integrate NASA FIRMS data to display satellite-detected hotspots, timestamps, and detection attributes. |
| FR-02 | Weather Forecasts | Display wind speed and direction, temperature, humidity, and precipitation forecasts. |
| FR-03 | Interactive Maps | Provide interactive maps with satellite, terrain, fire detection, and weather layers. |
| FR-04 | Geospatial Imagery | Display satellite imagery and relevant geographic information for affected areas. |
| FR-05 | Data Filtering | Filter information by location, date, data source, and relevant parameters. |
| FR-06 | Historical Analysis | View historical fire detections and changes in activity over time. |
| FR-07 | Risk Indicators | Highlight potentially affected areas using fire detections, weather conditions, and geographic context. |
| FR-08 | Alerts | Notify users of new fire detections or predefined risk conditions in monitored areas. |
| FR-09 | Reporting | Export maps, selected datasets, and basic operational summaries. |
| FR-10 | Live Updates | Push new detections, incident changes, and alerts to the dashboard without manual refresh. |
| FR-11 | Fire Incidents | Group related detections into incidents and show each incident's lifecycle, growth, and weather context. |
| FR-12 | Watch Areas | Let users define monitored areas that drive alerts. |
| FR-13 | Field Reports | Let responders confirm or dismiss detections and acknowledge alerts. |
| FR-14 | Air Quality | Display haze and particulate-matter (PM2.5) observations and forecasts. |
| FR-15 | Pipeline Status | Show data freshness, processing lag, and rejected-event counts for each data source. |
| FR-16 | Historical Replay | Replay past detections and incidents over a selected time range. |

## 4. Non-Functional Requirements

| ID | Requirement | Description |
|---|---|---|
| NFR-01 | Availability | Ensure high availability for operational use. |
| NFR-02 | Performance | Provide responsive maps and timely access to updated information. |
| NFR-03 | Data Freshness | Refresh data automatically and clearly display observation and forecast timestamps. |
| NFR-04 | Scalability | Support multiple users and geographic expansion across Southeast Asia. |
| NFR-05 | Usability | Provide an intuitive, map-first interface suitable for emergency response teams. |
| NFR-06 | Compatibility | Support desktop and tablet devices. |
| NFR-07 | Security | Implement secure access, authentication, and appropriate data protection. |
| NFR-08 | Reliability | Handle unavailable or delayed data sources and clearly indicate data limitations. |
| NFR-09 | Event Sourcing | Record observations, forecasts, and user actions as events in Kafka; derived stores must be rebuildable by replaying them. |
| NFR-10 | Schema Governance | Version and register event schemas; route invalid events to dead-letter topics without halting processing. |

## 5. Data Sources and Integrations

| Data Source | Information | Priority |
|---|---|---|
| NASA FIRMS (VIIRS / MODIS) | Satellite fire detections, locations, timestamps, and confidence indicators | Required |
| Meteorological forecast provider (e.g., ECMWF) | Wind, temperature, humidity, precipitation, and forecasts | Required |
| Satellite imagery (e.g., Sentinel / Landsat) | Geospatial imagery and environmental context | Required |
| Geographic basemaps | Terrain, roads, administrative boundaries, and geographic features | Required |
| Population and infrastructure datasets | Settlements, critical infrastructure, and potentially exposed assets | Optional |
| Land-cover and vegetation datasets | Forest cover, vegetation, peatlands, and land use | Optional |
| Air quality (Open-Meteo Air Quality, OpenAQ) | PM2.5 and PM10 forecasts and ground-sensor observations for haze monitoring | Recommended |
| Geostationary fire detections (JAXA Himawari-9) | 10-minute wildfire detections covering Southeast Asia | Optional |
| User-generated events | Watch areas, field reports, and alert acknowledgements | Recommended |

**Data considerations:** Satellite detections are not confirmed fire perimeters. The platform must distinguish observations from forecasts, identify data sources and timestamps, and communicate relevant uncertainty.

---

**Note:** Technology stack, system architecture, infrastructure, and detailed implementation specifications will be defined in subsequent project phases.