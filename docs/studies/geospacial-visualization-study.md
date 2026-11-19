# Geospatial Visualization Library — Technology Selection Study

**Project:** Wildfire Monitoring & Decision Support Platform  
**Version:** 0.2  
**Status:** Proposed decision (qualitative evaluation; load testing pending)  
**Proposed repository path:** `docs/studies/geospatial-visualization.md`

## 1. Requirements

**Use case:** A React/Vite dashboard for Southeast Asian wildfire response, combining NASA FIRMS satellite hotspots, satellite images, weather forecasts, and geographic context on an interactive 2D map.

### Hard requirements (must meet)

| ID | Requirement | Minimum acceptance criterion |
|---|---|---|
| **H1** | **Frontend compatibility** | Works in a browser with React, TypeScript, and Vite. |
| **H2** | **Interactive operational map** | Supports 2D pan/zoom, feature selection, tooltips, and independently controlled map layers. |
| **H3** | **Fire hotspot visualization** | Displays georeferenced FIRMS detections with timestamps and attributes; supports time-based filtering. |
| **H4** | **Imagery and weather overlays** | Displays satellite imagery and weather overlays (e.g., wind, rainfall), including time-dependent layers; supports standard raster/map services such as WMS/WMTS, directly or via integration. |
| **H5** | **High-volume rendering path** | Offers a documented way to display large hotspot datasets (clustering, vector tiles, GPU rendering, or preprocessing). |
| **H6** | **Independent deployment** | Open-source client library that does not require a proprietary mapping platform or mandatory commercial API key. **Assumption: confirm before approval.** |

### Soft requirements (compare and rank)

| ID | Requirement | Evaluation focus |
|---|---|---|
| **S1** | **Geospatial interoperability** | Native GeoTIFF/COG, raster formats, WMS/WMTS, and coordinate-system support. |
| **S2** | **Responsive visualization** | Smooth interaction with dense hotspots and multiple weather layers; efficient temporal updates. |
| **S3** | **Developer productivity** | React integration, API clarity, documentation, ecosystem, and maintainability. |
| **S4** | **Client efficiency** | Browser bundle size, memory use, rendering overhead, and plugin complexity. |

**Out of scope:** Built-in fire spread prediction, routing/dispatch logic, source data accuracy, imagery licensing, and backend GIS processing. These are not requirements of the map library itself.

## 2. Options Considered and Hard-Requirement Screening

| Option | Screening | Reason |
|---|---|---|
| **OpenLayers** | **Pass** | Broad GIS raster/vector sources, WebGL rendering, open source. |
| **MapLibre GL JS** | **Pass** | Strong vector-tile mapping, hotspot clustering, raster/WMS support, open source. |
| **Leaflet** | **Pass** | Lightweight 2D mapping; large-data and advanced raster cases may require plugins or preprocessing. |
| **CesiumJS** | **Pass** | Open-source 2D/3D visualization and time-based data; 3D focus adds complexity. |
| ~~Mapbox GL JS (current proprietary releases)~~ | **Excluded — H6** | Current releases use commercial licensing rather than a permissive open-source client license. |
| ~~Google Maps JavaScript API~~ | **Excluded — H6** | Proprietary service, commercial terms, and API-key dependence. |
| ~~ArcGIS Maps SDK for JavaScript~~ | **Excluded — H6** | Proprietary SDK governed by Esri terms; valuable but not an open-source client library. |

**Complementary tool:** deck.gl is a GPU visualization framework that can enhance dense overlays, particularly with MapLibre; it is not selected as the primary map engine in this comparison.

## 3. Benchmark of Remaining Options

**Qualitative suitability assessment (not measured performance results):**

| Criterion | OpenLayers | MapLibre GL JS | Leaflet | CesiumJS |
|---|---|---|---|---|
| Satellite imagery / raster formats | **Excellent** | Good | Moderate | Good |
| Weather grids / GIS services | **Excellent** | Good | Moderate | Good |
| Dense hotspot rendering | Good–Excellent | **Excellent** | Moderate | Good |
| Time-dependent layer support | **Excellent** | Good | Good | **Excellent** |
| React/Vite integration | Good | **Excellent** | **Excellent** | Good |
| Simplicity for a 2D GIS dashboard | Good | **Excellent** | **Excellent** | Moderate |
| Overall fit for this wildfire platform | **Best fit** | Strong alternative | Suitable for simpler MVP | Better if 3D becomes core |

**Basis:** OpenLayers provides built-in GeoTIFF/COG, WMS/WMTS, WebGL raster/vector and wind-layer examples. MapLibre provides clustering, vector tiles, raster layers and optional deck.gl integration. Leaflet has basic WMS support but requires additional work for advanced GIS workflows. CesiumJS supports 2D and temporal visualization, but its strengths center on 3D/globe applications.

**Required validation before production:** Under identical browsers, devices and datasets, measure first-map render time, p95 pan/zoom frame rate, p95 time to update layers, memory usage and bundle transfer size. Test 10k / 100k / 500k FIRMS points (using appropriate clustering or tiles), 3 simultaneous weather rasters, 1 satellite image layer and 24 forecast frames. Compare with identical source formats and caching policies; no performance ranking above is a measured result.

## 4. Final Selection

**Select OpenLayers as the primary frontend geospatial visualization library.**

**Rationale:** This platform is a **multi-source GIS and weather monitoring dashboard**, not primarily a styled basemap. OpenLayers offers the strongest first-party support for raster imagery, COG/GeoTIFF, GIS service interoperability, wind/weather visualization and temporal layers. Its WebGL rendering paths also support dense visualizations.

**Runner-up:** MapLibre GL JS, especially if the implementation becomes mainly vector tiles and clustered hotspots with pre-rendered weather tiles. Reassess after the stated workload benchmark.

**Constraints:** This decision selects a browser mapping library only. It does not determine imagery providers, tile servers, backend geospatial processing, infrastructure or architecture.

## References

- OpenLayers examples (COG, WMS time, WMTS, wind, WebGL): https://openlayers.org/en/latest/examples/
- OpenLayers GeoTIFF API: https://openlayers.org/en/latest/apidoc/module-ol_source_GeoTIFF.html
- MapLibre GL JS features: https://maplibre.org/projects/gl-js/
- Leaflet WMS support and limitations: https://leafletjs.com/examples/wms/
- CesiumJS capabilities: https://cesium.com/platform/cesiumjs/
- deck.gl MapLibre integration: https://deck.gl/docs/developer-guide/base-maps/using-with-maplibre
- ArcGIS SDK licensing: https://developers.arcgis.com/javascript/latest/licensing/
