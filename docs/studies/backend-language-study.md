# Backend Language Selection — Python vs. Rust

**Project:** Wildfire Monitoring & Decision Support Platform  
**Version:** 0.1  
**Status:** Proposed decision  
**Repository path:** `docs/studies/backend-language.md`

## 1. Requirements

### Hard requirements

- Integrate NASA FIRMS, weather APIs, Kafka, PostgreSQL/PostGIS, and S3-compatible storage.
- Support REST APIs, scheduled ingestion, asynchronous processing, and geospatial transformations.
- Run in independently deployed Podman containers with separate build and runtime stages.
- Support production security updates, automated testing, and operational monitoring.

### Soft requirements

- Mature geospatial and meteorological libraries.
- Rapid development and maintainability.
- Low CPU and memory consumption; predictable latency.
- Small, reproducible runtime images and straightforward builds.
- Availability of developers, documentation, and integrations.

## 2. Options Considered

| Option | Hard requirements | Result |
|---|---|---|
| Python + FastAPI | Meets all, subject to dependency validation | Retain |
| Rust + Axum | Meets all, subject to library integration validation | Retain |

**Eliminated:** None. Neither option demonstrably violates a hard requirement. Python on Alpine Linux is the currently selected *Python container base*, not a language-neutral hard requirement. Rust would use its own minimal runtime image if selected.

## 3. Comparative Benchmark

*Qualitative assessment based on ecosystem and runtime characteristics; **no project-specific performance tests have been run**.*

| Criterion | Python | Rust | Advantage |
|---|---|---|---|
| Geospatial / raster / weather ecosystem | Mature: GeoPandas, Rasterio, xarray, GDAL | Capable: geo, GDAL bindings; less integrated scientific tooling | Python |
| API and ingestion development speed | High | Moderate | Python |
| CPU-intensive custom processing | Often requires native/vectorized libraries | Strong native compiled performance | Rust |
| Memory use and runtime overhead | Generally higher | Generally lower | Rust |
| Container deployment | Straightforward; native dependencies may complicate Alpine | Compact compiled deployment possible; native GDAL still adds dependencies | Rust |
| Maintenance and iteration | Strong for data-centric applications | Strong compile-time correctness; steeper implementation effort | Python for MVP |

**Reproducible performance benchmark for later validation:** Implement the same FIRMS parsing, Kafka-to-PostGIS ingestion, spatial query API, and representative raster operation in both languages. Measure throughput, p95 latency, peak RSS memory, CPU, image size, build duration, and engineering effort under identical workload and hardware. Do not use generic language benchmark figures as project measurements.

## 4. Final Selection

**Select Python + FastAPI for the initial backend.** The workloads are integration- and geospatial-data-heavy, where ecosystem coverage and iteration speed outweigh unproven runtime performance gains. Rust remains an option for isolated, profiled CPU-intensive workers.

**Container decision:** Use a multi-stage Podman build with **Python on Alpine Linux** as the proposed runtime. Before finalizing the image, verify installation, compatibility, image size, security updates, and performance of actual native GIS dependencies (especially GDAL/Rasterio). If Alpine creates material integration or maintenance risks, evaluate a slim glibc-based Python image as an explicit follow-up decision.

## References

- [GeoPandas installation and native dependencies](https://geopandas.org/en/stable/getting_started/install.html)
- [Rasterio installation and GDAL dependencies](https://rasterio.readthedocs.io/en/stable/installation.html)
- [Rust GDAL bindings](https://docs.rs/crate/gdal/latest)
- [PyO3: Python/Rust integration](https://github.com/PyO3/pyo3)
