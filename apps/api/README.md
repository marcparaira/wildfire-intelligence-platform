# API

FastAPI application package. Keep HTTP routes and API concerns here; independent
data ingestion and stream-processing workers belong under the repository's
`services/` directory.

The `/health` endpoint is an application liveness check. `GET /hotspots`
queries the sink worker's `fire_detections` projection and hides cross-sensor
duplicates unless `include_duplicates=true`. `GET /weather/forecast` returns
one forecast hour over the H3 forecast grid from the `weather_cells` and
`weather_hourly` projections, with the times when the forecasts were
retrieved. The API never calls FIRMS or Open-Meteo; the ingestor publishes
both to Kafka on a schedule. Database configuration is provided through
environment variables for local development.
