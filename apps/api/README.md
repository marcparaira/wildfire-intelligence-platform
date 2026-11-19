# API

FastAPI application package. Keep HTTP routes and API concerns here; independent
data ingestion and stream-processing workers belong under the repository's
`services/` directory when those phases are implemented.

The `/health` endpoint is an application liveness check. Database and Kafka
configuration is provided through environment variables for local development.
