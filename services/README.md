# Data services

`firms-pipeline/` contains the FIRMS ingestion command and the independently
running Kafka-to-PostGIS processor. They share a versioned event model and
container image, but run as separate processes. Keep HTTP API concerns in
`apps/api`.
