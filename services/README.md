# Data services

Independent workers belong here as the ingestion and stream-processing phases
are implemented. Give each service its own package, tests, and multi-stage
container definition; keep HTTP API concerns in `apps/api`.
