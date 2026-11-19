# API

FastAPI application package. Keep HTTP routes and API concerns here; independent
data ingestion and stream-processing workers belong under the repository's
`services/` directory.

The `/health` endpoint is an application liveness check. `POST /weather/forecast`
fetches seven days of hourly temperature, precipitation, and wind forecasts
from Open-Meteo on demand, stores them in PostgreSQL, and returns the selected
hour for the dashboard. Stored forecasts are reused for six hours unless the
caller sets `refresh=true`. The forecast grid is coarse and covers the
configured Southeast Asia bounds. Database configuration is provided through
environment variables for local development.
