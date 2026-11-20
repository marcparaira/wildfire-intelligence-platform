"""ASGI entry point: assembles the API from its routers."""

from fastapi import FastAPI

from wildfire_api.hotspots import router as hotspots_router
from wildfire_api.incidents import router as incidents_router
from wildfire_api.live import router as live_router
from wildfire_api.pipeline.routes import router as pipeline_router
from wildfire_api.stats import router as stats_router
from wildfire_api.weather import router as weather_router

app = FastAPI(title="Wildfire Intelligence Platform API", version="0.2.0")


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(hotspots_router)
app.include_router(weather_router)
app.include_router(live_router)
app.include_router(pipeline_router)
app.include_router(incidents_router)
app.include_router(stats_router)
