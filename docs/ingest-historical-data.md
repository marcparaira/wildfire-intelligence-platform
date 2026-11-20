# Ingesting Historical Fire Data

To populate the Growth over time chart with historical data spanning multiple days, you can use the FIRMS ingestor to fetch data from recent days.

## Prerequisites

You need a NASA FIRMS API key. If you don't have one:

1. Register at [NASA FIRMS MAP_KEY](https://firms.modaps.eosdis.nasa.gov/api/area/)
2. Save the key to `.firms-map-key` file in the repository root

## Fetching Historical Data

The FIRMS ingestor can fetch up to **5 days** of historical data in a single run:

```bash
export FIRMS_MAP_KEY=$(cat .firms-map-key)
podman compose run --rm -e FIRMS_MAP_KEY="$FIRMS_MAP_KEY" ingestor python -m wildfire_ingestor.firms --days 5
```

### Options

- `--days N` (1-5, default=1): Number of past days to fetch
- `--product PRODUCT` (default=VIIRS_NOAA20_NRT): Specific FIRMS product to fetch
  - Supported: `VIIRS_SNPP_NRT`, `VIIRS_NOAA20_NRT`, `VIIRS_NOAA21_NRT`, `MODIS_NRT`

### Example: Fetch 5 days of all products

```bash
export FIRMS_MAP_KEY=$(cat .firms-map-key)
podman compose run --rm -e FIRMS_MAP_KEY="$FIRMS_MAP_KEY" ingestor python -m wildfire_ingestor.firms --days 5
```

## How It Works

1. **Deduplication**: The ingestor checks `firms.detections` and skips detections already published
2. **Stream Processing**: New detections flow through Kafka to the stream processor
3. **Incident Clustering**: The stream processor aggregates detections into incidents by space and time
4. **Daily Aggregation**: Incidents accumulate daily statistics automatically
5. **Persistence**: All data is persisted in the database across restarts

## Results

After running the ingestor:
- Detections are published to Kafka
- The stream processor clusters them into incidents
- Incidents accumulate days of data in their `daily` field
- The Growth over time chart on the frontend displays all available days

Example: Before ingestion, fires showed 1-3 days. After fetching 5 days:
- Incidents now show daily progression from 2026-11-16 through 2026-11-20
- Each day's detections, new cells, and max FRP are tracked
- The Growth over time chart displays the complete timeline

## Notes

- The ingestor is idempotent: running it multiple times with the same data won't create duplicates
- Data is filtered to selected countries (Myanmar, Thailand, Laos, Vietnam, Cambodia)
- Maximum API limit is 5 days per request; older data requires separate requests
- Database persists data across `podman compose down/up` cycles
