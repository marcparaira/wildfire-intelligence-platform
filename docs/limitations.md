# Known limitations
- Cross-sensor duplicates are only worked out in PostGIS, so a live detection can disappear on Refresh data if it turns out to be a duplicate.
- Each open dashboard holds its own Kafka consumer, which is fine at this scale.
- Source freshness is based on the newest stored record. A FIRMS product that is polled successfully but has no new detections looks the same as one whose polls fail.
- Freshness thresholds are fixed in the API and do not follow a changed `WEATHER_INTERVAL_MINUTES` or `FIRMS_INTERVAL_MINUTES`.
