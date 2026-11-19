# Known limitations
- Cross-sensor duplicates are only worked out in PostGIS, so a live detection can disappear on Refresh data if it turns out to be a duplicate.
- Each open dashboard holds its own Kafka consumer, which is fine at this scale.