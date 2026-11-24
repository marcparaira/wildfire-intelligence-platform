# Event Pipeline Operations

Kafka is the system of record. PostGIS tables are projections of the log that
the sink worker maintains and can rebuild at any time by replaying the topics.
Derived topics, such as `fire.incidents`, are computed from the source topics
by the stream processor and can be recomputed the same way.

## Topics

| Topic | Key | Partitions | Retention | Contents |
|---|---|---|---|---|
| `firms.detections` | H3 cell, resolution 2 | 6 | Unlimited (`retention.ms=-1`) | One `FirmsDetection` per FIRMS pixel and product |
| `weather.forecasts` | H3 cell, resolution 2 | 6 | Compacted | Latest `WeatherForecast` (7 days, hourly) per cell |
| `fire.incidents` | Incident ID | 6 | Compacted | Latest `FireIncident` revision per incident |
| `detections.enriched` | H3 cell, resolution 2 | 6 | Unlimited | One `EnrichedDetection` (country, weather, risk) per detection |
| `stats.hourly` | `<country>\|<hour start>` | 6 | Compacted | Latest `HourlyDetectionStats` per country and UTC hour |
| `imagery.scenes` | Scene ID | 6 | Compacted | Latest `SatelliteScene` (image metadata) per layer, day (or Sentinel-2 pass), and tile |
| `sink-worker.dlq` | Original key | 1 | 30 days | Records the sink worker could not process |
| `stream-processor.dlq` | Original key | 1 | 30 days | Records the stream processor could not decode |

The stream processor also uses Quix Streams internal topics named after each
job's consumer group: a compacted `changelog__…` topic per job that backs its
state, and two repartition topics with 3-day retention,
`repartition__stream-processor.incidents--firms.detections--source-partition`
and `repartition__stream-processor.statistics--detections.enriched--country`.

Topics are defined in `packages/events/src/wildfire_events/topics.py` and
created by the services at startup; the broker's automatic topic creation is
disabled. Existing topics are never reconfigured automatically.

**Keys and co-partitioning.** A detection's key is the resolution-2 H3 cell
that contains it, and the weather grid consists of exactly those cells. Because
both source topics have the same partition count, a detection and the forecast
for its cell always land on the same partition number. Stream-processing joins
(Phase 8) rely on this. Changing `PARTITIONS` requires recreating the topics
and replaying.

**Event IDs.** Each detection's `event_id` is a UUIDv5 of its product,
satellite, UTC acquisition time, and the provider's coordinate strings, and is
sent in the `event_id` record header. The ingestor reads these headers back
from the last `FIRMS_DAYS + 1` days of the topic at startup and does not
republish a known ID. The sink worker stores each ID once.

## Schemas

Values use the Confluent wire format: a zero byte, a 4-byte schema ID, and
UTF-8 JSON. The JSON Schemas are generated from the pydantic models and checked
in under `packages/events/src/wildfire_events/schemas/`; a test fails if they
drift from the models. Producers register the checked-in schema under the
`<topic>-value` subject when they start. The registry runs with `BACKWARD`
compatibility, so a producer with an incompatible schema fails at startup
instead of publishing.

To evolve a schema, add optional fields with defaults; do not remove, rename, or
retype fields. Then run `python -m wildfire_events.schemas` and commit the
regenerated file. Browse subjects and versions in Kafka UI
(<http://localhost:8090>) or the registry API
(<http://localhost:18081/subjects>).

## Dead letters

Each consuming service has its own dead-letter topic. The stream processor
sends a detection to `stream-processor.dlq` when it cannot be decoded (the
first two cases below). The sink worker sends a record to `sink-worker.dlq`
and continues when:

- the value is not in the wire format, or its schema ID is not registered for
  the topic's subject;
- the JSON does not validate against the event model;
- the database rejects the event for a data reason (for example a constraint),
  in which case the rest of the batch is still applied.

The dead-letter record keeps the original key, value, and headers, and adds
`dlq.original.topic`, `dlq.original.partition`, `dlq.original.offset`,
`dlq.consumer.group`, `dlq.error`, and `dlq.failed.at`. Inspect it in Kafka UI,
or with:

```sh
podman compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic sink-worker.dlq --from-beginning \
  --property print.headers=true
```

Transient failures are not dead-lettered: if PostGIS, Kafka, or the schema
registry is unavailable, the affected job restarts from its last committed
offset after 5 seconds. Offsets are committed only after the PostGIS
transaction and any dead-letter writes succeed, and projections are idempotent,
so reprocessing is safe. A transient failure can dead-letter the same invalid
record more than once.

## Sink worker jobs

| Job | Consumer group | Tables |
|---|---|---|
| Detections | `sink-worker.postgis.detections` | `fire_detections` |
| Weather | `sink-worker.postgis.weather` | `weather_cells`, `weather_hourly` |
| Incidents | `sink-worker.postgis.incidents` | `fire_incidents` |
| Enrichment | `sink-worker.postgis.enrichment` | `detection_conditions` |
| Statistics | `sink-worker.postgis.statistics` | `detection_stats_hourly` |
| Imagery | `sink-worker.postgis.imagery` | `satellite_scenes` |

Each job has its own consumer group and database connection, so the jobs track
their offsets independently. The weather projection ignores a forecast that is
older than the one stored for its cell, and deletes a cell when it receives a
tombstone (a record with a null value). The incidents projection keeps the
latest revision of each incident and never replaces it with an older one.
`detection_conditions` holds one row per detection `event_id`; it has no
foreign key to `fire_detections` because the two jobs run independently. The
statistics projection keeps the latest value per country and hour. The imagery
projection keeps the newest acquisition of each scene, with the tile as a
`footprint` polygon. Incident, statistics, and scene tombstones delete the row.

## Stream processor

The stream processor (`services/stream-processor`) runs stateful jobs with
[Quix Streams](https://quix.io/docs/quix-streams/introduction.html). Each job
is its own Quix application and consumer group, and runs in its own process;
`python -m wildfire_processor` starts all of them and stops the others when one
exits (`--job <name>` runs a single job). All jobs send records they cannot
decode to `stream-processor.dlq`, and all are exactly-once.

| Job | Consumer group | Reads | Writes |
|---|---|---|---|
| Incidents | `stream-processor.incidents` | `firms.detections` | `fire.incidents` |
| Enrichment | `stream-processor.enrichment` | `firms.detections`, `weather.forecasts` | `detections.enriched` |
| Statistics | `stream-processor.statistics` | `detections.enriched` | `stats.hourly` |

The consumer groups can be overridden with `INCIDENTS_CONSUMER_GROUP`,
`ENRICHMENT_CONSUMER_GROUP`, and `STATISTICS_CONSUMER_GROUP`. Quix keeps each
job's local state under `STATE_DIR/<consumer group>`.

### Fire incidents

**Clustering.** Each detection is placed in a resolution-7 H3 cell (about
5 km², 1.4 km edge). It joins the incident that owns that cell or one of its
six neighbours (roughly 2.5–4 km), provided the cell was detected within
48 hours of the detection. Without a match it starts a new incident. If it
links two or more incidents, the oldest absorbs the others. Incidents stay
within one resolution-2 cell (the Kafka key).

**Lifecycle.** Statuses are judged against the stream clock: the newest
observation time the processor has seen in the partition, capped at each
detection's ingestion time.

| Status | Rule |
|---|---|
| `new` | First observed less than 24 hours ago |
| `growing` | Gained footprint cells in the last 24 hours (after the first 24 hours) |
| `stable` | Still active, but no new cells in the last 24 hours |
| `inactive` | No detection for 48 hours; final |
| `merged` | Absorbed by an older incident (`merged_into`); final |

**Output.** Every change publishes a new revision, keyed by `incident_id`, with
`revision` increasing by one and `previous_status` set when the status changed.
A revision carries the first and last observation times, detection count,
footprint (distinct resolution-7 cells and an approximate area), mean position,
bounding box, maximum and total FRP, products, and a `daily` list of detections,
new cells, and maximum FRP per UTC day. Compaction keeps the latest revision of
each incident. Cross-sensor observations of the same fire each count as a
detection. The parameters are constants in
`services/stream-processor/src/wildfire_processor/incidents.py`.

**State and determinism.** Quix Streams keeps state per message key, but a
detection's key (its H3 cell) can stop receiving data, and then its incidents
would never age. The processor therefore re-keys each detection by its source
partition (the repartition topic), so each partition shares one state and one
clock. Per-partition order is preserved, and incident IDs derive from the
first detection's `event_id`, so reprocessing the same detections produces the
same incidents and revisions. Processing is exactly-once: state, outputs, and
offsets commit in one Kafka transaction. Local state lives in a tmpfs and is
restored from the changelog topic on every start.

A status change that only depends on time passing, such as becoming inactive,
is applied when the next detection arrives in the same partition. A detection
that arrives after its incident became inactive starts a new incident.

### Weather enrichment

The enrichment job joins each detection with the forecast for its H3 cell.
Detections and forecasts share the cell key and partition count, so the job
reads both topics together, in record-timestamp order, and keeps per-cell
state:

- **Weather history.** Each forecast's hourly values are merged into the
  cell's history; a newer forecast (by `generated_at`) replaces the hours it
  covers, and an older one never does. Hours more than 72 hours before the
  newest forecast's first hour are dropped. A forecast tombstone clears the
  cell's history. Keeping past hours matters because Open-Meteo forecasts start
  at 00:00 UTC of the current day, while FIRMS can deliver detections observed
  the previous day.
- **Detections.** The weather hour is the observation time rounded to the
  nearest hour (half past rounds up). If the history holds that hour, the
  detection gets its temperature, precipitation, wind, the retrieval time of
  the forecast that supplied it, and the precipitation over the 24 hours ending
  at that hour (null unless all 24 hours are known). Otherwise `weather` is
  null. The country comes from the bundled Natural Earth boundaries.

The output, keyed like the detection, goes to `detections.enriched`.

### Fire risk indicator

Each enriched detection carries a simple, points-based risk indicator. It is a
screening heuristic for the region's conditions, not a calibrated fire danger
rating such as the Canadian Fire Weather Index. Four factors score 0–3 points
each (lower bounds inclusive):

| Factor | 0 | 1 | 2 | 3 |
|---|---|---|---|---|
| Wind speed, 10 m | < 10 km/h | 10–20 km/h | 20–30 km/h | ≥ 30 km/h |
| Temperature, 2 m | < 25 °C | 25–30 °C | 30–35 °C | ≥ 35 °C |
| Dryness: precipitation over 24 h | ≥ 10 mm | 2–10 mm | 0.2–2 mm | < 0.2 mm |
| Fire intensity: FRP | unknown or < 10 MW | 10–50 MW | 50–100 MW | ≥ 100 MW |

The score is the sum (0–12): **low** 0–3, **moderate** 4–6, **high** 7–9,
**extreme** 10–12. The risk is null when the temperature, wind speed, or
24-hour precipitation is unknown. The rules are constants in
`services/stream-processor/src/wildfire_processor/risk.py`.

The dashboard shows an incident's **peak risk**: the highest score among
detections in the incident's H3 cell during its last 24 hours of activity that
lie within its bounding box (plus 0.01°). Detections are not linked to
incidents, so this is approximate where incidents are close together.

### Hourly statistics

The statistics job counts detections and sums fire radiative power per
country and UTC hour of observation, using a one-hour tumbling window with a
3-day grace period on the observation time. Every update publishes the running
totals (`detections`, `detections_with_frp`, `total_frp_mw`, `max_frp_mw`) to
the compacted `stats.hourly` topic, so the latest record per key is final once
the window closes. Detections outside the selected countries are not counted,
and detections that arrive more than 3 days after a newer observation in the
same country was counted are dropped (Quix logs them). Cross-sensor
observations of the same fire each count. Query them with
`GET /api/stats/hourly?country=IDN&since=…&until=…` (default: the last 48 hours).

### Reprocessing

After changing a job's rules, recompute its output from the start. For
example, for the incidents job:

```sh
podman compose stop stream-processor
podman compose run --rm --no-deps stream-processor python -m wildfire_processor.reset --yes
podman compose start stream-processor
```

Add `--job enrichment` or `--job statistics` to reset another job (the default
is `incidents`).

The reset refuses to run while the processor is active. It deletes the
consumer group and the internal topics; the tmpfs state is gone after the stop.
The output topic is kept. For incidents, with unchanged rules the replay
republishes identical records. If the rules changed, incidents that no longer
exist keep their last revision in the topic. Delete and recreate
`fire.incidents` as well when downstream consumers must not see them.

**Enrichment and statistics.** A replayed enrichment appends a second copy of
every detection to `detections.enriched`, and the statistics job counts
records, so it would count them twice. Recompute both together, starting from
empty derived topics:

```sh
podman compose stop stream-processor sink-worker
for topic in detections.enriched stats.hourly; do
  podman compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic "$topic"
done
for job in enrichment statistics; do
  podman compose run --rm --no-deps stream-processor \
    python -m wildfire_processor.reset --job "$job" --yes
done
podman compose run --rm --no-deps sink-worker python -m wildfire_sink.rebuild --yes
podman compose start stream-processor sink-worker
```

The services recreate the deleted topics at startup, and the
[PostGIS rebuild](#rebuilding-postgis-by-replaying-the-topics) reloads the
projections. Enrichment can only use the forecasts that the compacted
`weather.forecasts` topic still holds, so replayed detections older than about
three days before the newest forecast get no weather and no risk. Resetting
only the statistics job is safe on its own.

## Pipeline status

The dashboard's **Pipeline status** page (`#/status`) and
`GET /api/pipeline/status` show the health of each source. The page refreshes
every 15 seconds; the API caches results for 10 seconds.

| Measure | Source | Meaning |
|---|---|---|
| Freshness | PostGIS | Age of the newest observation (`observed_at`) per FIRMS product, or of the newest forecast retrieval (`generated_at`) |
| Last ingested / stored | PostGIS | When the ingestor fetched the newest record, and when the sink worker wrote it |
| Ingested 1 h / 24 h | PostGIS | Records per source by fetch time (`ingested_at`, `generated_at`), so replays do not inflate them |
| Messages 1 h / 24 h | Kafka | Records appended to each topic, from offsets by timestamp |
| Consumer lag | Kafka | End offset minus committed offset, per sink-worker group and partition |
| Dead letters | `*.dlq` topics | Records in the last 24 hours by original topic, and the 10 newest with their errors |

Freshness thresholds:

| Source | Fresh | Delayed | Stale |
|---|---|---|---|
| FIRMS products | ≤ 18 h | ≤ 36 h | > 36 h |
| Open-Meteo forecast | ≤ 6 h | ≤ 24 h | > 24 h |

Polar-orbiting satellites pass over the region about twice a day and FIRMS NRT
data arrive about three hours after acquisition, so a healthy product's newest
detection can be 15 hours old. The forecast thresholds assume the default
3-hour `WEATHER_INTERVAL_MINUTES`. `No data` means PostGIS holds nothing for
the source (for example, a product not listed in `FIRMS_PRODUCTS`); `Unknown`
means PostGIS could not be read.

Consumer lag includes the stream processor's groups; the enrichment group has a row for each of its two topics. A lag that keeps growing
means that consumer is stopped or failing; check
`podman compose logs sink-worker` or `podman compose logs stream-processor`. A consumer group in the `Empty` state has no
running members. After a replay, lag counts down to zero.

## Satellite imagery

The imagery worker (`services/imagery-worker`, package `wildfire_imagery`)
acquires the imagery used to confirm fires: Sentinel-2 L2A short-wave infrared
(bands B12, B8A, and B04) at 20 m, from Microsoft Planetary Computer (no API
key). Flames are bright in B12, burn scars lose B8A, and smoke is thin at these
wavelengths. Its job runs every `IMAGERY_INTERVAL_MINUTES` (default 60) when
`IMAGERY_LAYERS` includes `Sentinel-2_L2A_SWIR_B12-B8A-B04` (the default), and
reads the latest revision of every incident from the compacted `fire.incidents`
topic. The code is in `wildfire_imagery.sentinel`.

- **Tiles.** Images are tiles of a fixed 0.5° grid (`x<column>y<row>`, counted
  from 0°, 0°), not per-incident crops. Neighbouring incidents share tiles, and
  a growing incident only adds the tiles it newly reaches. A scene is one layer
  on one UTC pass day for one tile; its ID is a UUIDv5 of the three, so it is
  acquired once.
- **Which passes.** Every non-merged incident with detections among the last
  `IMAGERY_DAYS` (default 5) days, counted back from the newest incident
  activity rather than from today, so stale or replayed data still gets
  imagery. Only the tiles that contain its detections are used (a tile already
  shows the surroundings). Sentinel-2 passes every 2-5 days, so passes from 6
  days (`SENTINEL_CONTEXT_DAYS`) before an incident's first to 6 days after its
  last day of activity are taken. Every tile gets its first scene before any
  tile gets a second, so a backlog gives each incident one image first. For each
  tile, passes during the activity come first (the fire may be burning), then
  after it (the burn scar), then before it (the baseline), most recent first.
  Activity up to 6 days before the `IMAGERY_DAYS` window still counts, so its
  later passes are found.
- **Search.** One STAC search per run (`POST /api/stac/v1/search`) covers every
  needed tile (neighbours in a row merged into strips) and the whole date range,
  without granules over 90% cloud, following the `next` pages.
- **Scenes.** Each granule (110 km MGRS tile) of the pass whose footprint
  covers at least 1% of the tile is rendered over the tile at 2048 × 2048 px
  (about 27 m) by the Planetary Computer data API. Footprints, not bounding
  boxes, decide: at a swath edge the bounding box covers far more than the data.
  The renders are composited, clearest on top, into one WebP whose area without
  data stays transparent. Scenes with under 1% coverage are
  not stored and not retried until a restart; failed ones are retried after an
  hour. At most `SENTINEL_MAX_SCENES_PER_RUN` (default 200, most of an hour) per run.
- **Claim check.** The image is written to the imagery store first, under
  `<layer>/<day>/<tile>-<sha256 prefix>.webp`; then its `SatelliteScene`
  metadata is published: bounds, size, pass day and sensing time
  (`acquired_at`), `cloud_cover` (mean of the granules), `granules` (their IDs),
  retrieval time, hash, store key, and source URL. The image never travels
  through Kafka. Locally, the store is the `imagery-data` volume, written by the
  imagery worker and mounted read-only into the API (`IMAGERY_DIR`).
- **Restarts.** At startup the imagery worker reads the scene IDs already in
  `imagery.scenes` and does not fetch them again. Deleting the volume therefore
  does not refetch existing scenes; their images return 404 until the scenes are
  removed from the topic.

The API lists an incident's scenes with `GET /api/incidents/{id}/imagery` (tiles
intersecting its bounding box grown by 0.05°, from 6 days before its first to 6
days after its last UTC day of activity) and serves each
image with `GET /api/imagery/{scene_id}`. Images never change, so the API sends
them with an immutable cache header.

## Rebuilding PostGIS by replaying the topics

Use this after changing a projection's table layout, after data loss in
PostGIS, or to verify that the database is a faithful projection of the log.

1. Stop the sink worker so its consumer groups become empty:

   ```sh
   podman compose stop sink-worker
   ```

2. Drop the projection tables and reset the consumer groups:

   ```sh
   podman compose run --rm --no-deps sink-worker python -m wildfire_sink.rebuild --yes
   ```

   The command refuses to run while any sink-worker consumer is active. Keep
   `--no-deps`: without it, podman-compose 1.0.6 stops and tries to recreate
   PostgreSQL, Kafka, and the schema registry.

3. Start the sink worker. With no committed offsets, it consumes every topic
   from the earliest retained offset and recreates the tables:

   ```sh
   podman compose start sink-worker
   podman compose logs -f sink-worker
   ```

The rebuild is complete when the consumer lag on the dashboard's Pipeline
status page (or in Kafka UI) reaches zero.
Cross-sensor duplicate marking is order independent, so a replay produces the
same `duplicate_of` values as live processing. Row `id` values are reassigned.

To replay only one job, stop the worker and reset that group's offsets with
Kafka's own tooling instead, for example:

```sh
podman compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group sink-worker.postgis.weather \
  --reset-offsets --to-earliest --topic weather.forecasts --execute
```

A replay can only restore what Kafka still retains. `firms.detections` keeps
events indefinitely. `weather.forecasts` keeps the latest forecast per cell,
which is all the projection needs. `podman compose down -v` deletes the Kafka
volume and therefore the history.
