# Event Pipeline Operations

Kafka is the system of record. PostGIS tables are projections of the log that
the sink worker maintains and can rebuild at any time by replaying the topics.

## Topics

| Topic | Key | Partitions | Retention | Contents |
|---|---|---|---|---|
| `firms.detections` | H3 cell, resolution 2 | 6 | Unlimited (`retention.ms=-1`) | One `FirmsDetection` per FIRMS pixel and product |
| `weather.forecasts` | H3 cell, resolution 2 | 6 | Compacted | Latest `WeatherForecast` (7 days, hourly) per cell |
| `sink-worker.dlq` | Original key | 1 | 30 days | Records the sink worker could not process |
| `firms.detections.v1` | none | 3 | Broker default | Legacy Phase 2-5 events; read only by `migrate_legacy` |

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

The sink worker sends a record to `sink-worker.dlq` and continues when:

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

Each job has its own consumer group and database connection, so the jobs track
their offsets independently. The weather projection ignores a forecast that is
older than the one stored for its cell, and deletes a cell when it receives a
tombstone (a record with a null value).

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

The rebuild is complete when the consumer-group lag in Kafka UI reaches zero.
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
