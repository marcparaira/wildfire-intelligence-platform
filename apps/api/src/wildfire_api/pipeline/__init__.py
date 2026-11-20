"""Pipeline health: per-source freshness, throughput, consumer lag, and dead letters.

Freshness and per-source throughput come from the PostGIS projections, which hold
the event times (``observed_at``, ``ingested_at``, ``generated_at``); see
``sources``. Topic throughput, consumer lag, and dead-letter counts come from Kafka
offsets and the dead-letter topics; see ``kafka_offsets``. Either half can fail on
its own: the response then carries the other half and names the failure in
``errors`` (``status``).
"""
