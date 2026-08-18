# prism

A columnar store for telemetry. No server, any schema. A schema declares its
fields and gets its own namespace for querying, while storage, partitioning,
and query stay generic.

Parquet on object storage, DataFusion for query.

Spans are the first schema: one span, one row. A sender that writes a span more
than once — a cron run checking in as it goes — has its newest row win, by
`(trace_id, span_id)`. A span that never finished has no `ended_at`.

OpenTelemetry traces arrive over OTLP through a separate optional receiver that
maps them onto this schema. Nothing in the core depends on OpenTelemetry or protobuf.
