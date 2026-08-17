# prism

A columnar store for telemetry. No server, any schema. A schema declares its
fields and gets its own namespace for querying, while storage, partitioning,
and query stay generic.

Parquet on object storage, DataFusion for query.

Spans are the first schema: normally one span, one row. When the start and the
finish arrive separately — a cron run that dies before it can report — each
writes its own row and a read combines them by `(project_id, span_id)`. A span
that started and never finished simply has no `end_ts`.

OpenTelemetry traces arrive over OTLP through a separate optional receiver that
maps them onto this schema. Nothing in the core depends on OpenTelemetry or protobuf.
