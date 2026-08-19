# prism

A columnar store for telemetry. No server, any schema. A schema declares its
fields and gets its own namespace for querying, while storage, partitioning,
and query stay generic.

Parquet on object storage, DataFusion for query.

OpenTelemetry traces arrive over OTLP through a separate optional receiver that
maps them onto this schema. Nothing in the core depends on OpenTelemetry or protobuf.
