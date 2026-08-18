# Elucid Specifications

Elucid specifications are versioned as coherent product contracts. The current contract is [v0](v0/), with status `DRAFT`.

| Document | Contract owner |
|---|---|
| [Catalog](v0/catalog.md) | Sources, schemas, fields, inputs, ingest-profile revisions, and declarative catalog application |
| [Query Language](v0/query-language.md) | Syntax, semantics, typing, field resolution, and diagnostics |
| [Query Engine](v0/query-engine.md) | Planning snapshots, schema adaptation, DataFusion execution, and result bounds |
| [Storage](v0/storage.md) | Segments, Parquet objects, S3 keyspace, object lifecycle, and garbage collection |
| [Metastore](v0/metastore.md) | PostgreSQL representation, constraints, transactions, migrations, and repositories |
| [Ingestion](v0/ingestion.md) | HTTP ingest requests, framing, normalization, idempotency, attempts, publication, and recovery |
| [Compaction](v0/compaction.md) | Segment selection, provenance, fenced execution, atomic replacement, and recovery |
| [Retention](v0/retention.md) | Idempotency expiry, data and dead-letter expiration, object reclamation, and provenance pruning |
| [Service](v0/service.md) | Executable, configuration, lifecycle, CLI, HTTP API, errors, limits, and telemetry |
| [Showcase](v0/showcase.md) | Demonstrable v0 profile, fixture, web application, packaging, and verification |

Documents inside one version do not carry independent semantic versions. While a version is `DRAFT`, its documents change in place. After acceptance, a change to observable semantics, durable representation, or compatibility creates a new version directory; editorial corrections remain in the accepted directory.
