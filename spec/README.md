# Elucid Specifications

Elucid specifications are versioned as coherent product contracts. The current contract is [v0](v0/), with status `DRAFT`.

Deferred design work that is explicitly outside the v0 delivery scope lives in [v1](v1/). Those notes are not part of the current product contract.

| Document | Contract owner |
| --- | --- |
| [Catalog](v0/catalog.md) | Sources, additive schemas, inputs, immutable ingestion-profile revisions, and explicit historical adapters |
| [Query Language](v0/query-language.md) | Syntax, semantics, typing, field resolution, and diagnostics |
| [Query Engine](v0/query-engine.md) | Planning snapshots, schema adaptation, DataFusion execution, and result bounds |
| [Storage](v0/storage.md) | Segments, Parquet objects, S3 keyspace, object lifecycle, and garbage collection |
| [Metastore](v0/metastore.md) | Seven PostgreSQL product tables, constraints, visibility transactions, and SQLx migrations |
| [Ingestion](v0/ingestion.md) | HTTP NDJSON admission, durable local spool, at-least-once delivery, normalization, publication, and recovery |
| [Compaction](v0/compaction.md) | Bounded single-owner selection, atomic replacement, and recovery |
| [Retention](v0/retention.md) | Data and dead-letter expiration, object reclamation, and bounded metadata cleanup |
| [Service](v0/service.md) | One-process topology, configuration, lifecycle, HTTP API, embedded UI, limits, and telemetry |
| [Showcase](v0/showcase.md) | Two-week golden path, fixture, UI, packaging, and focused verification |

Documents inside one version do not carry independent semantic versions. While a version is `DRAFT`, its documents change in place. After acceptance, a change to observable semantics, durable representation, or compatibility creates a new version directory; editorial corrections remain in the accepted directory.
