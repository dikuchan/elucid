# Elucid v0 Showcase Specification

This document defines the executable two-week delivery target. It is intentionally narrower than a production SIEM and is the priority order for implementation.

## 1. Outcome

One documented command starts PostgreSQL, MinIO, one Elucid server, and its persistent spool volume. A reviewer can ingest deterministic NDJSON through the checked-in Vector integration, observe durable asynchronous publication, inspect the catalog, run typed queries in a browser, restart Elucid without losing an acknowledged batch, observe dead letters, and watch automatic compaction reduce small segments without changing query results.

The showcase proves a coherent local vertical slice. It does not prove horizontal scaling, replicated durability, authentication, distributed query execution, Tantivy indexing, or production deployment readiness.

## 2. Topology

```text
Vector / fixture client ─────────────HTTP NDJSON──┐
browser ─────────────────────────────HTTP──────────┤
                                                   v
                                             Elucid server
                                            /      |       \
                                  local spool   PostgreSQL   MinIO
```

The Elucid container mounts one persistent volume for acknowledged spool data and separate bounded scratch space for replaceable staging and query spill. PostgreSQL and MinIO use their own persistent Compose volumes.

The server runs ingestion, query, UI, automatic compaction, retention, and garbage collection. There is no proxy, separate frontend server, service-role topology, or authentication layer.

## 3. Repository and startup

The repository contains:

- one Rust workspace and one release server image;
- one production UI build embedded in the server image;
- SQLx migration files embedded in the binary;
- one Compose definition with pinned major dependency versions and health checks;
- one integration-tested configuration for a pinned Vector release;
- deterministic catalog and NDJSON fixtures;
- one quickstart documenting startup, verification, restart, logs, metrics, and cleanup.

`docker compose up --build` is the golden startup command. It requires no private registry, product credential, or separately provisioned service. A clean machine may need ordinary network access to download public build dependencies and container images.

## 4. Demo data

The demo source is `demo_logs`. Its first schema contains non-null `message`, nullable `service`, and nullable `status`. The fixture contains valid security events across two UTC event days, duplicate byte-identical events at different positions, unknown top-level values captured in `@rest`, blank lines, malformed JSON, an invalid typed value, and one late event for the older day.

The sender submits several bounded batches so ingestion initially publishes multiple small segments per day. The exact fixture counts and expected sorted query results are committed as data, not duplicated as hand-maintained prose constants.

One later catalog revision adds nullable `region` with `historical_remainder_pointer: /region`, followed by a profile revision that promotes `/region` for new events.

## 5. Vector integration

The checked-in example uses Vector's HTTP sink with JSON encoding, newline-delimited framing, `Content-Type: application/x-ndjson`, no compression, a batch byte limit with measured headroom below Elucid's advertised encoded-body limit, and a batch event limit below Elucid's framed-record limit. Its health check targets `/health/ready`. It uses a bounded disk buffer with `when_full: block`; buffer storage is mounted persistently in the example deployment.

The pinned integration MUST verify that Vector treats `202` as delivered, does not retry permanent `400`, `404`, `413`, or `415` responses, and retries network failures, `429`, and retryable `5xx` responses with bounded backoff. The test also documents that a timeout or lost response after Elucid's durable append can cause Vector to resend the same events as a new batch, producing duplicates by contract.

When PostgreSQL or MinIO is unavailable, Elucid closes readiness and rejects new batches with retryable `503` before taking ownership. Vector therefore retains those events in its persistent disk buffer and retries them after recovery; Elucid's local spool remains responsible for requests that already passed admission.

Elucid accepts ordinary Vector-produced NDJSON and does not require Vector-specific headers, event identities, transforms, or acknowledgement extensions. The example is an operator starting point, not a second ingestion protocol.

## 6. Web application

The browser application provides:

- source selection and active-schema history;
- an editor for the v0 query language and explicit UTC time range;
- typed result columns and values;
- diagnostics with source spans;
- completion or truncation, elapsed time, selected segments, and selected bytes;
- a bounded segment list showing ingestion inputs and compaction outputs;
- spool backlog, dead-letter summaries, and compaction status.

Loading, empty, success, truncated, and error states are distinct. Rows and JSON values are safely rendered as text. The release application does not load scripts, fonts, styles, or source maps from external origins.

## 7. Golden path

Implementation and review proceed in this order:

1. Start the complete Compose stack and apply SQLx migrations automatically.
2. Apply the demo catalog and inspect its source, schema, and input in the UI.
3. Send the fixture through the checked-in Vector configuration and receive `202 DURABLY_QUEUED` after local fsync rather than S3 publication.
4. Observe spool backlog drain and immutable Parquet segments become `ACTIVE`.
5. Run filter, projection, sorting, limiting, and aggregation queries in the browser and compare deterministic results.
6. Inspect malformed records through the dead-letter UI/API while valid records from the same batches remain queryable.
7. Apply the additive `region` schema and profile revisions and verify one query over old remainder-backed and new promoted values.
8. Restart Elucid after acknowledging a batch but before its publication; verify recovery publishes each durable local occurrence once without requiring the sender to resend it.
9. Let automatic compaction replace small same-day segments and verify the sorted logical query result is unchanged.
10. Observe the ingestion, spool, Parquet, PostgreSQL publication, query, compaction, and object-deletion metrics needed for a traffic bottleneck investigation.

Automatic compaction is deliberately implemented after the working ingestion-query-UI path. It is nevertheless required for completion of the showcase.

## 8. Required correctness checks

Automation protects these observable behaviors:

- server startup applies embedded SQLx migrations to a fresh database and succeeds on an unchanged restart.
- `202` is not returned before the spool append is durable.
- the pinned Vector configuration emits compatible NDJSON batches and follows the documented success, permanent-failure, retry, and ambiguous-response behavior.
- a PostgreSQL or object-store outage makes readiness and new ingestion admission return `503` before ownership, while a request that already passed admission can still become durable and publish after recovery.
- a torn final spool write is discarded while all earlier acknowledged batches recover.
- a lost HTTP response followed by sender retry may create two occurrences and is documented as such.
- replay after Elucid restart does not duplicate one already acknowledged local occurrence.
- query planning uses exact PostgreSQL object references and never S3 listing.
- schema promotion reads old values through the declared historical remainder adapter and rejects an undeclared field typo.
- compaction publication exposes either all inputs or all outputs and preserves exact sorted rows.
- no object is deleted before query-snapshot grace expires.
- configured capacity exhaustion returns bounded backpressure instead of consuming unbounded memory or disk.

Tests should be placed at the cheapest faithful level. Parser/type rules belong in unit tests; PostgreSQL/S3 publication, spool recovery, schema adaptation, and compaction replacement require focused integration tests; the browser needs only the critical golden journey.

## 9. Delivery exclusions

The showcase does not include:

- `Idempotency-Key` or exactly-once HTTP semantics;
- ingestion request, attempt, or commit APIs;
- active-active processes or concurrent maintenance owners;
- role-based routing, authentication, TLS termination, or CORS modes;
- source-statistics materialization or exact counts in the source list;
- full provenance history after objects and terminal metadata are reclaimable;
- joins, distributed execution, continuous alerts, or Tantivy;
- exhaustive failure certification for every PostgreSQL or object-store instruction boundary.

These exclusions are product scope, not claims that the omitted concerns never matter.
