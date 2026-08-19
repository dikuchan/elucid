# Elucid v0 Service Specification

This document owns the executable, configuration, startup and shutdown, HTTP API, embedded UI delivery, limits, errors, and telemetry.

## 1. Executable and topology

V0 ships one Rust executable, `elucid`. `elucid server` runs HTTP ingestion, catalog, query execution, the embedded web application, the local ingestion worker, and maintenance loops in one process.

PostgreSQL, one S3-compatible object store, and one persistent local volume are external dependencies. V0 does not require or promise active-active instances, service-role routing, request affinity, leader election, distributed query execution, or replicated ingestion.

The server has no authentication or network-trust modes. The UI and API share one origin and the server emits no cross-origin access policy. Documentation MUST state that V0 is a local or trusted-environment prototype and MUST NOT be exposed directly to an untrusted network.

## 2. Configuration

The server reads one optional TOML file and `ELUCID_*` environment overrides. Configuration is decoded once into typed domain values before dependency access. Unknown fields, invalid units, missing required secrets, zero or negative required limits, and inconsistent capacity relationships fail startup.

Configuration covers only:

- listen address, request timeout, and shutdown timeout;
- PostgreSQL URL secret and bounded pool size;
- object-store endpoint, bucket, root prefix, credentials, and request timeout;
- durable spool path and capacity, scratch path and capacity, and maximum HTTP batch bytes;
- ingestion and query concurrency, query timeout, scan-byte, memory, result-row, and result-byte limits;
- maintenance mode `AUTOMATIC` or `DISABLED` and event and dead-letter retention durations;
- log format `PRETTY` or `JSON`.

Spool capacity is reserved independently from replaceable Parquet staging, compaction staging, query spill, and caches. Cross-field validation ensures each configured operation can fit inside its corresponding local capacity.

Lower-level builder targets, row-group sizes, retry intervals, orphan grace, maintenance scan cadence, and upload, deletion, and compaction concurrency are bounded implementation constants in V0 rather than public configuration.

## 3. Startup, readiness, and shutdown

Startup performs these phases in order:

1. Decode and validate configuration.
2. Bind health endpoints.
3. Connect to PostgreSQL and run embedded SQLx migrations.
4. Load and validate the complete active catalog.
5. Initialize the object-store client and establish access to the configured bucket.
6. Open and recover the durable local spool.
7. Acquire the maintenance advisory lock when maintenance mode is `AUTOMATIC`.
8. Initialize HTTP, ingestion, query, and maintenance work.
9. Become ready.

`GET /health/live` reports whether the process runtime can make progress. `GET /health/ready` returns `200` only after startup and while the latest bounded health checks for PostgreSQL and the configured object store have succeeded, the active catalog is internally valid, the ingestion worker is operational, and the spool can durably accept at least one maximum-size request. Otherwise it returns `503 SERVER_NOT_READY` with bounded `Retry-After`. Its bounded body reports PostgreSQL, object-store, spool, ingestion-worker, query, and maintenance status as `UP`, `DEGRADED`, or `DOWN`.

A transient PostgreSQL or object-store outage prevents or removes readiness. While the readiness gate is closed, new ingestion and query admission return `503 SERVER_NOT_READY` with bounded `Retry-After`; ingestion rejects the request before reading its body or beginning a spool append. Operations that require an unavailable dependency fail with the owning typed error. Liveness, metrics, the embedded application, and `GET /api/v1/status` remain available for diagnosis.

The readiness decision is neither a per-request dependency probe nor a transaction with PostgreSQL or the object store. Once an ingestion request passes the admission gate, a later or not-yet-observed dependency failure does not turn it into a late `503`: if the body and local durable append succeed, Elucid returns `202` and keeps the batch in the spool until publication can resume. Publication of already accepted batches waits and retries with bounded backoff.

A failed critical supervised task terminates the process instead of leaving it falsely ready. A dependency outage is degraded external state, not a crashed supervised task.

Shutdown first removes readiness and stops new ingestion and query admission. It then finishes any in-progress spool append, cancels query work, stops claiming maintenance work, resolves any PostgreSQL publication transaction, persists local ingestion state, and exits within the configured deadline. Restart performs ordinary recovery.

## 4. HTTP conventions

Product endpoints use `/api/v1`. JSON responses use UTF-8. UUID-backed resource identities use lowercase UUID strings and timestamps use RFC 3339 UTC with millisecond precision.

Every response includes `X-Request-Id`; a valid caller-supplied UUID is preserved, otherwise the server creates one. This identity is for tracing only and never controls ingestion batch identity or deduplication. Errors use:

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "Request body is invalid",
    "details": {}
  }
}
```

Error messages and details are bounded and never contain credentials, stack traces, complete request bodies, event values, or object-store secrets.

## 5. Catalog, source, and status API

`POST /api/v1/catalog-applications` accepts one `application/yaml` catalog document and applies [Catalog](catalog.md#6-catalog-application). Success returns the source identity, active schema version, active input-profile revisions, and whether durable state changed.

`GET /api/v1/sources` returns a bounded list of source identity, name, display name, and active schema version. It does not compute event counts, time bounds, object bytes, or mutable source statistics.

`GET /api/v1/sources/{source_id}` adds the active schema, immutable schema-version summaries, inputs, and active profile summaries.

`GET /api/v1/segments?source_id={uuid}&state={state}` returns a bounded operational list of segment identity, state, origin, schema version, event day, row count, Parquet bytes, bounds, and publication or retirement time. It is intended for the V0 UI and showcase, not for unbounded inventory export.

`GET /api/v1/status` returns a bounded operational summary for the UI: component health, effective hard limits, spool usage and oldest queued age, publication backlog, maintenance ownership, and current or recent compaction summaries. It remains available from local and cached state during a dependency outage; unavailable subparts are explicitly marked `DOWN`. It does not scan event objects or materialize per-source statistics.

## 6. Ingestion and dead-letter API

`POST /api/v1/sources/{source_name}/inputs/{input_name}/events` implements [Ingestion](ingestion.md#2-admission-and-acknowledgement). It requires `application/x-ndjson` and returns `202` only after durable local spooling:

```json
{
  "batch_id": "019d...",
  "state": "DURABLY_QUEUED",
  "ingestion_time": "2026-08-19T12:00:00.000Z",
  "body_bytes": 12345
}
```

The endpoint has no `Idempotency-Key`, replay response, committed counters, or synchronous record-validation result.

For this endpoint, `202` means ownership accepted; `400`, `404`, `413`, and `415` are permanent request or configuration errors; `429` and `503` are retryable admission failures that occur before ownership. A connection failure or `500` response is ambiguous and MAY occur after the batch became durable, so retry can duplicate events.

`GET /api/v1/dead-letters?source_id={uuid}` returns a bounded list of published dead-letter object summaries. `GET /api/v1/dead-letters/{object_id}` returns entries up to the reported response limit and explicitly reports truncation. V0 provides no dead-letter pagination or bulk export API.

## 7. Query API

`POST /api/v1/query-executions` synchronously executes one query:

```json
{
  "query": "source demo_logs | filter status >= 400 | project @event_time, message, status | sort by -@event_time | take 100",
  "time_range": {
    "start_inclusive": "2026-08-18T00:00:00.000Z",
    "end_exclusive": "2026-08-20T00:00:00.000Z"
  },
  "output_rows": 1000
}
```

The success response contains query identity, resolved source and schema identities, effective range, selected segments and bytes, typed columns, rows, completion `COMPLETE` or `TRUNCATED`, truncation reason, output rows and bytes, and elapsed milliseconds. Syntax and semantic failures use the ordinary error envelope and carry the ordered query diagnostics in `error.details.diagnostics`.

Rows are arrays aligned with columns. `int64` and `uint64` use decimal strings, `eid` uses 32 lowercase hexadecimal characters, other finite scalar values use their natural JSON representation, `datetime` uses an RFC 3339 UTC string with millisecond precision, and `json` returns parsed JSON.

There is no PostgreSQL query-execution table, asynchronous queue, polling endpoint, or durable query state. Disconnect, timeout, or shutdown cancels the in-process query.

## 8. Embedded web application

The server embeds production UI assets and serves the single-page application from the same listener as the API. Unknown non-API browser routes fall back to the application entry point; API and health routes never do.

The V0 UI provides:

- source selection and active-schema inspection;
- a query editor with an explicit time range;
- typed result columns, rows, diagnostics, truncation, elapsed time, and scanned segments/bytes;
- bounded segment inspection showing ingestion and compaction changes;
- ingestion spool, publication, dead-letter, and compaction status from bounded API data and metrics.

The UI stores no credentials because V0 has no authentication. It does not require a separate web server or development runtime in the release image.

## 9. Limits and backpressure

The service bounds HTTP headers and bodies, spool bytes, record bytes, concurrent requests, open builders, staging bytes, uploads, selected query segments and bytes, query memory and spill, output rows and bytes, dead-letter responses, compaction inputs and outputs, maintenance batches, and object-store concurrency.

Capacity rejection occurs before accepting ownership of data. Ingestion and query admission return `429 CAPACITY_EXHAUSTED`; dependency or readiness rejection returns `503 SERVER_NOT_READY` with bounded `Retry-After`; a running query exceeding its own bound returns its named query error or a successful configured truncation.

## 10. Telemetry and errors

`GET /metrics` exposes Prometheus text format. Structured logs and metrics cover startup, HTTP, spool durability, ingestion processing, Parquet construction, object upload/publication/deletion, catalog application, query planning/execution, compaction, retention, recovery, and shutdown.

High-cardinality identities, object keys, query text, request bodies, event data, and error messages are excluded from metric labels. Logs may include request, batch, segment, object, query, and compaction identities where needed for bounded operational tracing, but never event payloads by default.

The stable HTTP error registry contains only errors named by the owning catalog, ingestion, storage, query, compaction, retention, and metastore contracts plus `INVALID_REQUEST`, `NOT_FOUND`, `CAPACITY_EXHAUSTED`, `SERVER_NOT_READY`, `SERVER_DRAINING`, and `INTERNAL_ERROR`.
