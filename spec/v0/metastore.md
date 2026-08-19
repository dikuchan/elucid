# Elucid v0 Metastore Specification

This document owns PostgreSQL authority, SQLx migrations, the seven Elucid product tables, required constraints, and visibility-changing transactions.

## 1. Authority and transaction rules

PostgreSQL is the source of truth for catalog state, visible segments, stored-object lifecycle, compaction ownership, and retention decisions. It does not store event rows, one row per HTTP batch, local spool offsets, query executions, source statistics, or Tantivy metadata.

All timestamps governing visibility and retention use PostgreSQL time. Identity columns use UUID. Counts and byte sizes use non-negative `BIGINT` with checked conversion at Rust boundaries.

Transactions that change query visibility are short and contain no filesystem, object-store, or DataFusion work. Query planning uses `REPEATABLE READ`; ordinary catalog and maintenance mutations use the weakest isolation that preserves their stated row-locking invariant.

## 2. SQLx migrations

Migration SQL files are embedded in the server binary with `sqlx::migrate!`. Startup calls `Migrator::run` before initializing runtime components. SQLx owns migration ordering, checksums, locking, and its `_sqlx_migrations` bookkeeping table.

The build MUST track the migrations directory so adding or removing a migration invalidates the embedded set. This build dependency is not a custom migration runner.

Applied migration files are immutable. A migration failure, missing applied migration, or checksum mismatch prevents readiness and terminates startup with `METASTORE_MIGRATION_FAILED`. Elucid defines no custom migration ledger, migration-lock protocol, checksum format, repair command, or migration-specific public API.

The application database role may run the embedded migrations in V0. A separately privileged production migration job is outside this contract.

## 3. Product tables

The metastore has exactly seven Elucid-owned product tables. `_sqlx_migrations` is SQLx-owned and is not a product table.

### 3.1 `sources`

`sources` contains `source_id UUID PRIMARY KEY`, unique `name TEXT`, `display_name TEXT`, `active_schema_id UUID`, `created_at TIMESTAMPTZ`, and `updated_at TIMESTAMPTZ`.

The active schema must belong to the same source. Source identity and name are immutable in V0.

### 3.2 `schema_versions`

`schema_versions` contains `schema_id UUID PRIMARY KEY`, `source_id UUID`, positive `version BIGINT`, `definition JSONB`, and `created_at TIMESTAMPTZ`.

`(source_id, version)` is unique. The validated definition contains the complete ordered materialized field list, stable field identities, logical types, nullability, descriptions, and historical remainder adapters. Rows are immutable.

Fields are not split into a separate relational table because V0 always loads and validates a complete immutable schema value. JSONB is decoded once into a strict Rust domain type before use.

### 3.3 `inputs`

`inputs` contains `input_id UUID PRIMARY KEY`, `source_id UUID`, `name TEXT`, `active_profile_revision_id UUID`, `created_at TIMESTAMPTZ`, and `updated_at TIMESTAMPTZ`.

`(source_id, name)` is unique. The active profile must belong to the input. Input identity, source, and name are immutable.

### 3.4 `ingestion_profile_revisions`

`ingestion_profile_revisions` contains `profile_revision_id UUID PRIMARY KEY`, `input_id UUID`, `source_id UUID`, positive `revision BIGINT`, `target_schema_id UUID`, `definition JSONB`, and `created_at TIMESTAMPTZ`.

`(input_id, revision)` is unique. Composite foreign keys or equivalent constraints prove input ownership and that the target schema belongs to the input's source. Rows are immutable.

### 3.5 `segments`

`segments` contains:

- `segment_id UUID PRIMARY KEY`;
- `source_id UUID` and `schema_id UUID`;
- `origin TEXT` with `INGESTION` or `COMPACTION`;
- nullable `produced_by_compaction_run_id UUID` and `claimed_by_compaction_run_id UUID`;
- UTC `event_day DATE`;
- minimum and maximum event and ingestion timestamps;
- positive `row_count BIGINT` and `uncompressed_bytes BIGINT`;
- nullable `data_expires_at TIMESTAMPTZ` while prepared, required after activation;
- `state TEXT` defined by the segment lifecycle;
- nullable `published_at`, `retired_at`, and `reclaim_after` timestamps;
- `created_at` and `updated_at` timestamps.

Source/schema ownership, origin-specific compaction references, state/timestamp combinations, ordered bounds, and non-negative sizes are constrained. An active input segment can be claimed by at most one compaction run because the claim lives on the segment row itself. A successfully superseded input retains that same claim to identify the consuming run.

An ingestion segment has no PostgreSQL ingestion-request parent. Its durable pre-publication ownership remains in the local spool. A compaction output references its producing run. A committed input records the run that superseded it.

### 3.6 `stored_objects`

`stored_objects` contains:

- `object_id UUID PRIMARY KEY`;
- `kind TEXT` with `PARQUET_DATA` or `DEAD_LETTER`;
- nullable unique `segment_id UUID` for Parquet ownership;
- nullable `input_id UUID` and nullable `batch_id UUID` for dead-letter ownership;
- unique `object_key TEXT`, expected byte size, BLAKE3 digest, media type, and positive format version;
- lifecycle `state TEXT`;
- nullable upload, publication, retention, delete-request, and deletion timestamps;
- nullable bounded `last_error_code TEXT`;
- `created_at` and `updated_at` timestamps.

`PARQUET_DATA` requires exactly one segment and no batch owner. `DEAD_LETTER` requires an input and batch identity and no segment. State and timestamps follow [Storage](storage.md#5-stored-object-lifecycle).

At most one dead-letter object may exist for one `(input_id, batch_id)` pair.

### 3.7 `compaction_runs`

`compaction_runs` contains `compaction_run_id UUID PRIMARY KEY`, `source_id UUID`, `schema_id UUID`, `event_day DATE`, state `BUILDING`, `UPLOADING`, `COMMITTED`, or `FAILED`, nullable bounded `failure_code TEXT`, and creation, update, and completion timestamps.

Inputs are the segment rows whose `claimed_by_compaction_run_id` equals the run. Outputs are rows whose `produced_by_compaction_run_id` equals it. A separate input-association table is unnecessary because a segment can be consumed by at most one compaction run.

## 4. Required access paths

The metastore provides indexes for:

- source and input lookup by name;
- schema and profile lookup by owner and version;
- active segments by source, event day, event-time bounds, and segment identity;
- unclaimed small active segments by source, schema, event day, size, and publication time;
- segments by producer and claim compaction-run references;
- stored objects by state, owner, retention deadline, and delete-request time;
- terminal segments by reclamation time;
- compaction runs by state and update time.

Indexes exist to serve these operations, not to satisfy a prescribed index count or name.

## 5. Visibility and lifecycle transactions

The implementation performs these state changes atomically; the contract does not prescribe a repository or service-object interface:

1. Apply one complete catalog manifest.
2. Register one prepared ingestion or compaction segment and its planned Parquet object.
3. Register one planned dead-letter object.
4. Mark one verified object uploaded.
5. Publish one ingestion segment or dead-letter object.
6. Claim bounded compaction inputs and create a run.
7. Register compaction outputs.
8. Publish one compaction replacement atomically.
9. Fail a compaction run and release its active input claims.
10. Expire bounded active segments.
11. Claim, finish, or retry exact-key object deletion.
12. Prune terminal metadata that has no remaining references.

Each transaction validates expected state in its mutation predicate and treats a zero-row update as a conflict to resolve from durable state. Rust callers distinguish applied, already-resolved, and conflicting outcomes with named variants rather than boolean flags.

## 6. SQLx usage

Fixed SQL is compile-time checked where SQLx supports it. Dynamic query construction is limited to closed internal choices such as sort direction or optional predicates; values are always bound.

Database rows are decoded directly into boundary structs, validated, and converted into domain types before entering catalog, ingestion, query, or maintenance logic. No component passes unvalidated JSONB or string states through its domain core.

## 7. Errors

Metastore exposes `METASTORE_UNAVAILABLE`, `METASTORE_MIGRATION_FAILED`, `METASTORE_CONFLICT`, and `METASTORE_CORRUPT`. Lower-level SQLx and PostgreSQL details remain in bounded internal diagnostics and logs.
