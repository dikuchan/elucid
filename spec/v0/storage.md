# Elucid v0 Storage Specification

This document owns immutable Parquet segments, S3-compatible objects, publication visibility, object lifecycle, local staging, and garbage collection.

## 1. Authority

The configured S3-compatible bucket is authoritative for published Parquet and dead-letter bytes. PostgreSQL is authoritative for which objects are visible to queries and eligible for deletion. The local spool is authoritative only for acknowledged data that has not yet reached a terminal published output.

Queries MUST use exact object descriptors selected from PostgreSQL. S3 prefix listing is never a query-planning or visibility mechanism.

## 2. Segment contract

A segment is a non-empty immutable group of event rows with one source, one stored schema, and one half-open UTC event-time day. Its origin is `INGESTION` or `COMPACTION`.

Segment state is `PREPARED`, `ACTIVE`, `SUPERSEDED`, `EXPIRED`, or `ABANDONED`:

- `PREPARED` has durable metadata but is not queryable.
- `ACTIVE` is visible to new query snapshots.
- `SUPERSEDED` was atomically replaced by compaction.
- `EXPIRED` was removed by retention.
- `ABANDONED` was never published.

Only `ACTIVE` segments are selected for new queries. A segment never returns to an earlier state. Identity, source, stored schema, day, row count, event-time bounds, ingestion-time bounds, and retention deadline are immutable after activation.

Rows are ordered by `@event_time` and then `@event_id`. Compaction preserves event identities and logical values but may change row groups and physical encodings.

## 3. Parquet representation

One segment has exactly one Parquet data object. It contains:

- `@event_time` and `@ingestion_time` as UTC millisecond timestamps;
- `@event_id` as non-null 16-byte binary;
- promoted user fields in stored-schema order and types;
- `@rest` as nullable UTF-8 JSON-object text.

Every Arrow field carries its stable `elucid.field_id` metadata. The Parquet footer records segment identity, source identity, stored-schema identity, row count, field identities, and format version. The reader validates these values against PostgreSQL before yielding rows.

The writer uses bounded row groups and compression. Exact codec levels, library versions, dictionary heuristics, and internal buffer choices are implementation choices, not product semantics.

Before upload, a local Parquet file is closed, reopened, validated, and hashed with BLAKE3 over its exact bytes.

## 4. Object keys

Managed keys use generated identities and never operator-controlled names:

```text
{root_prefix}/segments/{segment_id}/{object_id}.parquet
{root_prefix}/dead-letters/{batch_id}/{object_id}.ndjson
```

An upload never intentionally overwrites another object. On retry, an existing exact key is accepted only when its expected byte length and Elucid BLAKE3 metadata match the registered object; another value at that key is an integrity error.

V0 requires a bucket configuration in which deleting an exact key makes those managed bytes unavailable without requiring Elucid to manage object versions or delete markers.

## 5. Stored-object lifecycle

Stored-object kind is `PARQUET_DATA` or `DEAD_LETTER`. State is `PLANNED`, `UPLOADED`, `PUBLISHED`, `DELETE_PENDING`, or `DELETED`. Normal publication follows `PLANNED → UPLOADED → PUBLISHED`. Any of those three states may move forward to `DELETE_PENDING`, which moves to `DELETED` after successful deletion or confirmed absence.

`PLANNED` records the immutable object identity, kind, owner, exact key, expected length, digest, media type, and format version before object-store I/O. `UPLOADED` means a fresh exact-key metadata request verified the registered length and digest metadata. `PUBLISHED` means the object is part of committed visible or retained product state.

`DELETE_PENDING` records the PostgreSQL decision to remove external bytes before issuing or retrying the delete. `DELETED` records successful deletion or confirmed absence. A transient object-store failure leaves the object `DELETE_PENDING` for retry.

A published object is immutable. Missing or mismatched bytes for a `PUBLISHED` object are data-integrity failures, not reasons to hide metadata or return partial results.

## 6. Publication

Ingestion publication is one short PostgreSQL transaction that:

1. locks and validates one `PREPARED` segment and its `UPLOADED` Parquet object;
2. changes the object to `PUBLISHED`;
3. changes the segment to `ACTIVE` and records publication time and retention deadline;
4. commits.

Dead-letter publication validates one `UPLOADED` object, changes it to `PUBLISHED`, records publication time and its retention deadline, and commits. It has no segment row.

Compaction publishes all outputs and retires all inputs in one transaction defined by [Compaction](compaction.md#5-publication). Object-store work and local file work never occur while holding that transaction open.

A query snapshot sees either a segment before publication or after publication, never an active segment whose object is not `PUBLISHED`.

## 7. Local storage

The durable spool and its recovery metadata live on a configured persistent volume. Replaceable Parquet staging, query spill, and caches use separate bounded directories or separate capacity reservations so they cannot consume space required for acknowledged spool data.

Admission stops before the spool reaches its reserved limit. Maintenance stops before staging exhaustion. Query spill exhaustion fails the query and cannot delete or overwrite ingestion state.

## 8. Garbage collection

An object is reclaimable when PostgreSQL proves one of these conditions:

- its owner segment is `SUPERSEDED` or `EXPIRED` and `reclaim_after` has passed;
- it is a published dead-letter object whose retention deadline has passed;
- it is a `PLANNED` or `UPLOADED` Parquet object, its segment owner is `ABANDONED`, and its orphan grace period has passed.

The garbage collector revalidates eligibility, changes the object to `DELETE_PENDING`, commits, deletes the exact key, then records `DELETED`. It uses registered keys and does not discover candidates by listing S3.

Segment and object metadata may be removed after the object is `DELETED` and no active query snapshot or compaction run can reference it. Metadata cleanup is bounded and may lag byte deletion.

## 9. Errors and telemetry

Stable storage errors are `PARQUET_BUILD_FAILED`, `PARQUET_INVALID`, `OBJECT_STORE_UNAVAILABLE`, `OBJECT_UPLOAD_FAILED`, `OBJECT_VERIFICATION_FAILED`, `OBJECT_INTEGRITY_ERROR`, `OBJECT_DELETE_FAILED`, and `LOCAL_CAPACITY_EXHAUSTED`.

Metrics cover local bytes, object state counts, upload and delete attempts, object-store bytes and latency, active/prepared/retired segment counts, Parquet rows and bytes, and the age of the oldest reclaimable object.
