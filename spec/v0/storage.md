# Elucid v0 Storage Specification

- Status: `DRAFT`
- Depends on: [Catalog](catalog.md)

## 1. Terminology

| Term | Definition |
|---|---|
| Object-store authority | Stable identity for one S3-compatible namespace. Endpoint and credentials can change only while continuing to address that namespace. |
| Stored object | An immutable Elucid-produced object registered by identity, exact final key, kind, size, digest, producer, and lifecycle state. |
| Segment | A non-empty bounded group of accepted event rows with one source, stored schema, publication provenance, UTC event-time day, and statistics. |
| Parquet data object | The stored object containing the physical Parquet bytes of exactly one segment. |
| Dead-letter object | A stored object containing rejected-record entries for exactly one ingestion commit. |
| Published segment | An `ACTIVE` segment whose direct Parquet data object and provenance chain are committed and queryable. |
| Superseded segment | A formerly published segment atomically replaced by one committed compaction run. |
| Expired segment | A formerly published segment atomically removed from new query snapshots after reaching its data-retention deadline. |
| Data-expiry bucket | The half-open UTC calendar day beginning at `data_expiry_bucket_start` and containing one segment's `data_expires_at`. |
| Orphan candidate | A managed stored object that is not reachable from a complete published provenance chain. |

## 2. Authority and immutability

The configured S3-compatible object store MUST authoritatively store Parquet data objects and dead-letter objects. PostgreSQL MUST authoritatively determine object lifecycle and query visibility as specified by the [Metastore](metastore.md).

The object-store authority MUST provide immediate exact-key read visibility after a successful create to every Elucid instance configured for that authority. Elucid MUST NOT rely on listing consistency.

An object-store listing, tag, local file, process memory, upload response, or object existence MUST NOT establish publication. Query planning MUST use exact objects selected through committed metastore references.

Every Elucid output object MUST have a final unique key before upload begins and MUST be created without overwrite. Rename, copy, prefix replacement, and overwrite MUST NOT act as publication operations.

## 3. Segment contract

A segment MUST contain events from exactly one source, one stored schema, and one half-open UTC calendar day. Segment origin MUST be `INGESTION` or `COMPACTION`. An ingestion-origin segment MUST identify exactly one ingestion request and ingestion attempt, identify an ingestion commit after activation, and omit compaction-run identity. A compaction-origin segment MUST identify exactly one compaction run and omit ingestion-request, ingestion-attempt, and ingestion-commit identities.

Before serialization, rows within a segment MUST be ordered by `@event_time` ascending and `@event_id` ascending. An ingestion-origin segment MUST break remaining ties by original input position. A compaction-origin segment MUST break remaining ties by claimed input ordinal and input row ordinal. Every tie-breaker is construction-only and MUST NOT become a stored column. Physical order MUST be deterministic for the same producer plan and input bytes; query result order still requires an explicit language `sort` stage.

Each segment MUST record:

- `segment_id`;
- origin, source identity, schema identity, and the origin-specific producer identities;
- direct `data_object_id`;
- event-time bucket start and end;
- inclusive minimum and maximum event time;
- inclusive minimum and maximum ingestion time;
- row count;
- uncompressed byte estimate;
- Parquet byte size;
- binary lexical minimum and maximum event identity;
- data-expiry-bucket start, data-expiry time, state, publication time, nullable supersession time, nullable expiration time, nullable reclamation time, and lifecycle timestamps.

Segment state MUST be `PREPARED`, `ACTIVE`, `SUPERSEDED`, `EXPIRED`, or `ABANDONED`. Permitted transitions are `PREPARED` to `ACTIVE`, `PREPARED` to `ABANDONED`, `ACTIVE` to `SUPERSEDED`, and `ACTIVE` to `EXPIRED`. Only `ACTIVE` segments are queryable. An ingestion-origin `ACTIVE` segment MUST reference a committed ingestion commit and a `PUBLISHED` `PARQUET_DATA` object produced by its ingestion attempt. A compaction-origin `ACTIVE` segment MUST reference a committed compaction run and a `PUBLISHED` `PARQUET_DATA` object produced by that run.

Data expiry MUST be present in every state and later than maximum ingestion time. `data_expiry_bucket_start` MUST be the UTC midnight beginning the calendar day that contains `data_expires_at` and satisfy `data_expiry_bucket_start <= data_expires_at < data_expiry_bucket_start + one day` with checked arithmetic. Publication time MUST be present exactly for `ACTIVE`, `SUPERSEDED`, and `EXPIRED`. Supersession time MUST be present exactly for `SUPERSEDED`; expiration time MUST be present exactly for `EXPIRED`; reclamation time MUST be present exactly for `SUPERSEDED` and `EXPIRED` and later than the corresponding retirement time. `SUPERSEDED`, `EXPIRED`, and `ABANDONED` are terminal segment states. Segment identity, origin, producer identities, source, schema, object reference, event-time bucket, data-expiry bucket, statistics, data expiry, and publication time MUST remain immutable after activation.

Segment bounds MUST satisfy `minimum_event_time <= maximum_event_time` and remain inside the segment's UTC day. Metadata overlap with `[start_inclusive, end_exclusive)` is `minimum_event_time < end_exclusive AND maximum_event_time >= start_inclusive`.

Event time need not be monotonic within an ingestion request, input, source, attempt, commit, or compaction run. A valid late-arriving event MUST be written to a new immutable ingestion-origin segment for its event-time day. Ingestion MUST NOT reopen, append, overwrite, or supersede an existing segment. Compaction MAY supersede active segments only through the [compaction publication contract](compaction.md#6-publication). Publication order determines snapshot visibility; event time determines bucketing, query pruning, and row filtering; ingestion time determines the minimum data-retention promise.

## 4. Parquet format

A Parquet data object MUST use media type `application/vnd.apache.parquet` and format version `1`. Storage format version `1` MUST encode `@event_time` and `@ingestion_time` as Parquet `INT64` annotated with `TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS)`, `@event_id` as non-null `FixedSizeBinary(16)`, promoted fields according to the stored schema, and `@rest` as nullable UTF-8 canonical JSON.

The physical Arrow schema MUST equal the materialized schema identified by `schema_id`, including field order, type, nullability, timezone, logical metadata, and `elucid.field_id` values.

Parquet profile version `1` MUST use writer version 2.0, ZSTD level 3, dictionary encoding for eligible promoted fields, and the configured maximum row-group rows. The writer MUST emit row-group minimum, maximum, and null-count statistics for system time fields and promoted scalar fields and MUST disable value statistics for `@rest`.

Each footer MUST contain these UTF-8 key-value entries:

- `elucid.storage_format_version`;
- `elucid.parquet_profile_version`;
- `elucid.segment_origin`;
- `elucid.source_id`;
- `elucid.schema_id`;
- `elucid.schema_digest`;
- `elucid.field_id_map` as canonical JSON ordered by physical ordinal;
- `elucid.segment_id`;
- `elucid.event_time_bucket_start`;
- `elucid.event_time_bucket_end`.

An ingestion-origin footer MUST contain `elucid.ingestion_commit_id` and MUST omit `elucid.compaction_run_id`. A compaction-origin footer MUST contain `elucid.compaction_run_id` and MUST omit `elucid.ingestion_commit_id`. Each `elucid.field_id_map` entry MUST contain zero-based ordinal, exact physical column name, and canonical field UUID. Every embedded producer identity MUST equal the durable producer plan and final publication identity.

A local Parquet file MUST be closed and reopened before upload. Finalization MUST verify its footer, schema, row count, and required metadata, then compute a 32-byte BLAKE3 digest over the exact bytes. A rebuild MUST allocate new segment and object identities.

## 5. Stored-object contract

Stored-object kind MUST be `PARQUET_DATA` or `DEAD_LETTER`. Producer kind MUST be `INGESTION` or `COMPACTION`. State MUST be `PLANNED`, `UPLOADED`, `PUBLISHED`, `DELETE_PENDING`, or `DELETED`. Permitted transitions are `PLANNED` to `UPLOADED`, `UPLOADED` to `PUBLISHED`, `PLANNED`, `UPLOADED`, or an eligible retired or dead-letter-expired `PUBLISHED` object to `DELETE_PENDING`, and `DELETE_PENDING` to `DELETED`.

Managed output keys MUST have these forms:

```text
{root_prefix}/sources/{source_id}/segments/{segment_id}/{object_id}.parquet
{root_prefix}/sources/{source_id}/dead-letters/{ingestion_commit_id}/{object_id}.jsonl
```

Operator-controlled names MUST NOT determine output keys. Every normalized managed key MUST remain below the configured root prefix.

Before upload, the metastore MUST contain a `PLANNED` stored-object row with object identity, producer kind, exactly one producing ingestion attempt or compaction run, object kind, authority, alias, bucket, exact key, expected byte size, BLAKE3 digest, media type, and format version. These identity fields MUST remain immutable. A `DEAD_LETTER` object MUST have an ingestion producer.

The uploader MUST use a create-only conditional request. An existing key MUST produce `OBJECT_KEY_COLLISION`, regardless of observed bytes. A client and service combination that cannot prove create-only behavior MUST produce `OBJECT_STORE_CAPABILITY_MISSING` during readiness evaluation.

Multipart upload MUST complete before verification. A known failed multipart upload MUST be aborted, and the deployment MUST configure bounded expiration of incomplete multipart uploads.

Verification MUST use a new exact-key request after successful upload and compare key and content length. It MUST compare stored object identity, kind, and BLAKE3 metadata when the service accepted those fields and MUST compare a server-side checksum when the upload API returned one. A returned object version identity MUST be persisted as `remote_version_id`; every later read, verification, and deletion MUST address that exact version. A client that cannot address a returned version identity MUST produce `OBJECT_STORE_CAPABILITY_MISSING`. Successful verification MUST transition `PLANNED` to `UPLOADED`; failure MUST leave the object unpublished.

Only an ingestion or compaction publication transaction MAY transition a directly referenced `UPLOADED` object to `PUBLISHED`. A missing or corrupt `PUBLISHED` object is a data-integrity failure, never an implicit reason to remove its metadata reference.

## 6. Dead-letter object

A commit with rejected records MUST publish exactly one dead-letter object; a commit without rejected records MUST publish none. A dead-letter object MUST use media type `application/x-ndjson` and format version `1`, contain every entry in input-position order as UTF-8 NDJSON with LF terminators, and receive its retention expiry during publication.

Dead-letter bytes MUST be finalized, digested, registered, uploaded, verified, and published under the same object lifecycle as Parquet data. Failure of the dead-letter object MUST prevent publication of the complete ingestion request.

## 7. Garbage collection

Garbage collection MUST use registered exact keys and MUST NOT derive candidates or visibility from an object-store listing.

A `PLANNED` or `UPLOADED` object is an orphan candidate when all conditions hold:

- its producer is terminal;
- no `ACTIVE` segment references it as `data_object_id`;
- no committed ingestion commit references it as `dead_letter_object_id`;
- its creation time is older than the configured orphan grace period.

An orphan candidate from a committed producer MUST increment an invariant-anomaly counter before normal deletion. The anomaly MUST NOT make an unreferenced object immortal.

A `PUBLISHED` object is a retired candidate when it is `PARQUET_DATA`, its unique direct segment is `SUPERSEDED` or `EXPIRED`, PostgreSQL time has reached the segment's reclamation time, and no `ACTIVE` segment references it. A `PUBLISHED` `DEAD_LETTER` object is an expired dead-letter candidate when PostgreSQL time has reached its retention expiry.

The janitor MUST claim only an orphan, retired, or expired dead-letter candidate. It MUST lock and revalidate the object and every direct reference, transition it to `DELETE_PENDING`, and commit before issuing object-store operations. When `remote_version_id` is absent, it MUST first issue an exact-key metadata request and persist any returned version identity; absence is successful deletion. The delete MUST address `remote_version_id` when present and MUST NOT create a delete marker instead of deleting those bytes. Object-or-version-not-found is successful deletion. Successful deletion MUST transition the row to `DELETED`. A transient failure MUST preserve `DELETE_PENDING`, record a bounded error, and permit retry.

Before retrying `DELETE_PENDING`, the janitor MUST repeat the candidate-specific checks. An active segment reference, unexpired segment reclamation time, unexpired dead-letter retention time, or producer-state contradiction MUST produce `GC_REFERENCE_INVARIANT_VIOLATION` and stop deletion for that object. Metadata references MUST remain after object deletion until eligible provenance pruning.

Metrics MUST distinguish orphan, superseded, expired-segment, and expired-dead-letter candidates; claims; deleted objects; absent objects; retries; committed-producer anomalies; and reference violations.

## 8. Local storage

Staging, spill, and compaction working files MUST be reconstructible and MUST NOT determine committed visibility. Deleting them while Elucid is stopped MUST preserve every published event, terminal ingestion-request result, and committed compaction result.

Configured local directories and generated paths MUST be canonicalized. An absolute child, parent traversal, or symlink escape MUST be rejected. Files MUST use restrictive permissions and opaque identity-derived names.

## 9. Errors

Storage MUST define stable errors `PARQUET_BUILD_FAILED`, `PARQUET_VALIDATION_FAILED`, `OBJECT_KEY_COLLISION`, `OBJECT_STORE_CAPABILITY_MISSING`, `OBJECT_UPLOAD_FAILED`, `OBJECT_VERIFICATION_FAILED`, `PUBLISHED_OBJECT_MISSING`, `PUBLISHED_OBJECT_CORRUPT`, `DEAD_LETTER_BUILD_FAILED`, and `GC_REFERENCE_INVARIANT_VIOLATION`.
