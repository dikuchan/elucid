# Elucid v0 Metastore Specification

- Status: `DRAFT`
- Depends on: [Catalog](catalog.md), [Storage](storage.md)

## 1. Authority and representation

PostgreSQL MUST authoritatively store catalog state, active idempotency reservations, ingest-request identity and outcome, ingestion attempts and commits, compaction runs and provenance, retention deadlines, segment visibility, stored-object metadata, and migration history. Every transition that changes query visibility, completes an ingest request, publishes a compaction run, expires durable state, or prunes provenance MUST occur in PostgreSQL.

The minimum supported PostgreSQL server major version is `16`. Startup MUST read `server_version_num` before migrations and produce `METASTORE_VERSION_UNSUPPORTED` below `160000`. Continuous integration MUST execute metastore integration tests against major version `16`.

The metastore MUST NOT store a row, array element, or JSON member per input record, accepted event, event identity, or dead-letter entry. Data-plane metadata growth MUST be `O(active idempotency reservations + retained ingest requests + retained attempts + retained commits + retained compaction runs + retained compaction inputs + retained segments + retained stored objects)` and independent of accepted-event count within a request.

UUID identities MUST use PostgreSQL `UUID`. Persistent versions, positions, counts, and byte quantities MUST use `BIGINT` with positive or non-negative checks. Metadata instants MUST use `TIMESTAMPTZ(3)`. A database-generated instant that participates in a durable contract MUST use `date_trunc('milliseconds', clock_timestamp())`; assignment to `TIMESTAMPTZ(3)` alone is insufficient because PostgreSQL rounds excess precision.

A 32-byte digest MUST use `BYTEA` with `octet_length(value) = 32`; an event-identity bound MUST use `BYTEA` with `octet_length(value) = 16`. State and kind columns MUST use `TEXT` with closed `CHECK` constraints and MUST decode to Rust enums at the repository boundary.

Every mutable row MUST contain `created_at`, `updated_at`, and non-negative `update_version`. Each successful externally observable update MUST set `updated_at` and increment `update_version`. Catalog definitions and migration rows MUST reject update or deletion of identity and definition-bearing state. Immutable operational rows MAY be deleted only by the retention provenance-pruning transaction that owns their complete terminal component.

A heartbeat-only update to `ingest_attempts` or `compaction_runs` is an operational lease renewal, not an externally observable update. It MUST modify only `heartbeat_at` from PostgreSQL time and MUST NOT modify `updated_at` or `update_version`.

Cross-table ownership, kind, state, and cardinality invariants MUST be enforced by foreign keys, exclusion constraints, or deferred constraint triggers. Circular active-pointer constraints MUST be deferrable and valid at transaction commit.

## 2. Migrations

The executable MUST embed an ordered forward-only migration manifest. Each entry MUST contain positive version, unique name, UTF-8 SQL with LF line endings, and the 32-byte BLAKE3 digest of the exact script bytes. An accepted migration entry MUST remain byte-identical in every later compatible executable.

Elucid MUST own the migration ledger and runner through SQLx connection and transaction primitives. SQLx's built-in migration ledger MUST NOT become a second authority.

`elucid_migrations` MUST contain `version BIGINT PRIMARY KEY`, `name TEXT NOT NULL UNIQUE`, `script_checksum BYTEA NOT NULL`, and `applied_at TIMESTAMPTZ(3) NOT NULL`. Its rows MUST be an exact prefix of the embedded manifest by version, name, and checksum.

Migration startup MUST use a dedicated connection and acquire one product advisory lock before reading or creating the ledger. It MUST retain the lock through initial validation, application, final validation, and migration-phase completion. Lock acquisition MUST use a finite deadline.

An absent ledger is version zero only when no recognized Elucid relation exists. Under the lock, the runner MUST create the empty ledger in one transaction. Each pending migration MUST execute in its own transaction and insert its ledger row before commit.

After an ambiguous migration commit, the runner MUST reconnect, reacquire the lock, and inspect the ledger. A matching row proves success; an absent row permits retry; a different row is divergence. Migration failures MUST prevent runtime initialization.

Migration errors MUST be `METASTORE_MIGRATION_LOCK_TIMEOUT`, `METASTORE_MIGRATION_PERMISSION_DENIED`, `METASTORE_SCHEMA_TOO_NEW`, `METASTORE_MIGRATION_HISTORY_DIVERGED`, `METASTORE_MIGRATION_CHECKSUM_MISMATCH`, or `METASTORE_MIGRATION_FAILED`.

## 3. Tables

The metastore MUST contain `elucid_migrations` from Section 2 and exactly the product tables in this section. Implementations MAY choose constraint and index names.

### 3.1 `sources`

Columns MUST be `source_id UUID PRIMARY KEY`, `name TEXT NOT NULL UNIQUE`, `display_name TEXT NOT NULL`, `declaration JSONB NOT NULL`, `declaration_digest BYTEA NOT NULL`, `active_schema_id UUID NOT NULL`, `state TEXT NOT NULL`, `update_version BIGINT NOT NULL`, `created_at TIMESTAMPTZ(3) NOT NULL`, and `updated_at TIMESTAMPTZ(3) NOT NULL`.

State MUST be `ACTIVE`. Identity, name, declaration, and declaration digest are immutable. Catalog application MAY update display name and active schema. A deferred composite foreign key `(source_id, active_schema_id) -> schema_versions(source_id, schema_id)` MUST enforce ownership.

### 3.2 `schema_versions`

Columns MUST be `schema_id UUID PRIMARY KEY`, `source_id UUID NOT NULL`, `schema_version BIGINT NOT NULL`, `format_version INTEGER NOT NULL`, `declaration JSONB NOT NULL`, `declaration_digest BYTEA NOT NULL`, `materialized_definition JSONB NOT NULL`, `materialized_digest BYTEA NOT NULL`, `arrow_schema_descriptor JSONB NOT NULL`, and `created_at TIMESTAMPTZ(3) NOT NULL`.

Version and format version MUST be positive. Required unique keys are `(source_id, schema_id)`, `(source_id, schema_version)`, `(source_id, declaration_digest)`, and `(source_id, materialized_digest)`. `source_id` MUST reference `sources`. Rows are immutable.

### 3.3 `schema_fields`

Columns MUST be `schema_id UUID NOT NULL`, `ordinal INTEGER NOT NULL`, `field_id UUID NOT NULL`, `name TEXT NOT NULL`, `logical_type TEXT NOT NULL`, `arrow_type_descriptor JSONB NOT NULL`, `nullability TEXT NOT NULL`, `role TEXT NOT NULL`, nullable `description TEXT`, and `logical_metadata JSONB NOT NULL`.

The primary key MUST be `(schema_id, ordinal)`. Required unique keys are `(schema_id, field_id)` and `(schema_id, name)`. Ordinal MUST be non-negative; `schema_id` MUST reference `schema_versions`. Rows are immutable. `field_id` is intentionally reusable across schema versions.

### 3.4 `inputs`

Columns MUST be `input_id UUID PRIMARY KEY`, `source_id UUID NOT NULL`, `name TEXT NOT NULL`, `input_kind TEXT NOT NULL`, `declaration JSONB NOT NULL`, `declaration_digest BYTEA NOT NULL`, `materialized_configuration JSONB NOT NULL`, `materialized_digest BYTEA NOT NULL`, `active_ingest_profile_revision_id UUID NOT NULL`, `state TEXT NOT NULL`, `update_version BIGINT NOT NULL`, `created_at TIMESTAMPTZ(3) NOT NULL`, and `updated_at TIMESTAMPTZ(3) NOT NULL`.

Input kind MUST be `HTTP_NDJSON`; state MUST be `ACTIVE`. Required unique keys are `(input_id, source_id)`, `(source_id, name)`, and `(source_id, declaration_digest)`. `source_id` MUST reference `sources`. Identity, ownership, name, kind, declaration, configuration, and digests are immutable. A deferred composite foreign key `(input_id, active_ingest_profile_revision_id) -> ingest_profile_revisions(input_id, ingest_profile_revision_id)` MUST enforce ownership.

### 3.5 `ingest_profile_revisions`

Columns MUST be `ingest_profile_revision_id UUID PRIMARY KEY`, `input_id UUID NOT NULL`, `source_id UUID NOT NULL`, `revision BIGINT NOT NULL`, `target_schema_id UUID NOT NULL`, `parser_kind TEXT NOT NULL`, `declaration JSONB NOT NULL`, `declaration_digest BYTEA NOT NULL`, `materialized_definition JSONB NOT NULL`, `materialized_digest BYTEA NOT NULL`, and `created_at TIMESTAMPTZ(3) NOT NULL`.

Revision MUST be positive; parser kind MUST be `NDJSON`. Required unique keys are `(input_id, ingest_profile_revision_id)`, `(input_id, ingest_profile_revision_id, target_schema_id)`, `(input_id, revision)`, `(input_id, declaration_digest)`, and `(input_id, materialized_digest)`. Composite foreign keys to `inputs(input_id, source_id)` and `schema_versions(source_id, schema_id)` MUST enforce ownership. Rows are immutable.

### 3.6 `ingest_idempotency_keys`

Columns MUST be `input_id UUID NOT NULL`, `idempotency_key_digest BYTEA NOT NULL`, unique `ingest_request_id UUID NOT NULL`, `body_blake3_digest BYTEA NOT NULL`, `body_byte_count BIGINT NOT NULL`, nullable `expires_at TIMESTAMPTZ(3)`, `update_version BIGINT NOT NULL`, `created_at TIMESTAMPTZ(3) NOT NULL`, and `updated_at TIMESTAMPTZ(3) NOT NULL`.

The primary key MUST be `(input_id, idempotency_key_digest)`. Both digests MUST contain 32 bytes, and body byte count MUST be non-negative. Input, key digest, request identity, body identity, and creation time are immutable. Expiry MUST be null while the referenced request is non-terminal and set exactly once by the request's terminal transaction. A deferred composite foreign key to `ingest_requests(ingest_request_id, input_id)` MUST enforce request ownership.

### 3.7 `ingest_requests`

Columns MUST be `ingest_request_id UUID PRIMARY KEY`, `input_id UUID NOT NULL`, `source_id UUID NOT NULL`, `ingest_profile_revision_id UUID NOT NULL`, `schema_id UUID NOT NULL`, `body_blake3_digest BYTEA NOT NULL`, `body_byte_count BIGINT NOT NULL`, `state TEXT NOT NULL`, `committed_accepted_record_count BIGINT NOT NULL`, `committed_rejected_record_count BIGINT NOT NULL`, `committed_ignored_blank_record_count BIGINT NOT NULL`, `committed_segment_count BIGINT NOT NULL`, `committed_parquet_object_count BIGINT NOT NULL`, `committed_dead_letter_object_count BIGINT NOT NULL`, nullable `minimum_event_time TIMESTAMPTZ(3)`, nullable `maximum_event_time TIMESTAMPTZ(3)`, nullable `failure_code TEXT`, nullable `failure_message TEXT`, nullable `failure_details JSONB`, `retry_expires_at TIMESTAMPTZ(3) NOT NULL`, nullable `completed_at TIMESTAMPTZ(3)`, nullable `provenance_expires_at TIMESTAMPTZ(3)`, `update_version BIGINT NOT NULL`, `created_at TIMESTAMPTZ(3) NOT NULL`, and `updated_at TIMESTAMPTZ(3) NOT NULL`.

State MUST be `PROCESSING`, `RETRYABLE`, `COMMITTED`, or `FAILED`. Required unique keys are `(ingest_request_id, input_id)` and `(ingest_request_id, source_id, schema_id)`. Body byte count and counters MUST be non-negative, body digest MUST contain 32 bytes, and retry expiry MUST be later than creation time.

Constraints MUST enforce `committed_segment_count = committed_parquet_object_count`, committed dead-letter object count of zero or one, every committed counter equal to zero outside `COMMITTED`, a dead-letter count of one exactly when `committed_rejected_record_count` is positive, null event-time bounds exactly when `committed_accepted_record_count` is zero, and ordered non-null bounds otherwise. Failure fields MUST be present exactly for `FAILED`; completion and provenance expiry MUST be present exactly for `COMMITTED` and `FAILED`; provenance expiry MUST be later than completion.

Composite foreign keys MUST prove input ownership, schema ownership, profile ownership, and equality between pinned schema and profile target. Identity, body digest, body byte count, pinned identities, retry expiry, and `created_at` are immutable. The database-generated `created_at` is event `@ingest_time`.

### 3.8 `ingest_attempts`

Columns MUST be `ingest_attempt_id UUID PRIMARY KEY`, `ingest_request_id UUID NOT NULL`, `instance_id UUID NOT NULL`, nullable `planned_ingest_commit_id UUID`, `state TEXT NOT NULL`, `heartbeat_at TIMESTAMPTZ(3) NOT NULL`, `deadline_at TIMESTAMPTZ(3) NOT NULL`, `accepted_record_count BIGINT NOT NULL`, `rejected_record_count BIGINT NOT NULL`, `ignored_blank_record_count BIGINT NOT NULL`, nullable `error_code TEXT`, nullable `error_message TEXT`, nullable `error_details JSONB`, nullable `terminal_at TIMESTAMPTZ(3)`, `update_version BIGINT NOT NULL`, `created_at TIMESTAMPTZ(3) NOT NULL`, and `updated_at TIMESTAMPTZ(3) NOT NULL`.

State MUST be `PREPARING`, `UPLOADING`, `COMMITTING`, `COMMITTED`, `ABANDONED`, or `FAILED`. Counts MUST be non-negative. Deadline MUST be later than creation time, no later than the request's retry expiry, and immutable. Planned ingest-commit identity MUST be assigned exactly once before `UPLOADING` and be present in `UPLOADING`, `COMMITTING`, and `COMMITTED`. Error code and message MUST be present exactly for `ABANDONED` and `FAILED`; error details MAY be present only in those states.

Required unique keys are `(ingest_attempt_id, ingest_request_id)`, `(ingest_attempt_id, planned_ingest_commit_id)`, and non-null `planned_ingest_commit_id`. A partial unique constraint MUST permit at most one non-terminal attempt per ingest request. A deferred constraint MUST require exactly one non-terminal attempt for a `PROCESSING` ingest request and none for every other ingest-request state. `terminal_at` MUST be present exactly for terminal states. `ingest_request_id` MUST reference `ingest_requests`.

### 3.9 `ingest_commits`

Columns MUST be `ingest_commit_id UUID PRIMARY KEY`, `ingest_attempt_id UUID NOT NULL UNIQUE`, `ingest_request_id UUID NOT NULL UNIQUE`, `source_id UUID NOT NULL`, `schema_id UUID NOT NULL`, `accepted_record_count BIGINT NOT NULL`, `rejected_record_count BIGINT NOT NULL`, `ignored_blank_record_count BIGINT NOT NULL`, `input_byte_count BIGINT NOT NULL`, nullable unique `dead_letter_object_id UUID`, and `committed_at TIMESTAMPTZ(3) NOT NULL`.

Rows are immutable. Counts and input byte count MUST be non-negative. A dead-letter object MUST be present exactly when rejected count is positive. Deferred constraints MUST prove equality with the final attempt plan and ingest-request body, commit identity preallocation, ownership across attempt, request, source, and schema, counter equality with the committed request, and correct dead-letter object kind and producer.

### 3.10 `compaction_runs`

Columns MUST be `compaction_run_id UUID PRIMARY KEY`, `instance_id UUID NOT NULL`, `source_id UUID NOT NULL`, `schema_id UUID NOT NULL`, `event_time_bucket_start TIMESTAMPTZ(3) NOT NULL`, `event_time_bucket_end TIMESTAMPTZ(3) NOT NULL`, `data_expiry_bucket_start TIMESTAMPTZ(3) NOT NULL`, `state TEXT NOT NULL`, `heartbeat_at TIMESTAMPTZ(3) NOT NULL`, `input_segment_count BIGINT NOT NULL`, `input_row_count BIGINT NOT NULL`, `input_uncompressed_byte_estimate BIGINT NOT NULL`, `input_parquet_byte_count BIGINT NOT NULL`, nullable `output_segment_count BIGINT`, nullable `output_row_count BIGINT`, nullable `output_parquet_byte_count BIGINT`, nullable `error_code TEXT`, nullable `error_message TEXT`, nullable `error_details JSONB`, nullable `terminal_at TIMESTAMPTZ(3)`, nullable `provenance_expires_at TIMESTAMPTZ(3)`, `update_version BIGINT NOT NULL`, `created_at TIMESTAMPTZ(3) NOT NULL`, and `updated_at TIMESTAMPTZ(3) NOT NULL`.

State and transitions MUST match the [compaction ownership contract](compaction.md#7-ownership-failure-and-recovery). Input segment count MUST be at least two; input row count, uncompressed byte estimate, and Parquet byte count MUST be positive. Output counters MUST be all null or all non-null, MUST be null in `PREPARING`, and MUST be positive in `UPLOADING`, `COMMITTING`, and `COMMITTED`; an `ABANDONED` run MAY retain either form according to whether its output plan committed. When present, output row count MUST equal input row count and output segment count MUST be less than input segment count. Terminal time and provenance expiry MUST be present exactly for `COMMITTED` and `ABANDONED`, and provenance expiry MUST be later than terminal time. Error code and message MUST be present exactly for `ABANDONED`; error details MAY be present only for `ABANDONED`.

The event-time bucket and data-expiry bucket MUST each be one half-open UTC calendar day. The unique key `(compaction_run_id, source_id, schema_id, event_time_bucket_start, event_time_bucket_end, data_expiry_bucket_start)` and composite foreign keys MUST prove source-schema ownership. Multiple non-terminal runs MAY own disjoint reserved inputs in the same source, schema, event-time bucket, and data-expiry bucket. Deferred constraints MUST require input counters to equal the exact sums over input segments and, when output counters are present, require them to equal the exact count, row sum, and Parquet byte sum over output segments owned by the run. Run identity, owner, source, schema, both buckets, input counters, and creation time are immutable. A terminal run MAY be deleted only by retention provenance pruning.

### 3.11 `compaction_run_inputs`

Columns MUST be `compaction_run_id UUID NOT NULL`, `input_ordinal INTEGER NOT NULL`, `input_segment_id UUID NOT NULL`, `reservation_state TEXT NOT NULL`, `update_version BIGINT NOT NULL`, `created_at TIMESTAMPTZ(3) NOT NULL`, and `updated_at TIMESTAMPTZ(3) NOT NULL`.

The primary key MUST be `(compaction_run_id, input_ordinal)`, and `(compaction_run_id, input_segment_id)` MUST be unique. Input ordinal MUST be non-negative. Reservation state MUST be `CLAIMED`, `CONSUMED`, or `RELEASED`; permitted transitions are `CLAIMED` to `CONSUMED` and `CLAIMED` to `RELEASED`. A partial unique constraint on `input_segment_id` where reservation state is `CLAIMED` MUST prevent concurrent consumption.

Compaction-run and input-segment identities and input ordinal are immutable. Both identities MUST reference their respective tables. Deferred constraints MUST require every run to have exactly `input_segment_count` inputs, at least two inputs, and inputs equal to the run in source, schema, event-time bucket, and data-expiry bucket. Every input MUST have `published_at <= run.created_at` and MUST NOT be produced by the same run. A `COMMITTED` run MUST have only `CONSUMED` inputs whose segments are `SUPERSEDED`; an `ABANDONED` run MUST have only `RELEASED` inputs whose segments were not superseded by that run. Terminal associations MAY be deleted only with their run by retention provenance pruning.

### 3.12 `segments`

Columns MUST be `segment_id UUID PRIMARY KEY`, `origin TEXT NOT NULL`, nullable `ingest_request_id UUID`, nullable `ingest_attempt_id UUID`, nullable `ingest_commit_id UUID`, nullable `compaction_run_id UUID`, `source_id UUID NOT NULL`, `schema_id UUID NOT NULL`, unique `data_object_id UUID NOT NULL`, `event_time_bucket_start TIMESTAMPTZ(3) NOT NULL`, `event_time_bucket_end TIMESTAMPTZ(3) NOT NULL`, `minimum_event_time TIMESTAMPTZ(3) NOT NULL`, `maximum_event_time TIMESTAMPTZ(3) NOT NULL`, `minimum_ingest_time TIMESTAMPTZ(3) NOT NULL`, `maximum_ingest_time TIMESTAMPTZ(3) NOT NULL`, `row_count BIGINT NOT NULL`, `uncompressed_byte_estimate BIGINT NOT NULL`, `parquet_byte_size BIGINT NOT NULL`, `minimum_event_id BYTEA NOT NULL`, `maximum_event_id BYTEA NOT NULL`, `data_expiry_bucket_start TIMESTAMPTZ(3) NOT NULL`, `data_expires_at TIMESTAMPTZ(3) NOT NULL`, `state TEXT NOT NULL`, `prepared_at TIMESTAMPTZ(3) NOT NULL`, nullable `published_at TIMESTAMPTZ(3)`, nullable `superseded_at TIMESTAMPTZ(3)`, nullable `expired_at TIMESTAMPTZ(3)`, nullable `reclamation_not_before TIMESTAMPTZ(3)`, `update_version BIGINT NOT NULL`, `created_at TIMESTAMPTZ(3) NOT NULL`, and `updated_at TIMESTAMPTZ(3) NOT NULL`.

Origin, state, timestamps, retention deadlines, and field invariants MUST match the [segment contract](storage.md#3-segment-contract). An `INGESTION` segment MUST contain request and attempt identities, contain a commit identity exactly in `ACTIVE`, `SUPERSEDED`, or `EXPIRED`, and omit compaction-run identity. A `COMPACTION` segment MUST contain a compaction-run identity and omit ingestion identities. Composite foreign keys and deferred constraints MUST prove origin-specific producer ownership, source-schema ownership, attempt-commit equality, data-object kind, data-object producer, Parquet byte size, data-expiry-bucket derivation, and committed compaction provenance including bucket equality. An `ACTIVE` row MUST NOT be deleted; a terminal row MAY be deleted only by retention provenance pruning.

### 3.13 `stored_objects`

Columns MUST be `object_id UUID PRIMARY KEY`, `producer_kind TEXT NOT NULL`, nullable `ingest_attempt_id UUID`, nullable `compaction_run_id UUID`, `kind TEXT NOT NULL`, `object_store_authority TEXT NOT NULL`, `object_store_alias TEXT NOT NULL`, `bucket TEXT NOT NULL`, `object_key TEXT NOT NULL`, `expected_byte_size BIGINT NOT NULL`, `blake3_digest BYTEA NOT NULL`, `media_type TEXT NOT NULL`, `format_version INTEGER NOT NULL`, `state TEXT NOT NULL`, nullable `remote_version_id TEXT`, nullable `remote_etag TEXT`, nullable `remote_checksum TEXT`, nullable `retention_expires_at TIMESTAMPTZ(3)`, nullable `uploaded_at TIMESTAMPTZ(3)`, nullable `published_at TIMESTAMPTZ(3)`, nullable `delete_requested_at TIMESTAMPTZ(3)`, nullable `deleted_at TIMESTAMPTZ(3)`, nullable `last_error_code TEXT`, nullable `last_error_details JSONB`, `update_version BIGINT NOT NULL`, `created_at TIMESTAMPTZ(3) NOT NULL`, and `updated_at TIMESTAMPTZ(3) NOT NULL`.

Producer kind, object kind, state, mutability, and timestamps MUST match the [stored-object contract](storage.md#5-stored-object-contract). Expected byte size and format version MUST be positive. An `INGESTION` producer MUST contain only `ingest_attempt_id`; a `COMPACTION` producer MUST contain only `compaction_run_id`; and `DEAD_LETTER` MUST require `INGESTION`. Producer identities MUST reference their respective tables. Required unique keys are `(object_store_authority, bucket, object_key)`, `(object_id, ingest_attempt_id)`, and `(object_id, compaction_run_id)`. A partial unique constraint MUST permit at most one `DEAD_LETTER` object per ingest attempt. Retention expiry MUST be present only for a published or formerly published `DEAD_LETTER`. A `DELETED` row MAY be deleted only by retention provenance pruning.

## 4. Required access paths

The metastore MUST provide:

- `(input_id, idempotency_key_digest)` for idempotency-reservation claim and replay;
- a partial `(expires_at, input_id, idempotency_key_digest)` index where idempotency-reservation expiry is non-null;
- `(input_id, state, created_at, ingest_request_id)` for ingest-request lists and grouped summaries;
- partial `(retry_expires_at, ingest_request_id)` access where request state is `RETRYABLE`;
- partial stale-attempt and attempt-deadline indexes over non-terminal state, heartbeat, deadline, and attempt identity;
- a partial stale-compaction-run index over non-terminal state and heartbeat;
- a partial compaction-candidate index over source, schema, event-time bucket, data-expiry-bucket start, uncompressed byte estimate, publication time, and segment identity, including data expiry, where segment state is `ACTIVE`;
- `(ingest_attempt_id, state)` indexes for segments and stored objects;
- `(compaction_run_id, state)` indexes for segments and stored objects;
- a partial source-summary index over `(source_id, segment_id) INCLUDE (row_count, minimum_event_time, maximum_event_time, parquet_byte_size) WHERE state = 'ACTIVE'`;
- a partial GiST index over `(source_id, tstzrange(minimum_event_time, maximum_event_time, '[]')) WHERE state = 'ACTIVE'`;
- a partial index over `(data_expires_at, segment_id) WHERE state = 'ACTIVE'`;
- a partial index over `(reclamation_not_before, segment_id) WHERE state IN ('SUPERSEDED', 'EXPIRED')`;
- a partial index over `(retention_expires_at, object_id) WHERE kind = 'DEAD_LETTER' AND state = 'PUBLISHED'`;
- partial provenance-pruning indexes over terminal ingest-request and compaction-run expiry;
- a partial garbage-collection index over object state, creation time, retention expiry, and object identity.

Migrations MUST install `btree_gist` and use its UUID equality operator class for the active-segment index. Insufficient privilege MUST produce `METASTORE_MIGRATION_PERMISSION_DENIED`.

## 5. Transaction operations

The metastore boundary MUST expose typed atomic operations for catalog application, ingest-request claim and replay, retry-window expiration, idempotency-reservation expiration, ingestion output-plan persistence, compaction claim, compaction output-plan persistence, upload-state transition, ingestion publication, compaction publication, ambiguous-publication lookup, owner failure, stale-owner recovery, query-snapshot selection, segment expiration, garbage-collection claim, and provenance pruning.

Each operation MUST accept one complete immutable command and return a named outcome. State distinctions MUST use enums rather than booleans. No operation MAY hold a database connection while performing HTTP body I/O, S3, local-file, or DataFusion work.

Every mutation after ingestion claim except heartbeat renewal MUST predicate on attempt identity, expected state, and expected `update_version`. Every mutation after compaction claim except heartbeat renewal MUST predicate on run identity, expected state, and expected `update_version`. A heartbeat renewal MUST predicate on producer identity, owning instance identity, and non-terminal state. A zero-row mutation or renewal is `ATTEMPT_FENCED` for ingestion and `COMPACTION_RUN_FENCED` for compaction.

## 6. SQLx repository

Only the metastore adapter MAY issue product SQL or expose SQLx row types. It MUST use SQLx directly and MUST NOT use an ORM, generic CRUD repository, service locator, or repository API that merely mirrors tables.

Fixed-shape statements MUST use SQLx compile-time-checked query macros with committed offline metadata. Dynamic statements MUST bind every value, construct identifiers only from a closed internal vocabulary, and enforce explicit predicate and parameter bounds.

Rows MUST be validated into domain newtypes, enums, bounded collections, and unit-bearing values before leaving the adapter. Driver errors, SQL text, database nullability, and raw JSONB MUST NOT escape the boundary unchecked.

The connection pool MUST enforce maximum connections and finite acquisition, connection, statement, and lock deadlines. Every transaction MUST end through explicit commit or rollback.

## 7. Errors

Metastore errors MUST include `METASTORE_VERSION_UNSUPPORTED`, `METASTORE_SCHEMA_NOT_READY`, migration errors from Section 2, `METASTORE_TRANSACTION_FAILED`, `INGEST_REQUEST_STATE_CONFLICT`, `ATTEMPT_FENCED`, `COMPACTION_RUN_FENCED`, `PUBLICATION_AMBIGUOUS`, `COMPACTION_PUBLICATION_AMBIGUOUS`, `RETENTION_REFERENCE_INVARIANT_VIOLATION`, and `RETENTION_TIMESTAMP_OVERFLOW`. Persisted errors MUST contain bounded stable code, bounded sanitized message, versioned bounded details, and no driver display string.
