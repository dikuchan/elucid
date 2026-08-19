# Elucid v0 Compaction Specification

- Status: `DRAFT`
- Depends on: [Storage](storage.md), [Metastore](metastore.md), [Query Engine](query-engine.md), [Retention](retention.md)

## 1. Terminology

| Term | Definition |
|---|---|
| Compaction run | One fenced maintenance execution that rewrites a bounded set of active input segments into fewer output segments. |
| Input segment | An `ACTIVE` segment reserved by one compaction run. |
| Output segment | A `COMPACTION`-origin segment produced by one compaction run. |
| Input reservation | The durable association that prevents concurrent compaction runs from consuming the same segment. |
| Compaction publication | The PostgreSQL transaction that atomically activates every output segment and supersedes every input segment. |
| Reclamation time | The earliest PostgreSQL instant at which a superseded segment's Parquet object may be deleted. |

## 2. Core invariant

A committed compaction run MUST preserve the exact multiset of logical rows and replace all input segments with all output segments atomically. It MUST NOT deduplicate rows, change event identities, rewrite schemas, reinterpret `@rest`, or change any field value.

Every run MUST contain segments from exactly one source, one stored schema, one UTC event-time day, and one [data-expiry bucket](storage.md#1-terminology). It MUST consume at least `minimum_input_segments`, produce at least one output segment, and produce fewer output segments than it consumes.

Every compaction-provenance edge MUST point to a segment locked as `ACTIVE` by the claim transaction before any output exists for that run. The resulting graph MUST be acyclic, and every path MUST terminate at an ingestion-origin segment and committed ingestion commit. Committed runs, input associations, superseded segments, and deleted-object metadata MUST remain available for provenance traversal until eligible [provenance pruning](retention.md#6-provenance-pruning).

PostgreSQL state MUST determine run ownership, provenance, reservation, and visibility. Object-store listing, process memory, and local files MUST NOT determine any compaction decision.

## 3. Selection and claim

Every server instance with the `MAINTENANCE` service enabled MAY search for compaction candidates. The system MUST require no leader and MUST remain correct with any positive number of maintenance instances.

Multiple runs MAY execute concurrently for one source, stored schema, UTC event-time day, and data-expiry bucket. Their input reservations MUST be disjoint; bucket identity alone MUST NOT serialize them.

Before claiming work, an instance MUST acquire one compaction-concurrency permit and reserve `maximum_output_parquet_bytes` from its local working capacity. It MUST retain both until every local output file has been deleted.

Candidate inputs MUST be `ACTIVE`, unreserved, equal in source identity, schema identity, event-time bucket start, event-time bucket end, and data-expiry-bucket start, individually smaller than `target_output_segment_uncompressed_bytes`, and retained beyond PostgreSQL time plus `run_timeout_seconds`. Selection MUST enforce configured maximum input segments, rows, uncompressed bytes, and Parquet bytes with checked arithmetic.

The conservative predicted output count MUST be `ceil_div(total_uncompressed_byte_estimate, target_output_segment_uncompressed_bytes) + ceil_div(total_rows, maximum_output_segment_rows) - 1`. It MUST fit `maximum_output_segments` and be strictly less than the selected input count. Selection MUST prefer smaller uncompressed estimates, then older publication times, then segment identities. Producing fewer objects is the only selection benefit criterion; proximity to a target MUST NOT justify an equal-count rewrite.

The claim transaction MUST acquire the product compaction-capacity advisory lock, count non-terminal runs, and return no work at `maximum_cluster_concurrent_runs`. Below that bound, it MUST lock candidates with `FOR UPDATE SKIP LOCKED`, allocate one UUIDv7 compaction-run identity, create the `PREPARING` run with the common data-expiry-bucket start, create ordered `CLAIMED` input reservations, initialize its heartbeat from PostgreSQL time, and commit. A uniqueness conflict MUST restart candidate selection and MUST NOT escape as an internal error. The advisory lock MUST be transaction-scoped and MUST NOT be retained during construction, upload, or publication.

The immutable claimed input set MUST remain the run's provenance after success or abandonment. A run MUST NOT add, remove, or replace an input after claim.

## 4. Construction

The owner MUST read each input through its registered exact object descriptor and validate the object, footer, stored schema, field identities, row count, retention deadline, and required metadata before yielding rows. It MUST NOT hold a PostgreSQL connection during object-store or local-file work.

Input rows are already ordered under the [segment contract](storage.md#3-segment-contract). The builder MUST perform a bounded streaming merge ordered by `@event_time`, `@event_id`, input ordinal, and input row ordinal. Input ordinal and row ordinal are construction-only tie-breakers and MUST NOT become stored columns.

The builder MUST preserve every Arrow value exactly and start a new output before adding a row that would exceed `target_output_segment_uncompressed_bytes` or `maximum_output_segment_rows`, except that one individually oversized row MAY exceed the byte target while remaining within the Parquet-object maximum. Every output MUST remain within the claimed UTC event-time day, use the claimed stored schema and data-expiry bucket, and set data expiry to the maximum input deadline. The builder MUST enforce the compaction memory pool, local working capacity, output segments, total output Parquet bytes, Parquet-object bytes, and Parquet row groups.

Before Parquet finalization, the owner MUST allocate every output segment and stored-object identity and final key. A rebuilt run MUST allocate a new run identity and new output identities.

## 5. Output plan and upload

A constructed plan that does not reduce segment count or exceeds an output bound, including `maximum_output_parquet_object_bytes`, MUST abandon the run without persisting or uploading an output plan and MUST record the corresponding stable error.

In one output-plan transaction, the owner MUST lock and fence the `PREPARING` run, revalidate every claimed input, insert every `COMPACTION`-producer `PLANNED` stored object, insert every `COMPACTION`-origin `PREPARED` output segment, persist output counters, transition the run to `UPLOADING`, increment its update version, and commit. Failure MUST leave no partial output plan.

The owner MUST upload and verify every output according to the [stored-object contract](storage.md#5-stored-object-contract). It MAY enter `COMMITTING` only when every output object is `UPLOADED`, every output segment is `PREPARED`, output row count equals input row count, and output segment count is positive and less than input segment count.

## 6. Publication

Compaction publication MUST execute these operations in one PostgreSQL transaction:

1. Lock the run and every input reservation and input segment.
2. Return the committed result when the run is already `COMMITTED`.
3. Require run state `COMMITTING`, matching owner, counters, and update version.
4. Require every input reservation to be `CLAIMED` and every input segment to remain `ACTIVE` with the claimed immutable identity, source, schema, event-time bucket, data-expiry bucket, statistics, retention deadline later than PostgreSQL time, and data-object reference.
5. Require every planned output segment and object to match the durable plan and be `PREPARED` and `UPLOADED` under the run.
6. Require equal total input and output row counts and strictly fewer output segments than input segments.
7. Transition every output object to `PUBLISHED` and every output segment to `ACTIVE`.
8. Transition every input segment to `SUPERSEDED`, transition every reservation to `CONSUMED`, and set one database-generated supersession time and reclamation time for all inputs.
9. Transition the run to `COMMITTED`, set its terminal time and provenance expiry, and commit.

The transaction MUST use one database-generated publication instant for output publication, input supersession, and run completion. Every output MUST retain the run's data-expiry bucket, and every output retention deadline MUST equal the maximum input deadline.

The reclamation time MUST equal the supersession time plus the configured retired-object grace period. That grace period MUST exceed the maximum lifetime of every v0 query snapshot.

Only this transaction changes query visibility. A query snapshot established before publication MAY contain all input segments; one established after publication MUST contain all output segments and no input segment. No query snapshot may contain a partial side of the replacement.

After an ambiguous commit response, recovery MUST resolve by compaction-run identity. `COMMITTED` proves success; any other durable state permits fenced abandonment. Connection loss alone MUST NOT determine the outcome.

## 7. Ownership, failure, and recovery

Permitted run transitions are `PREPARING` to `UPLOADING`, `UPLOADING` to `COMMITTING`, `COMMITTING` to `COMMITTED`, and any non-terminal state to `ABANDONED`. `COMMITTED` and `ABANDONED` are terminal.

The run owner MUST renew `heartbeat_at` from PostgreSQL time at the configured interval until the run becomes terminal or ownership is lost. Renewal MUST modify only `heartbeat_at`, predicate on run identity, owner instance identity, and non-terminal state, and MUST NOT read, increment, or predicate on `update_version`. Every other post-claim mutation MUST predicate on run identity, expected state, and expected update version. A zero-row mutation or renewal MUST produce `COMPACTION_RUN_FENCED` and stop owner work.

Failure to confirm ownership before the stale threshold MUST self-fence the owner, cancel local work, and prohibit every new object-store mutation and retry. An already issued object-store request MUST remain bounded by the configured request timeout and cancellation.

The run timeout MUST begin at claim commit and include construction, upload, publication, and retry delays inside object-store requests. Expiry MUST cancel local work and abandon the run as `COMPACTION_TIMEOUT`; publication already in progress MUST first resolve its transaction outcome.

Failure before publication MUST persist one bounded stable error, transition the run to `ABANDONED`, set terminal time and provenance expiry, release every claimed input reservation, abandon every prepared output segment, preserve input segments as `ACTIVE`, and leave registered output objects eligible for garbage collection. An abandoned run's output MUST never become queryable or be adopted by another run.

Startup and periodic maintenance recovery MUST use PostgreSQL time, claim at most `maximum_recovery_batch_runs` stale non-terminal runs per transaction with `FOR UPDATE SKIP LOCKED`, fence their owners, and perform the same abandonment transition. Recovery MUST be safe on every maintenance instance. Startup recovery MUST delete compaction working files without active local owners.

Shutdown MUST stop new claims before cancelling construction and upload work. A publication transaction already in progress MUST resolve its durable outcome before the owner releases the run.

## 8. Failure outcomes

| Failure point | Durable resolution | Query visibility |
|---|---|---|
| Before claim commit | No run or reservation exists | All candidate inputs remain visible |
| After claim commit but before output-plan commit | A stale `PREPARING` run is abandoned and its reservations are released | All inputs remain visible |
| After output-plan commit but before all uploads verify | The stale run and prepared outputs are abandoned; registered objects are collected | All inputs remain visible |
| After every upload verifies but before `COMMITTING` | The stale `UPLOADING` run is abandoned | All inputs remain visible |
| After `COMMITTING` but before publication commit | The run remains uncommitted and is abandoned after resolving commit absence | All inputs remain visible |
| Publication commit outcome is ambiguous | Recovery resolves the run before choosing success or abandonment | All inputs or all outputs |
| After publication commit | The run and provenance are committed; input objects await reclamation | All outputs are visible to new snapshots |

Fault injection MUST exercise every row. No failure may hide an input segment before atomic publication, expose an output segment before atomic publication, or delete a published object before its reclamation time.

## 9. Observability and errors

Compaction logs MUST identify instance, run, source, schema, event-time bucket, data-expiry bucket, segment, and stored-object identities; state transition; input and output counts and bytes; duration milliseconds; and stable outcome. Event rows, field values, object keys, and credentials MUST NOT appear in default logs or metric labels.

Metrics MUST distinguish candidates rejected by the selection estimate from claimed runs that produce `COMPACTION_NOT_BENEFICIAL` after construction. The latter outcome is an estimator miss. Metrics MUST expose candidate-group segment-count distribution, oldest eligible-candidate age, eligible-segment creation rate, and committed input-segment consumption rate without source, schema, day, or persistent-identity labels. Documentation MUST require capacity planning such that sustained compaction consumption exceeds eligible-segment creation and MUST relate this requirement to ingestion batching.

Stable errors MUST include `COMPACTION_RUN_FENCED`, `COMPACTION_OWNER_LOST`, `COMPACTION_CANCELLED`, `COMPACTION_TIMEOUT`, `COMPACTION_INPUT_INVALID`, `COMPACTION_BUILD_FAILED`, `COMPACTION_NOT_BENEFICIAL`, `COMPACTION_OUTPUT_INVALID`, and `COMPACTION_PUBLICATION_AMBIGUOUS`.
