# Elucid v0 Retention Specification

- Status: `DRAFT`
- Depends on: [Storage](storage.md), [Metastore](metastore.md), [Ingestion](ingestion.md), [Compaction](compaction.md)

## 1. Terminology

| Term | Definition |
|---|---|
| Idempotency reservation | The expiring association from one input-scoped idempotency-key digest to one ingestion request and body identity. |
| Retry expiry | The durable instant after which a `RETRYABLE` ingestion request can no longer create another attempt. |
| Data expiry | The earliest durable instant at which an active segment may be removed from new query snapshots. |
| Dead-letter expiry | The earliest durable instant at which a published dead-letter object may be reclaimed. |
| Provenance expiry | The earliest durable instant at which terminal metadata may be pruned after every reference and descendant condition is satisfied. |
| Expiration | An atomic PostgreSQL transition that removes data or a request from future product work without deleting object bytes. |
| Reclamation | Exact-key deletion of expired or superseded object bytes after the query-snapshot grace period. |
| Provenance pruning | Bounded deletion of a closed terminal metadata component after its objects have been reclaimed. |

## 2. Durable deadlines

Configured retention durations MUST apply uniformly to every source and input. Every duration MUST be positive and converted with checked timestamp arithmetic using PostgreSQL time. Each derived deadline MUST be persisted once and MUST NOT be recomputed from current configuration.

A new ingestion request MUST set `retry_expires_at = created_at + idempotency_retention_seconds`. A terminal ingestion request MUST set `provenance_expires_at = completed_at + ingestion_provenance_retention_seconds`. Its idempotency reservation MUST set `expires_at = completed_at + idempotency_retention_seconds` in the same terminal transition.

An ingestion-origin segment MUST set `data_expires_at = ingestion_request.created_at + event_data_retention_seconds` and derive its [data-expiry bucket](storage.md#1-terminology). A compaction run MUST consume one data-expiry bucket. Each output segment MUST retain that bucket and set `data_expires_at` to the maximum input deadline. Compaction therefore MAY extend an input row's deadline by less than one day, MUST NOT extend it into another data-expiry bucket, and MUST NOT shorten it.

A published dead-letter object MUST set `retention_expires_at = ingestion_commit.committed_at + dead_letter_retention_seconds`. A terminal compaction run MUST set `provenance_expires_at = terminal_at + compaction_provenance_retention_seconds`.

## 3. Idempotency and retry expiration

The active idempotency namespace MUST be represented only by `ingestion_idempotency_keys`. `ingestion_requests` MAY retain body identity for provenance but MUST NOT retain a uniqueness constraint on an idempotency-key digest.

Claim MUST lock the matching reservation. An unexpired reservation MUST apply the [Ingestion claim outcomes](ingestion.md#4-claim-and-idempotency). An expired reservation whose request is terminal MUST be deleted and replaced atomically by `CLAIMED_NEW`; the new request MUST receive a new identity and treat the submitted body as new data.

A `RETRYABLE` request at or after `retry_expires_at` MUST transition atomically to `FAILED` with `INGESTION_RETRY_WINDOW_EXPIRED`, terminal time, provenance expiry, and idempotency-reservation expiry. Claim MAY perform this transition before returning `REPLAY_FAILED`. Periodic maintenance MUST claim at most `maximum_retry_expiration_batch_requests` such requests with `FOR UPDATE SKIP LOCKED` and perform the same transition. The transition MUST require no non-terminal attempt; a contradiction MUST produce `RETENTION_REFERENCE_INVARIANT_VIOLATION` without changing the request.

An idempotency reservation MAY be deleted only when its request is terminal and PostgreSQL time has reached `expires_at`. Deleting it MUST NOT delete or change the ingestion request. Periodic maintenance MUST claim at most `maximum_idempotency_expiration_batch_reservations` eligible reservations with their requests by using `FOR UPDATE SKIP LOCKED`, revalidate both rows, and delete only the reservations. A later submission of the same input and key is a new request regardless of body identity.

The sender owns the complete body until it receives a successful `COMMITTED` response. A timeout, disconnect, non-success response, or absent response MUST be treated as unacknowledged and retried with the same input, key, and body while the retry window remains open.

## 4. Data expiration

Every maintenance instance MAY claim expirable data. The claim transaction MUST select at most `maximum_expiration_batch_segments` `ACTIVE` segments whose `data_expires_at` is not later than PostgreSQL time by using `FOR UPDATE SKIP LOCKED`, require no `CLAIMED` compaction reservation, transition them to `EXPIRED`, and set one database-generated expiration time and `reclamation_not_before = expired_at + retired_object_grace_period_seconds`.

`EXPIRED` segments MUST be absent from every query snapshot established after the expiration transaction. A snapshot established before the transaction MAY continue reading their objects until its lifetime ends. Expiration MUST NOT modify rows, Parquet bytes, provenance, or another segment.

Compaction selection MUST reject an input whose `data_expires_at` is not later than PostgreSQL time plus `run_timeout_seconds`. Publication MUST revalidate that every input remains `ACTIVE`, reserved by the run, and unexpired.

## 5. Dead-letter expiration and object reclamation

A published dead-letter object becomes reclaimable when PostgreSQL time reaches `retention_expires_at`. Its ingestion-commit reference MUST remain as provenance and MUST NOT prevent the object from transitioning through `DELETE_PENDING` to `DELETED`.

A published Parquet object becomes reclaimable when its direct segment is `SUPERSEDED` or `EXPIRED`, PostgreSQL time has reached `reclamation_not_before`, and no `ACTIVE` segment references it. Reclamation MUST follow the exact-key deletion protocol in [Storage](storage.md#7-garbage-collection).

The dead-letter API MUST return `410 DEAD_LETTER_EXPIRED` when the referenced object is `DELETE_PENDING` or `DELETED`. Before `retention_expires_at`, a missing or corrupt published object remains a data-integrity failure.

## 6. Provenance pruning

Pruning MUST operate on at most `maximum_provenance_roots_per_batch` roots selected with `FOR UPDATE SKIP LOCKED`. It MUST delete one complete eligible root and its exclusively owned terminal descendants in one transaction. A root that no longer satisfies every condition after locking MUST be skipped without partial deletion.

A compaction run is eligible only when it is terminal, PostgreSQL time has reached `provenance_expires_at`, every output segment is terminal with a `DELETED` object, and no later compaction input references an output segment. Pruning MUST delete its output stored-object rows, output segment rows, input associations, and run row. It MUST preserve input segments, which become independently eligible through their producers after the associations are removed.

An ingestion request is eligible only when it is terminal, PostgreSQL time has reached `provenance_expires_at`, no idempotency reservation references it, every attempt is terminal, every attempt-produced stored object is `DELETED`, every ingestion-origin segment is terminal, and no compaction input references an ingestion-origin segment. Pruning MUST delete its stored-object rows, segment rows, ingestion commit, attempts, and request.

Pruning MUST proceed from leaf compaction runs toward ingestion roots. It MUST NOT delete an active segment, non-terminal producer, idempotency reservation, catalog definition, migration row, or metadata required by a retained provenance edge. After an ingestion-request root is pruned, its API identity is no longer retained and lookup MUST return `INGESTION_REQUEST_NOT_FOUND`.

## 7. Ownership and recovery

The `MAINTENANCE` role owns retry expiration, idempotency-reservation expiration, data expiration, object reclamation, and provenance pruning. Retry expiration, idempotency-reservation expiration, data expiration, and provenance pruning MUST each start at least once per `retention.scan_interval_seconds` while the role is ready; object reclamation follows `garbage_collection.scan_interval_seconds`. No process may overlap two invocations of the same local retention task. No task requires a leader or instance affinity. Concurrent workers MUST remain correct through row locks, `SKIP LOCKED`, durable states, and reference revalidation.

Each retry-expiration, idempotency-reservation-expiration, data-expiration, and provenance-pruning invocation MUST capture a monotonic start time and repeatedly execute its bounded batch transaction until one claim returns no eligible item or elapsed time reaches `retention.maximum_task_duration_seconds`. A task MUST NOT begin another batch after reaching the duration bound; an already started transaction remains bounded by the metastore statement timeout and MUST resolve normally. Batch limits bound one transaction and MUST NOT cap one invocation.

Expiration and pruning transactions MUST be short and MUST perform no object-store or local-file operation. Object deletion remains separately retryable through `DELETE_PENDING`. Startup and periodic recovery MUST resume every durable intermediate state without restoring expired query visibility or an expired idempotency reservation.

## 8. Observability and errors

Metrics MUST distinguish expired retry windows, deleted idempotency reservations, expired segments, expired rows, expired dead-letter objects, reclaimed Parquet objects, reclaimed dead-letter objects, pruned compaction roots, pruned ingestion roots, skipped referenced roots, retries, and failures. Each periodic task MUST expose its eligible-item backlog count and oldest eligible-item age. Labels MUST use bounded vocabularies.

Stable errors MUST include `INGESTION_RETRY_WINDOW_EXPIRED`, `DEAD_LETTER_EXPIRED`, `RETENTION_REFERENCE_INVARIANT_VIOLATION`, and `RETENTION_TIMESTAMP_OVERFLOW`.
