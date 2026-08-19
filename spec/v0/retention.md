# Elucid v0 Retention Specification

This document owns event-data expiration, dead-letter expiration, object reclamation grace, and terminal metadata cleanup.

## 1. Deadlines

Every ingestion segment receives `data_expires_at = published_at + event_data_retention` in its PostgreSQL publication transaction. Compaction outputs receive the maximum deadline of their inputs and never shorten retained data.

Every dead-letter object receives `retention_deadline = published_at + dead_letter_retention` in its publication transaction. All deadlines are PostgreSQL UTC instants calculated with checked arithmetic. Later configuration changes affect newly published data only.

## 2. Segment expiration

At each scan, maintenance selects a bounded number of unclaimed `ACTIVE` segments whose `data_expires_at` is not later than PostgreSQL time. One transaction changes them to `EXPIRED`, records retirement time, and sets `reclaim_after` later than the maximum query lifetime.

New query snapshots exclude expired segments immediately. A snapshot established before expiration may continue reading their objects until its bounded lifetime ends.

Compaction does not claim a segment whose retention deadline is too close to the expected run duration. Expiration does not race a compaction claim: the segment row lock and nullable compaction claim decide one operation first.

## 3. Object reclamation

The garbage collector follows the eligibility and state machine in [Storage](storage.md#8-garbage-collection). In particular, it never deletes a superseded or expired segment object before `reclaim_after` and never deletes an object referenced by an `ACTIVE` or `PREPARED` segment.

Dead-letter objects become reclaimable at their retention deadline. Abandoned and orphaned output objects become reclaimable after a fixed orphan grace period.

Deletion is retryable and idempotent through `DELETE_PENDING`. Object-store absence counts as successful deletion.

## 4. Metadata cleanup

Cleanup removes only bounded terminal roots whose external objects are `DELETED` and whose references are no longer required:

- an `ABANDONED`, `SUPERSEDED`, or `EXPIRED` segment after its data object row is deleted;
- a deleted dead-letter object row;
- a terminal compaction run after no segment references it as producer or claim.

V0 does not retain an independent ingestion-request history or an unbounded transitive provenance graph. Direct compaction relationships live only as long as their segments and runs are operationally relevant.

## 5. Ownership, bounds, and telemetry

The single maintenance owner runs expiration, reclamation, and metadata cleanup. Every scan has item, byte, concurrent-delete, and wall-time limits. Failure of one item does not prevent later eligible items from being attempted.

Metrics cover expired segments and rows, reclaimable and deleted objects, failed deletions, terminal metadata backlog, and oldest eligible age. Stable retention errors are `RETENTION_TIMESTAMP_OVERFLOW`, `RETENTION_STATE_CONFLICT`, and `RETENTION_CLEANUP_FAILED`; object deletion errors are owned by [Storage](storage.md#9-errors-and-telemetry).
