# Elucid v0 Compaction Specification

This document owns automatic selection, construction, publication, failure recovery, and observability of Parquet compaction.

## 1. Contract

Compaction replaces several small active segments with fewer larger segments while preserving the exact multiset of logical rows, event identities, stored schema, and UTC event day without shortening retention. It never deduplicates events, reinterprets `@rest`, or performs schema migration.

Only one process owns the V0 maintenance loop at a time. It holds a PostgreSQL session advisory lock while claiming and recovering compaction work. Another process may take over after the session ends, but simultaneous active maintenance owners are outside the showcase contract.

## 2. Candidate selection and claim

One run consumes a bounded set of `ACTIVE`, unclaimed segments with equal source, stored schema, and UTC event day. Inputs must be smaller than the implementation target and retained long enough to finish the run.

Selection prefers older small segments and stops at bounded implementation limits for input segments, rows, Parquet bytes, and estimated uncompressed bytes. A run is useful only when its expected output count is smaller than its input count.

The claim transaction:

1. locks eligible segment rows;
2. creates one `BUILDING` compaction run with immutable source, schema, and day;
3. sets each input's `claimed_by_compaction_run_id` to the run;
4. commits.

No object-store or local-file work occurs in this transaction. A segment with a non-null claim cannot be selected by another run.

## 3. Construction

The worker reads each input through its exact registered `PUBLISHED` object descriptor and validates its Parquet footer, schema identity, row count, and segment metadata.

Input rows are already ordered by `@event_time` and `@event_id`. The worker performs a bounded streaming merge and cuts outputs at implementation row and byte targets. Every output remains inside the run's source, schema, and event day and receives the maximum input `data_expires_at`.

Construction enforces bounded memory, local staging bytes, input rows and bytes, output rows and bytes, and run duration. It does not hold a PostgreSQL connection while reading or writing Parquet.

After building all bounded outputs locally, one PostgreSQL transaction registers their `PREPARED` segments with `produced_by_compaction_run_id`, registers their `PLANNED` objects, changes the run to `UPLOADING`, and commits. The worker then uploads and verifies each object and changes it to `UPLOADED` without holding that transaction open.

## 4. Correctness checks

Before publication the worker verifies:

- every input remains `ACTIVE` and claimed by this run;
- every output is `PREPARED` and every output object is `UPLOADED`;
- output row count equals input row count;
- output segment count is positive and smaller than input segment count;
- output event-time and ingestion-time minima and maxima equal the combined input minima and maxima;
- all outputs use the run's source, schema, day, and retention deadline.

Exact logical-row preservation is the builder's core invariant and is covered by integration tests using sorted `@event_id` and value comparisons.

## 5. Publication

One PostgreSQL transaction:

1. locks the run, all input segments, output segments, and output objects;
2. repeats the correctness checks against durable state;
3. changes every output object to `PUBLISHED` and output segment to `ACTIVE`;
4. changes every input segment to `SUPERSEDED`, retains its run claim, and sets `reclaim_after` later than the maximum query lifetime;
5. changes the run to `COMMITTED` and records completion time;
6. commits.

A query snapshot established before this transaction may read all inputs. A later snapshot reads all outputs and no inputs. No snapshot can observe a partial replacement.

## 6. Failure and recovery

Before publication, failure changes the run to `FAILED`, clears claims from still-active inputs, marks its prepared outputs `ABANDONED`, and leaves their objects eligible for garbage collection. Inputs remain queryable.

After acquiring the maintenance advisory lock at startup, recovery examines every non-terminal run left by the previous owner. A transaction either observes `COMMITTED` or performs the ordinary failure cleanup. Publication itself is one PostgreSQL transaction, so connection loss is resolved from the run's durable state rather than inferred from the error returned to the former process.

V0 does not implement heartbeat renewal, lease stealing, concurrent maintenance owners, or distributed capacity accounting.

## 7. Scheduling and telemetry

The maintenance loop scans at a fixed bounded interval and runs at most a fixed small number of concurrent local compactions. The showcase uses one.

Metrics cover candidate backlog, input and output segments/rows/bytes, build and upload time, successful and failed runs, reclaimed input bytes, and oldest eligible segment age. Stable errors are `COMPACTION_INPUT_INVALID`, `COMPACTION_BUILD_FAILED`, `COMPACTION_NOT_BENEFICIAL`, `COMPACTION_PUBLICATION_FAILED`, and `COMPACTION_RECOVERY_FAILED`.
