# Elucid v0 Query Engine Specification

This document owns catalog snapshots, segment selection, stored-to-active schema adaptation, DataFusion planning, execution limits, cancellation, and result integrity.

## 1. Planning snapshot

One `REPEATABLE READ`, read-only PostgreSQL transaction:

1. resolves the source and captures its active schema;
2. analyzes the query into typed IR using that schema and one captured query reference time;
3. selects `ACTIVE` segments whose event-time bounds overlap the resolved query range;
4. joins each segment to its exact `PUBLISHED` Parquet object descriptor;
5. loads every distinct stored schema required by the selected segments;
6. validates the required stored-to-active adapters;
7. commits after materializing this immutable execution snapshot.

The captured query reference time is PostgreSQL transaction time. Planning never lists an object-store prefix and never refreshes catalog or segment metadata during execution. Segment selection orders descriptors deterministically by event day, segment identity, and object identity.

The query range is half-open `[start_inclusive, end_exclusive)`. PostgreSQL bounds prune segments, and the same row predicate is always applied during execution.

## 2. Snapshot safety

A compaction or expiration transaction may retire segments after planning. Their objects remain readable until `reclaim_after`, which is later than the maximum query lifetime. Therefore a query may finish from the exact snapshot it captured without holding a PostgreSQL transaction during object-store reads.

A planning snapshot observes either all compaction inputs or all outputs because publication changes their states in one transaction.

## 3. Schema adaptation

Before yielding rows, the reader validates Parquet schema, field identities, segment identity, source, stored schema, row count, and object digest metadata against the snapshot.

For each active-schema field:

1. If the stored schema contains the same `field_id`, read it and require identical logical type and nullability compatible with the active field.
2. If the field is absent and declares `historical_remainder_pointer`, extract that pointer from stored `@rest` and apply the declared JSON conversion; absence, JSON null, or conversion failure yields typed null.
3. If the field is absent without a historical adapter, produce typed null.
4. Reject the adapter if the missing active field is non-null or catalog identities/types contradict the validated history.

Historical conversion failures increment a bounded metric by target logical type. Logs may identify the source and field but never the event value. These failures do not make arbitrary unknown query identifiers valid.

Stored fields absent from the active schema are not exposed. System fields retain their reserved identities and exact types in every schema.

## 4. DataFusion execution

The Elucid table provider is created from the snapshot's explicit Parquet descriptors and active schema. It applies:

- projection pushdown for required stored columns;
- Parquet row-group and page pruning where valid statistics are available;
- the mandatory event-time row predicate;
- stored-to-active schema adaptation;
- the typed user pipeline.

V0 performs no distributed execution, join, external index lookup, or Tantivy planning. One server process executes a query locally through DataFusion.

The engine does not rely on implicit object ordering. A deterministic result order requires an explicit `sort` stage.

## 5. Bounds

Before execution, planning rejects a snapshot exceeding the reported implementation segment-count limit or configured total Parquet bytes. Execution enforces query timeout, cancellation, memory pool, scratch capacity, output rows, output bytes, and a reported maximum encoded row size.

Arithmetic over counts and bytes is checked. A limit violation returns `QUERY_RESOURCE_LIMIT_EXCEEDED`; reaching an output row or byte limit returns a successful truncated result with the exact limiting reason.

The physical pipeline streams bounded Arrow batches internally. The synchronous HTTP handler MAY buffer the bounded result so it returns either one complete success document or one error document; buffered output counts against the query memory and output-byte limits.

## 6. Cancellation and integrity failures

Client disconnect, server shutdown, or query timeout cancels DataFusion work and object-store reads. Cancellation never changes catalog, segment, or object state.

A missing or corrupt published object, mismatched Parquet footer, impossible schema adapter, or row-count contradiction fails the complete query. The engine never silently skips a selected segment or returns a partial result as complete.

Stable errors are `QUERY_RESOURCE_LIMIT_EXCEEDED`, `QUERY_TIMEOUT`, `QUERY_CANCELLED`, `QUERY_EXECUTION_FAILED`, `PUBLISHED_OBJECT_MISSING`, `PUBLISHED_OBJECT_CORRUPT`, and `CATALOG_CORRUPT`.
