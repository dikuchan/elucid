# Elucid v0 Query Engine Specification

- Status: `DRAFT`
- Depends on: [Catalog](catalog.md), [Query Language](query-language.md), [Storage](storage.md), [Metastore](metastore.md)

## 1. Terminology

| Term | Definition |
|---|---|
| Query execution | One bounded evaluation identified by `query_id`. |
| Query reference time | The immutable UTC millisecond instant from which every relative time expression in one execution is evaluated. |
| Stored schema | The schema identified by a selected segment's `schema_id` and used to encode its Parquet object. |
| Schema adapter | A checked transformation from one stored schema to the captured active schema. |
| Query snapshot | An immutable in-memory value containing the catalog state and exact stored objects selected under one PostgreSQL snapshot. |
| Query snapshot lifetime | The interval from establishment of the PostgreSQL snapshot until every query-local object descriptor, reader, stream, and task derived from it has been released. |

## 2. Planning

The engine MUST allocate a UUIDv7 `query_id`, capture `query_reference_time`, and parse the query before opening a metastore transaction. The request MUST supply default `start_inclusive` and `end_exclusive` UTC millisecond bounds. Language source bounds MUST resolve according to the [time-expression contract](query-language.md#4-time-expressions).

Planning MUST use one PostgreSQL `REPEATABLE READ READ ONLY` transaction and perform these operations in order:

1. Resolve the parsed source name and capture its source identity and active schema.
2. Analyze the query into typed IR using that active schema and the query reference time.
3. Resolve the source interval and require `start_inclusive < end_exclusive`.
4. Select every matching `ACTIVE` segment and its directly referenced `PUBLISHED` `PARQUET_DATA` stored object.
5. Load every distinct stored schema required by the selection.
6. Build and validate every stored-to-active schema adapter.
7. Enforce configured selected-segment and selected-object-byte limits.
8. Copy all selected state into an owned query snapshot and close the transaction.

The planning transaction MUST perform only bounded metastore reads and deterministic in-memory analysis. It MUST NOT perform object-store, local-file, or DataFusion work.

The v0 maximum query snapshot lifetime MUST be 30 seconds. The execution deadline MUST begin before the planning transaction establishes its PostgreSQL snapshot, and configured execution timeout MUST NOT exceed this maximum. Success, failure, timeout, client disconnect, and shutdown MUST release the complete query snapshot before its lifetime expires.

Segment selection MUST constrain `source_id` and use this PostgreSQL range predicate:

```sql
tstzrange(minimum_event_time, maximum_event_time, '[]') && tstzrange(start_inclusive, end_exclusive, '[)')
```

For ordered non-null bounds, the predicate is equivalent to `minimum_event_time < end_exclusive AND maximum_event_time >= start_inclusive`. Selection MUST order metadata by event-time bucket start, segment identity, and object identity.

A query snapshot MUST contain query identity, reference time, resolved source interval, source and active-schema identities, active-schema definition, every stored-schema definition, every adapter, segment identities, origins, direct producer identities, and statistics, object identities, authorities, aliases, buckets, exact keys, nullable remote version identities, expected sizes, and digests. It MUST contain no live database row, transaction, connection, or mutable catalog reference.

One planning transaction MUST observe either the input side or the output side of a committed compaction replacement. A query snapshot MUST NOT combine superseded inputs with their compaction outputs. Every selected object MUST remain readable until the query snapshot lifetime ends under the [object-reclamation contract](storage.md#7-garbage-collection).

Selected segment count and the checked sum of expected object bytes MUST be compared with `maximum_selected_segments` and `maximum_selected_object_bytes`. Overflow or a bound violation MUST produce `QUERY_RESOURCE_LIMIT_EXCEEDED` before a DataFusion plan is created. Planning MUST NOT discover input through S3 prefix listing.

## 3. Schema adaptation

The engine MUST validate a selected Parquet object against its stored schema before adaptation. Every field MUST match stored ordinal, physical name, Arrow type, nullability, logical metadata, and `field_id`.

For each active-schema field, the adapter MUST:

1. Read the stored field with the same `field_id` when present.
2. Require equal role and either equal type or one catalog lossless widening edge.
3. Apply the widening conversion and expose the active field's name, ordinal, type, metadata, and nullability.
4. Inject a typed null array when the field is absent and the active field is `NULLABLE`.
5. Reject the adapter as incompatible when the field is absent and `NON_NULL`, its role or type is incompatible, or stored nullability cannot satisfy active `NON_NULL` semantics.

Stored user fields absent from the active schema MUST NOT appear in the logical relation. System fields MUST have their reserved identities and exact types in every stored schema.

Schema activation uses the same adapter validation over every declared schema. Planning MUST treat an adapter failure after successful catalog activation as `CATALOG_CORRUPTION` or `PUBLISHED_OBJECT_CORRUPT`, according to whether the persisted definition or object bytes violate the captured metadata.

## 4. DataFusion plan

The logical source scan MUST be constructed from the query snapshot's explicit object descriptors. The table provider MUST NOT enumerate an object-store prefix or refresh the metastore during execution.

The table provider MUST read and validate every selected Parquet footer before yielding rows from that object. Projection pushdown MAY avoid decoding unused stored columns but MUST NOT bypass footer, schema-digest, or field-identity validation.

The logical plan MUST apply `@event_time >= start_inclusive AND @event_time < end_exclusive` to the source scan before user pipeline stages. Metadata pruning and the row predicate MUST use the same resolved bounds.

When the source scan selects no segments, the table provider MUST expose the captured active schema over an empty input. Global `count()` over that relation MUST return one row containing zero.

Typed IR casts, operators, projections, sorts, limits, and aggregates MUST lower without changing the [language semantics](query-language.md). An optimizer rewrite MAY reduce physical work but MUST preserve pipeline order where reordering changes results.

A remainder-origin `Field` and `rest` expression MUST lower to exact top-level extraction from `@rest` and return logical `json`; absent keys and null remainder values MUST return null. `rest_exists` MUST lower to exact top-level membership testing and return non-null `bool`.

## 5. Execution bounds

Execution MUST use a bounded DataFusion memory pool, bounded spill capacity, finite deadline within the maximum query snapshot lifetime, and configured maximum concurrent query count. Query-local spill paths MUST remain below the configured spill directory after canonicalization and MUST be removed after success, failure, cancellation, and startup recovery.

The engine MUST stream `RecordBatch` values. It MUST NOT collect an unbounded batch vector or result set in memory.

The HTTP output-row guard MUST be a physical `output_rows + 1` limit placed after the complete user pipeline. The sentinel row MUST be removed before serialization. Its presence means `TRUNCATED` with reason `ROW_LIMIT`; its absence means the logical result fits the row bound. A user `take` stage MUST NOT be reinterpreted as the output guard.

The result encoder MUST add only complete rows and enforce `maximum_result_bytes` over the compact UTF-8 encoding of the complete `rows` array. Exceeding the bound before another complete row produces `TRUNCATED` with reason `BYTE_LIMIT`. One row larger than the bound MUST produce `QUERY_RESULT_ROW_TOO_LARGE`; partial JSON and partial rows MUST NOT be emitted.

The encoder MUST reject non-finite floating-point output as `QUERY_RESULT_ENCODING_FAILED`. Row and byte counts MUST describe the serialized response, not upstream DataFusion batches.

After the final selected-object read and complete bounded result encoding, the engine MUST release every snapshot-derived descriptor, reader, stream, and task before handing the owned response bytes to the HTTP transport. Network transmission of those bytes MUST NOT extend the query snapshot lifetime.

## 6. Cancellation

Deadline expiry MUST cancel the DataFusion task tree and produce `QUERY_TIMEOUT`. Client disconnect and shutdown MUST trigger cooperative cancellation. Cancellation MUST stop object reads and release memory reservations, spill files, streams, and query-local object references before the maximum query snapshot lifetime expires and MUST NOT change catalog, ingestion, compaction, or storage state.

Queue wait time MUST count toward the execution deadline. Admission failure at the configured queue bound MUST produce `CAPACITY_EXHAUSTED` with a bounded retry delay.

## 7. Integrity failures

An absent selected object MUST produce `PUBLISHED_OBJECT_MISSING`. A size, footer, schema, field identity, or required metadata mismatch MUST produce `PUBLISHED_OBJECT_CORRUPT`. An adapter incompatibility among valid persisted definitions MUST produce `CATALOG_CORRUPTION`. Each integrity failure MUST terminate the complete query before a successful response is emitted.

An unclassified DataFusion planning or execution failure MUST produce `QUERY_EXECUTION_FAILED`. Data-dependent failures defined by the language MUST retain their language error code and MUST NOT be reclassified as internal execution failures.

Query logs MUST identify `request_id`, `query_id`, resolved source identity, phase, outcome, elapsed milliseconds, output rows, selected segments, and selected bytes. Query text and result rows MUST NOT appear in default logs.
