# Elucid v0 Ingestion Specification

This document owns HTTP batch admission, durable local spooling, NDJSON processing, event identity, dead letters, segment building, acknowledgement semantics, and crash recovery.

## 1. Contract

V0 ingestion is at-least-once. A successful HTTP response means the complete request body and the catalog identities required to interpret it have been durably committed to the configured local spool. It does not mean that records are valid, searchable, uploaded to S3, or published in PostgreSQL.

Elucid MUST preserve an acknowledged batch across process restart and continue processing it until every non-blank record is represented by either a published event or a published dead-letter entry. This guarantee covers the configured persistent volume. Loss or corruption of that volume MAY lose acknowledged but unpublished data; V0 provides no spool replication.

An ambiguous HTTP result MAY produce duplicates when the sender retries. Elucid does not require `Idempotency-Key`, does not deduplicate content, and does not promise atomic visibility for one HTTP request. Replaying the same durable spool record after an Elucid restart MUST NOT create another logical occurrence.

## 2. Admission and acknowledgement

`POST /api/v1/sources/{source_name}/inputs/{input_name}/events` accepts `application/x-ndjson` with identity content encoding. Before reading the body, the service requires its readiness gate to be open. It rejects a PostgreSQL, object-store, spool-health, or ingestion-worker readiness failure with `503 SERVER_NOT_READY` and bounded `Retry-After`, without beginning a spool append. It then resolves the input from its current in-memory catalog snapshot, checks spool capacity and ingestion concurrency, and rejects unavailable capacity with `429 CAPACITY_EXHAUSTED` and bounded `Retry-After`.

The gate decision is fixed for that request. If PostgreSQL or the object store becomes unavailable after admission, the service still completes the local append and returns `202` when the request body and spool operation succeed. It does not return a late dependency `503` after beginning the admitted append.

The service streams at most the configured HTTP batch bytes and the reported implementation limit of framed records. It rejects a byte- or record-limit violation with `413 INGESTION_BATCH_LIMIT_EXCEEDED`. An unreadable transfer fails with `400 INVALID_REQUEST` when a response is still possible. Neither case acknowledges or commits its tentative spool append. For an admitted body it generates a UUIDv7 `batch_id`, captures one UTC millisecond `ingestion_time`, and durably appends:

- batch identity;
- source, input, profile-revision, and target-schema identities;
- ingestion time;
- exact body byte count and BLAKE3 digest;
- exact body bytes;
- framing required to distinguish a complete committed spool record from a torn write.

The spool format is an implementation detail, but recovery MUST detect and discard an incomplete final append without discarding any earlier committed batch. Acknowledgement requires the data and recovery metadata to survive an ordinary process or host restart according to the persistent volume's documented durability semantics.

After the durable append completes, the endpoint returns `202` with `batch_id`, `ingestion_time`, `body_bytes`, and state `DURABLY_QUEUED`. A disconnect before the client receives that response does not cancel the batch. The sender should retry any request for which it did not receive `202` and must tolerate duplicate events after an ambiguous result.

## 3. Record positions and event identity

The first payload byte of a batch has input position zero. Each non-blank NDJSON record receives the byte position of its first payload byte and a one-based line number.

An accepted event identity is:

```text
first_16_bytes(
  BLAKE3(
    "elucid:event:v0\0" ||
    canonical_uuid_bytes(batch_id) ||
    input_position_u64_be
  )
)
```

The identity represents one occurrence inside one durably accepted batch. It is stable across local replay and compaction, but it is not a content identity and has no global uniqueness constraint across client retries.

## 4. Framing and normalization

LF delimits records. A CR immediately before LF belongs to the delimiter. A non-empty final record does not require a trailing LF. Empty or ASCII-whitespace-only records are ignored.

Each other record MUST be valid UTF-8 and exactly one JSON object. Duplicate JSON object keys are rejected. A record larger than the pinned profile limit is consumed through its delimiter and rejected without retaining its complete payload in memory.

Mappings evaluate the pinned RFC 6901 pointers and distinguish absence from JSON null. Conversion is strict:

- `utf8` accepts a JSON string;
- `bool` accepts a JSON boolean;
- integer types accept an integral JSON number within range;
- floating types accept a JSON number that rounds once to a finite target value using IEEE 754 round-to-nearest, ties-to-even;
- `datetime` accepts the profile's configured RFC 3339 or signed Unix-millisecond representation.

For a nullable field, absence or JSON null becomes Arrow null. For a non-null field either condition rejects the record. Elucid does not perform implicit string-number conversion, saturation, wrapping, or fractional truncation.

An accepted row contains `@event_time`, the batch `@ingestion_time`, deterministic `@event_id`, promoted fields in stored-schema order, and `@rest`. Remainder construction follows the active ingestion-profile contract in [Catalog](catalog.md#4-inputs-and-ingestion-profiles).

## 5. Dead letters

One invalid record does not reject another record in the same batch. Every invalid non-blank record produces one bounded dead-letter entry containing:

- batch identity, line number, and input byte position;
- stable error code and bounded message;
- payload byte count and BLAKE3 digest;
- either the complete payload or a bounded prefix encoded as UTF-8 or base64.

The worker writes at most one immutable NDJSON dead-letter object per batch and publishes it through the ordinary stored-object lifecycle. A batch with no rejected records produces no dead-letter object. Dead-letter publication may complete independently of event-segment publication, but the spool batch is not reclaimable until both accepted and rejected records have durable terminal outputs.

The worker admits only the reported implementation limit of distinct UTC event days from one batch, choosing days by their first occurrence in input order. After that limit, a record for a previously unseen day becomes a dead letter with `RECORD_EVENT_DAY_LIMIT_EXCEEDED`; records for already admitted days continue normally. Together with the admission record-count bound and the payload-prefix bound, this limits per-batch segment and dead-letter fan-out.

Record errors include `RECORD_INVALID_UTF8`, `RECORD_TOO_LARGE`, `RECORD_PARSE_FAILED`, `RECORD_FIELD_MISSING`, `RECORD_FIELD_NULL`, `RECORD_CONVERSION_FAILED`, `RECORD_EVENT_TIME_INVALID`, and `RECORD_EVENT_DAY_LIMIT_EXCEEDED`.

## 6. Segment building

The ingestion worker reads committed spool batches asynchronously and groups accepted rows by source, stored schema, and UTC event-time day. Builders are shared across HTTP batches.

A builder seals a segment when any bounded implementation row target, estimated uncompressed-byte target, or maximum open duration is reached. The worker also limits simultaneously open builders; reaching that limit seals the least recently used builder. Late events may create new segments for an older day. An existing published segment is never reopened or appended.

Rows in a sealed segment are ordered by `@event_time` and then `@event_id`. Segment rows, bytes, open duration, open builders, worker memory, local staging bytes, and concurrent uploads are bounded.

When a builder seals, the worker appends one bounded record to local recovery metadata containing the segment and object identities, immutable output metadata, and the covered batch-position ranges. It makes that record durable before PostgreSQL registration or object-store I/O. This local record is not a PostgreSQL entity or product history; it exists only so replay can resolve already published output and skip the exact covered occurrences.

The worker registers the retained `PREPARED` segment and `PLANNED` object in PostgreSQL, uploads and verifies the object, then publishes them according to [Storage](storage.md#6-publication).

## 7. Spool reclamation and recovery

The worker advances a durable local checkpoint only after all records before that checkpoint have terminal durable outputs. Published segments are resolved by their retained segment identities after restart; a crash after PostgreSQL publication but before local checkpoint advancement MUST reuse the published result rather than create a second segment.

Only spool bytes and local recovery records strictly before the durable checkpoint may be reclaimed. Reclamation MUST NOT depend on HTTP connection lifetime.

At startup, ingestion recovery:

1. removes an incomplete final spool append;
2. loads committed batches and the local checkpoint;
3. resolves any retained output identities against PostgreSQL;
4. marks any `PREPARED` ingestion segment not referenced by recovered local metadata as `ABANDONED` and schedules any unreferenced unpublished dead-letter object for deletion;
5. resumes unpublished dead-letter and segment work;
6. marks ingestion recovery complete only when the spool is writable and recovery state is internally consistent; service readiness additionally requires the dependencies named in [Service](service.md#3-startup-readiness-and-shutdown).

If locally staged Parquet bytes are missing while the object is still `PLANNED`, the worker first verifies the registered exact key. A matching object advances to `UPLOADED`; confirmed absence causes the worker to abandon the prepared metadata, rebuild from retained spool data with new output identities, and durably replace the corresponding local recovery record before new registration; a mismatch is an integrity failure. A verified `UPLOADED` object resumes publication without the local file, and a published segment is never rebuilt because local staging disappeared.

## 8. Backpressure and telemetry

Admission stops before spool exhaustion. The service exposes at least:

- accepted and rejected HTTP batches and bytes;
- acknowledgement and fsync latency;
- spool used bytes, oldest unprocessed age, and durable checkpoint lag;
- parsed, accepted, rejected, and ignored records;
- open builders and sealed segment sizes;
- normalization, Parquet build, upload, and publication latency;
- recovery, retry, and permanent failure counts.

Metric labels MUST use bounded vocabularies and MUST NOT contain source data, object keys, batch identities, or error messages.
