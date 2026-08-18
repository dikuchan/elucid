# Elucid v0 Ingestion Specification

- Status: `DRAFT`
- Depends on: [Catalog](catalog.md), [Storage](storage.md), [Metastore](metastore.md), [Retention](retention.md)

## 1. Terminology

| Term | Definition |
|---|---|
| Ingest request | The durable identity and state of one complete HTTP NDJSON entity body accepted under one catalog input and one idempotency key. |
| Idempotency reservation | The expiring input-scoped association from an idempotency-key digest to one ingest request and body identity. |
| Staged body | The exact complete request body held in a bounded replaceable local file for one handling attempt. |
| Input record | One NDJSON occurrence identified by a half-open byte range within one ingest-request body. |
| Input position | The zero-based byte offset of an input record's first byte within its ingest-request body. |
| Ingest attempt | One fenced execution that transforms a staged body and prepares one prospective ingest commit. |
| Attempt owner | The server instance identified by a non-terminal ingest attempt and currently authorized to mutate it. |
| Attempt deadline | The persisted PostgreSQL instant after which a non-terminal ingest attempt may no longer begin work outside publication-outcome resolution. |
| HTTP waiter | Request-local control flow that waits for a supervised attempt's durable outcome without owning its post-claim execution. |
| Ingest commit | The immutable record produced by successful atomic publication of one complete ingest request. |
| Segment | One ingestion-origin bounded group of accepted rows serialized as one Parquet data object under the [Storage segment contract](storage.md#3-segment-contract). |
| Stored object | One immutable ingestion-produced Parquet or dead-letter object governed by the [Storage stored-object contract](storage.md#5-stored-object-contract). |
| Publication | The PostgreSQL transaction that creates the ingest commit, activates every output, records committed counters, and completes the ingest request. |
| Dead-letter entry | One bounded diagnostic record for one rejected input-record occurrence. |

One ingest request produces at most one ingest commit and MAY produce multiple attempts, segments, and stored objects. An ingest request, ingest attempt, ingest commit, stored object, and segment are distinct entities.

## 2. Core invariant

One complete ingest request MUST either publish every accepted event and every rejected-record artifact exactly once or remain entirely invisible. The exactly-once guarantee applies to publication; later expiration and reclamation MUST follow the [Retention contract](retention.md) without creating another ingest commit. Elucid MUST return a successful or replayed HTTP response only after the publication outcome is known.

Every input-record occurrence MUST be accounted as accepted, rejected, or ignored blank. Byte-identical records at different positions are distinct events. Byte-identical bodies submitted with different idempotency keys are distinct ingest requests. Ingestion MUST NOT deduplicate by event content.

## 3. HTTP body staging

The ingestion endpoint MUST accept `application/x-ndjson` over a bounded HTTP request body. Body bytes are representation bytes after HTTP message framing; content coding MUST be `identity`. `Content-Length` MAY be absent. A declared or observed body size above `maximum_request_body_bytes` MUST produce `REQUEST_TOO_LARGE` before an ingest request is claimed.

The receiving instance MUST acquire an ingestion-concurrency permit and staging-capacity reservation before consuming the body. A known valid `Content-Length` requires a reservation of that size; an absent length requires a reservation of `maximum_request_body_bytes`. At end-of-body, the reservation MUST shrink to observed body size. The reservation and concurrency permit MUST remain held until staging-file deletion. The receiver MUST stream exact body bytes into one opaque local staging file while computing:

```text
body_blake3_digest = BLAKE3(ASCII("elucid:ingest-request-body:v1\0") || body_bytes)
```

The receiver MUST enforce the byte bound during streaming and MUST NOT hold a PostgreSQL connection while reading the body. It MUST close and reopen the staging file before processing. A body that ends normally is complete; a transport failure before end-of-body MUST delete the staging file and MUST NOT create or mutate an ingest request.

Local staging bytes are replaceable. They MUST be deleted after every claim outcome that does not create an active local attempt and after active-attempt success, failure, cancellation, or ownership loss. Their staging-capacity reservation and ingestion-concurrency permit MUST be released only after deletion. They MUST NOT be used to recover after instance loss.

## 4. Claim and idempotency

The claim operation MUST receive a validated idempotency-key digest from the HTTP boundary and scope it by resolved `input_id`. The claim transaction MUST lock the matching `ingest_idempotency_keys` row and its request when present and produce exactly one named outcome:

1. `CLAIMED_NEW`: resolve the active input, profile revision, and target schema; allocate UUIDv7 ingest-request and attempt identities; create a `PROCESSING` ingest request with immutable body digest, body byte count, pinned identities, database-generated creation time, and retry expiry; create its idempotency reservation and `PREPARING` attempt with heartbeat and deadline; and commit.
2. `REPLAY_COMMITTED`: require the same body digest and byte count and return the committed result without creating an attempt.
3. `IN_PROGRESS`: require the same body digest and byte count and report the existing non-stale attempt without changing it.
4. `CLAIMED_RETRY`: require the same body digest and byte count, require `RETRYABLE` before its retry expiry, create a new `PREPARING` attempt with heartbeat and deadline, transition the request to `PROCESSING`, and commit.
5. `REPLAY_FAILED`: require the same body digest and byte count and return the persisted terminal failure.
6. `IDEMPOTENCY_KEY_REUSED`: return when the key exists with another body digest or byte count and do not mutate it.

A claim MAY atomically fence and abandon a stale non-terminal attempt and its prepared segments. It MUST transition the request to `RETRYABLE` and produce `CLAIMED_RETRY` only before `retry_expires_at`; otherwise it MUST produce the terminal expiry transition and `REPLAY_FAILED`. Staleness MUST use PostgreSQL time and the configured threshold. Registered stored objects from the abandoned attempt MUST remain available for garbage collection.

A concurrent idempotency-reservation conflict MUST restart the claim lookup and return one of the named outcomes; it MUST NOT escape as an internal error. Expired reservations MUST follow the [retention contract](retention.md#3-idempotency-and-retry-expiration).

Every attempt created by `CLAIMED_NEW` or `CLAIMED_RETRY` MUST initialize `heartbeat_at` and `created_at` from one PostgreSQL instant in the claim transaction. It MUST persist `deadline_at` as the earlier of `created_at + ingestion.attempt_timeout_seconds` and the request's `retry_expires_at` by using checked timestamp arithmetic.

For `CLAIMED_NEW` and `CLAIMED_RETRY`, the receiver MUST register one supervised attempt-owner task and transfer the staged file, staging-capacity reservation, and ingestion-concurrency permit to it. This handoff MUST be cancellation-safe: after claim commit, either the registry owns the active attempt and all local resources or the receiver performs the ordinary pre-publication failure transition before releasing them. The HTTP handler becomes only a waiter for the task's durable outcome and MUST NOT own or cancel attempt execution.

The ingest request's database-generated `created_at` MUST use the metastore millisecond-truncation contract and is the exact `@ingest_time` for every accepted event in every attempt. A retry MUST preserve the original ingest-request identity, pinned profile, pinned schema, creation time, body digest, and body byte count.

## 5. Positions and event identity

Every ingest-request body has an independent byte coordinate beginning at zero. Segment flushes, retries, and event-time buckets MUST preserve original positions. Diagnostic line numbers are one-based presentation values and MUST NOT participate in identity.

An input position MUST fit `[0, i64::MAX - 1]` and be less than body byte count. An empty body has no input position.

Every accepted event MUST contain a 16-byte identity computed as:

```text
input =
    ASCII("elucid:event-id:v1\0") ||
    canonical_uuid_bytes(source_id) ||
    canonical_uuid_bytes(ingest_request_id) ||
    input_position_u64_be

@event_id = first_16_bytes(BLAKE3(input))
```

The event identity identifies an occurrence, not unique content. It MUST NOT be enforced as a global uniqueness constraint. JSON boundaries MUST encode it as 32 lowercase hexadecimal characters.

## 6. Framing

Framing MUST consume the complete staged body and tolerate read boundaries inside UTF-8 sequences, JSON tokens, delimiters, and oversized records.

LF is the record delimiter. A CR immediately before LF belongs to the delimiter. Bytes before body end form a final record even without LF.

One optional UTF-8 BOM is accepted only at position zero and belongs to the first record range. Parsing removes it after position and raw-range accounting. Another BOM is ordinary record data.

A record is blank when its payload after delimiter removal contains only ASCII space, horizontal tab, or carriage return. A blank record MUST increment `ignored_blank_record_count` and produce neither event nor dead-letter entry.

For each record, framing MUST retain `input_position`, exclusive `record_end_position`, payload byte count, delimiter byte count, and one-based line number. Delimiter byte count MUST be `0` for an unterminated final record, `1` for LF, and `2` for CRLF. `record_end_position - input_position` MUST equal payload byte count plus delimiter byte count.

The pinned profile's maximum record bytes MUST be finite. When a record exceeds it, the reader MUST consume and hash through the delimiter without retaining complete bytes and emit one `RECORD_TOO_LARGE` dead-letter entry with actual payload byte count, full BLAKE3 digest, and bounded prefix.

The complete ingest-commit input range is `[0, body_byte_count)`. An empty body, a blank-only body, and a body containing only rejected records are valid requests.

## 7. Normalization

Each non-blank payload MUST be valid UTF-8 and exactly one JSON object. Duplicate object keys at any depth MUST be rejected. Invalid UTF-8 produces `RECORD_INVALID_UTF8`; invalid JSON or a non-object root produces `RECORD_PARSE_FAILED`.

Mappings MUST evaluate the pinned RFC 6901 JSON Pointers and distinguish an absent value from explicit JSON null. Conversion MUST use the pinned target schema and strict profile contract:

- `utf8` accepts a JSON string;
- `bool` accepts a JSON boolean;
- integer types accept an integral JSON number within range;
- floating types accept a JSON number, round once using IEEE 754 round-to-nearest ties-to-even, and require a finite target value;
- `datetime` accepts an RFC 3339 JSON string with `Z` or an explicit numeric offset and requires an instant exactly representable in UTC milliseconds.

Conversion MUST reject wrapping, saturation, fractional truncation into an integer, locale-dependent parsing, implicit string-number conversion, non-finite values, and unrepresentable instants.

An absent or null value becomes Arrow null for `NULLABLE`. For `NON_NULL`, absence produces `RECORD_NON_NULL_FIELD_MISSING` and explicit null produces `RECORD_NON_NULL_FIELD_NULL`.

`@event_time` MUST be parsed by the profile's event-time mapping. `RFC3339` requires `Z` or an explicit numeric offset and an instant exactly representable in UTC milliseconds. `UNIX_MILLISECONDS` requires an integral signed 64-bit JSON number. Invalid event time rejects the record.

`CAPTURE_TOP_LEVEL_REMAINDER` MUST remove a top-level property only when a promoted-field or event-time pointer addresses that property exactly. A pointer below a top-level property MUST leave the complete top-level value in the remainder, including the mapped descendant, so sibling data is not lost. Keys MUST be sorted lexicographically and encoded without insignificant whitespace. Number tokens and array order MUST be preserved. An empty remainder MUST become null.

An accepted row MUST contain `@event_time`, pinned `@ingest_time`, deterministic `@event_id`, promoted fields in schema order, and `@rest`. Every retry with the same staged body and pinned identities MUST produce identical normalized values.

## 8. Dead letters

A record-level validation or normalization failure MUST create exactly one dead-letter entry and MUST NOT reject another valid record in the request.

Each entry MUST be one UTF-8 JSON object followed by LF and contain exactly `format_version`, `ingest_request_id`, `input_position`, `record_end_position`, `delimiter_byte_count`, `line_number`, `error_code`, `error_message`, `payload_byte_count`, `payload_blake3`, `raw_encoding`, `raw_capture`, and `raw_value`. `format_version` MUST be JSON integer `1`; identities MUST use canonical UUID strings; `payload_blake3` MUST be 64 lowercase hexadecimal characters. Payload byte count and digest MUST cover exact bytes after delimiter removal and before BOM removal or any decoding.

Positions, delimiter counts, and byte counts MUST use non-negative decimal strings in JSON; line number MUST use a positive decimal string. `raw_capture` MUST be `COMPLETE` only when `raw_value` represents every payload byte and decodes to `payload_byte_count`; otherwise it MUST be `PREFIX`. `raw_encoding` MUST be `UTF8` or `BASE64`.

Messages and raw captures MUST be bounded. A payload at or below `dead_letter_complete_raw_maximum_bytes` MAY use `COMPLETE`; a larger payload MUST use `PREFIX` with at most `dead_letter_raw_prefix_bytes` decoded bytes. A UTF-8 capture MUST contain valid UTF-8 and MUST end on a Unicode-scalar boundary; another byte sequence MUST use unpadded base64. Dependency errors, credentials, and stack traces MUST NOT appear. A request with rejected records MUST produce exactly one dead-letter object containing entries in input-position order.

## 9. Segment construction

The builder MUST enforce finite targets for segment rows, estimated uncompressed bytes, Parquet row-group rows, and simultaneously open event-time buckets. The estimator MUST include variable-length buffers, offsets, and validity buffers with documented conservative overhead.

Events MUST be grouped by UTC event-time day. Reaching the open-bucket limit MUST flush the oldest open bucket; a flush MUST NOT close that day for later events. Every segment output MUST have origin `INGESTION` and satisfy the [Storage segment contract](storage.md#3-segment-contract).

A request with zero accepted records MUST produce zero segments. A request containing only rejected records MUST still publish its dead-letter object and commit. An empty or blank-only request MUST publish a commit with no stored objects.

## 10. Attempt protocol

### 10.1 Prepare

The attempt owner MUST frame and normalize the complete staged body and build every segment and optional dead-letter object locally.

Before finalization, it MUST allocate a UUIDv7 planned ingest-commit identity plus every segment and stored-object identity and final key. The planned commit identity MUST be embedded in every prepared Parquet footer and dead-letter key.

In one output-plan transaction, the attempt owner MUST lock and fence the `PREPARING` attempt, verify its request identity, body digest, byte count, and pinned identities, insert every `INGESTION`-producer `PLANNED` stored object, insert every `INGESTION`-origin `PREPARED` segment with direct data-object reference and data expiry derived from the request, set planned ingest-commit identity exactly once, persist final counters, transition to `UPLOADING`, increment update version, and commit. Failure MUST leave no partial plan.

### 10.2 Upload

The attempt owner MUST upload and verify every planned object according to [Storage](storage.md#5-stored-object-contract). It MAY enter `COMMITTING` only when every required object is `UPLOADED`. An empty object set satisfies this condition.

### 10.3 Publish

Publication MUST execute these operations in one PostgreSQL transaction:

1. Lock the ingest request and attempt.
2. Return the existing ingest commit when the request or attempt is already `COMMITTED`.
3. Require request state `PROCESSING`, attempt state `COMMITTING`, matching planned ingest-commit identity, counters, and update version.
4. Require the request's immutable body digest, byte count, pinned profile, and pinned schema to match the attempt plan.
5. Require every prepared segment and optional dead-letter object to match the durable plan and be `UPLOADED` under the same attempt.
6. Insert the immutable ingest commit with direct ingest-request and dead-letter references.
7. Bind prepared segments to the commit and transition them to `ACTIVE`.
8. Transition directly referenced objects to `PUBLISHED`.
9. Persist committed counters, event-time bounds, completion time, and provenance expiry on the ingest request and transition it to `COMMITTED`.
10. Set the idempotency reservation expiry and optional dead-letter retention expiry from the same publication time.
11. Transition the attempt to `COMMITTED`, set terminal time, and commit.

Only this transaction changes query visibility or completes an ingest request. The HTTP handler MUST report success only after its commit is known.

After an ambiguous commit response, recovery MUST resolve by unique ingest-request and attempt identities. An existing commit is success; an absent commit permits a fenced retry. Connection loss alone MUST NOT determine outcome.

### 10.4 Heartbeats and failure

The attempt owner MUST begin heartbeat renewal after claim and continue at the configured interval until the attempt becomes terminal or ownership is lost. Renewal MUST use PostgreSQL time, update only `heartbeat_at`, predicate on attempt identity, owning instance identity, and non-terminal state, and stop on `ATTEMPT_FENCED`. It MUST NOT read, increment, or predicate on `update_version`. Every other post-claim mutation MUST predicate on attempt identity, expected state, and expected update version.

The attempt deadline MUST include framing, normalization, segment construction, upload, publication, and retry delays inside object-store requests. The owner MUST stop before beginning new work at or after `deadline_at`, cancel local work, and transition the attempt to `FAILED` with `INGEST_ATTEMPT_TIMEOUT` through the ordinary pre-publication failure transaction. A publication transaction already in progress MUST first resolve its durable outcome. When `deadline_at = retry_expires_at`, an uncommitted request MUST become `FAILED` with `INGEST_RETRY_WINDOW_EXPIRED`; otherwise it MUST become `RETRYABLE`.

Failure before publication MUST set a stable attempt error and terminal time and abandon prepared segments in one short transaction. Planned and uploaded objects MUST remain registered for garbage collection. Every pre-publication failure before `retry_expires_at` MUST transition the ingest request to `RETRYABLE`. At or after that instant it MUST transition to `FAILED`, persist `INGEST_RETRY_WINDOW_EXPIRED` as the bounded public failure, set completion and provenance-expiry times, and set the idempotency-reservation expiry. `FAILED` MUST have no other request-level cause in v0.

A client disconnect or HTTP deadline before claim MUST delete the staging file without creating durable ingestion state. After claim, either event MUST stop only HTTP waiting while the supervised attempt continues. Owner loss, shutdown, or explicit server cancellation before publication MUST transition the request to `RETRYABLE` while its retry window remains open and to `FAILED` with `INGEST_RETRY_WINDOW_EXPIRED` otherwise; cancellation during publication MUST resolve the transaction outcome before releasing the attempt.

## 11. Recovery

Startup and periodic maintenance recovery MUST use PostgreSQL time and authority and MUST be safe on every maintenance instance. Recovery MUST claim stale or deadline-expired non-terminal attempts with `FOR UPDATE SKIP LOCKED`, fence and transition each to `ABANDONED`, abandon its prepared segments, retain registered objects for garbage collection, and transition its ingest request to `RETRYABLE` when its retry window remains open or `FAILED` with `INGEST_RETRY_WINDOW_EXPIRED` otherwise.

A `RETRYABLE` ingest request MUST be reprocessed only after a caller supplies the complete body with the same input, idempotency key, body digest, and byte count before `retry_expires_at`. Recovery MUST NOT adopt an earlier attempt's stored object into a replacement attempt.

Startup recovery MUST delete staging files without active local owners. Instance loss MUST NOT alter committed visibility or a terminal ingest-request result.

## 12. Failure outcomes

The durable outcome at each boundary or interruption MUST satisfy this table:

| Failure point | Durable resolution | Query visibility | Same-key retry |
|---|---|---|---|
| Before complete body staging | No ingest request or attempt exists; local bytes are deleted | None | Stages the complete body and produces `CLAIMED_NEW` |
| After complete staging but before claim commit | No ingest request or attempt exists; local bytes are replaceable | None | Stages the complete body and produces `CLAIMED_NEW` |
| HTTP deadline or disconnect after claim while the owner remains live | The supervised attempt retains its local resources and continues to a durable terminal outcome | None before publication; complete request after publication | Produces `IN_PROGRESS` while active and then replays the durable outcome |
| Attempt deadline before publication | The attempt becomes `FAILED`, its unpublished outputs are abandoned, and the request becomes `RETRYABLE` or expires as `FAILED` | None | Produces `CLAIMED_RETRY` before retry expiry or `REPLAY_FAILED` after it |
| After claim commit but before output-plan commit | Request remains `PROCESSING` with one stale `PREPARING` attempt until recovery makes it `RETRYABLE` and abandons the attempt | None | Produces `CLAIMED_RETRY` after fencing and preserves ingest-request identity and event identities |
| After output-plan commit but before all uploads verify | Request remains `PROCESSING`; the stale attempt, prepared segments, and planned or uploaded objects are abandoned or collected by recovery and garbage collection | None | Produces `CLAIMED_RETRY`, rebuilds with new attempt, segment, and object identities, and preserves event identities |
| After every upload verifies but before `COMMITTING` | Durable state remains an `UPLOADING` attempt with only unpublished outputs until recovery abandons it | None | Behaves as the preceding row |
| After `COMMITTING` but before publication commit | Durable state remains `COMMITTING` when the transaction did not commit; recovery abandons its unpublished outputs | None | Produces `CLAIMED_RETRY` after resolving absence of the ingest commit |
| Publication commit outcome is ambiguous | Recovery resolves the unique ingest-request and planned commit identities before choosing an outcome | All committed outputs or none | Replays the existing commit when present; otherwise produces `CLAIMED_RETRY` after fencing |
| After publication commit but before HTTP success | Request, attempt, commit, segments, objects, counters, and retention deadlines are committed | Complete ingest request | Produces `REPLAY_COMMITTED` with identical identities and counters |
| After HTTP success | Same committed state as the preceding row | Complete ingest request | Produces `REPLAY_COMMITTED` |

Fault injection MUST exercise every row. No retry MAY adopt a prepared segment or stored object from an abandoned attempt, and garbage collection MUST NOT delete an object reachable from a committed ingest request.

## 13. Observability and errors

Ingestion logs MUST use the applicable instance, source, input, ingest-request, profile, schema, attempt, commit, segment, and stored-object identities; body byte count; transition; counters; duration milliseconds; and stable outcome. Committed counters MUST remain distinct from attempt counters.

Metrics MUST include a bounded histogram of framed records per request and distinguish new claims, committed replays, in-progress requests, retries, failed replays, key reuse, HTTP-waiter expiry, attempt expiry, and retry-window expiry. Documentation MUST recommend multi-record request batching within the configured body, record, staging, HTTP-waiter, and attempt-lifetime bounds because every request creates control-plane state and every segment creates one synchronous object upload.

Raw events, raw dead-letter values, idempotency keys, credentials, session tokens, and request bodies MUST NOT appear in default logs or metric labels.

Stable errors MUST include `INPUT_NOT_FOUND`, `INGEST_REQUEST_NOT_FOUND`, `INGEST_REQUEST_IN_PROGRESS`, `INGEST_REQUEST_NOT_COMMITTED`, `INGEST_REQUEST_FAILED`, `INGEST_RETRY_WINDOW_EXPIRED`, `INGEST_ATTEMPT_TIMEOUT`, `IDEMPOTENCY_KEY_REUSED`, `INGEST_BODY_READ_FAILED`, `INGEST_STAGING_FAILED`, `ATTEMPT_FENCED`, `RECORD_INVALID_UTF8`, `RECORD_TOO_LARGE`, `RECORD_PARSE_FAILED`, `RECORD_NON_NULL_FIELD_MISSING`, `RECORD_NON_NULL_FIELD_NULL`, and `RECORD_NORMALIZATION_FAILED`.
