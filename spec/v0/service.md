# Elucid v0 Service Specification

- Status: `DRAFT`
- Depends on: [Catalog](catalog.md), [Query Engine](query-engine.md), [Ingestion](ingestion.md), [Compaction](compaction.md), [Retention](retention.md), [Metastore](metastore.md)

## 1. Executable

The product MUST build one executable named `elucid`. Its HTTP runtime MUST use Tokio, Axum, Tower, Tower HTTP, Tokio Util cancellation primitives, Utoipa with Utoipa Axum, and Rust Embed.

`elucid server --config <path>` MUST start HTTP health and metrics endpoints, apply metastore migrations, verify object-store capabilities, recover durable work, initialize every configured runtime role, and admit only the traffic and work owned by those roles.

`elucid catalog apply --endpoint <base-url> --file <path-or-dash> [--operator-bearer-token-environment-variable <name>] [--timeout-seconds <seconds>]` MUST send the exact file or standard-input bytes to `POST /api/v1/catalog-applications`. Timeout MUST default to 120 seconds; expiry MUST stop only local waiting, return exit code `7`, and leave the server outcome indeterminate. Repeating the command with the exact same manifest MUST be safe: it MUST NOT duplicate immutable history or partially mutate catalog state, and its outcome MUST be derived from the current durable catalog.

`elucid ingest send --endpoint <base-url> --source <source-name> --input <input-name> --file <path-or-dash> --idempotency-key <key> [--operator-bearer-token-environment-variable <name>] [--timeout-seconds <seconds>]` MUST send the exact file or standard-input bytes to the HTTP ingestion endpoint. Timeout MUST default to 120 seconds, stop only local waiting, and return exit code `7` on expiry.

When the token option is present, a client command MUST read the token from the named environment variable and send it through `Authorization: Bearer`; the token MUST NOT appear in process arguments, output, or logs. Client commands MUST NOT access PostgreSQL or S3 directly.

Process exit codes MUST be `0` success, `1` uncategorized internal failure, `2` command or document validation failure, `3` configuration failure, `4` remote service availability or dependency readiness failure, `5` catalog conflict, `6` terminal ingestion failure, and `7` local client timeout.

## 2. Configuration

The service MUST read one optional UTF-8 TOML file and then apply `ELUCID_*` environment overrides. An absent optional file, unreadable supplied file, and malformed supplied file MUST have distinct outcomes.

Secrets MUST come from named environment variables or direct secret overrides. Effective configuration, logs, API responses, and diagnostics MUST redact PostgreSQL passwords, S3 secret keys, bearer tokens, session tokens, cursor keys, and every value marked secret.

The literal values below define the bounded acceptance profile. Mode-specific optional fields are specified after the profile and are not implicit service defaults:

```toml
[server]
bind = "127.0.0.1:8080"
browser_origin = "http://127.0.0.1:8080"
network_trust = "LOOPBACK_ONLY"
roles = ["SERVING", "MAINTENANCE"]
maximum_json_request_body_bytes = 1048576
maximum_request_header_bytes = 32768
request_timeout_seconds = 60
header_timeout_seconds = 5
idle_timeout_seconds = 30
shutdown_timeout_seconds = 15
default_page_items = 50
maximum_page_items = 200
cursor_hmac_key_environment_variable = "ELUCID_CURSOR_HMAC_KEY"

[metastore]
postgresql_dsn_environment_variable = "ELUCID_POSTGRES_DSN"
maximum_connections = 10
connection_timeout_seconds = 5
migration_lock_timeout_seconds = 30
statement_timeout_seconds = 30

[catalog]
maximum_manifest_bytes = 1048576
maximum_concurrent_applications = 2

[object_store]
alias = "default"
authority = "showcase-minio"
endpoint = "http://minio:9000"
region = "us-east-1"
bucket = "elucid"
root_prefix = "showcase"
addressing_style = "PATH"
access_key_id_environment_variable = "ELUCID_S3_ACCESS_KEY_ID"
secret_access_key_environment_variable = "ELUCID_S3_SECRET_ACCESS_KEY"
request_timeout_seconds = 30
maximum_request_attempts = 3

[ingestion]
staging_directory = "/var/lib/elucid/staging"
staging_capacity_bytes = 2147483648
maximum_request_body_bytes = 16777216
maximum_record_bytes = 10485760
dead_letter_complete_raw_maximum_bytes = 65536
dead_letter_raw_prefix_bytes = 4096
maximum_dead_letter_page_bytes = 4194304
target_segment_rows = 500
target_segment_uncompressed_bytes = 16777216
maximum_parquet_row_group_rows = 250
maximum_open_event_time_buckets = 8
maximum_concurrent_requests = 4
attempt_heartbeat_interval_seconds = 5
attempt_stale_after_seconds = 30
attempt_timeout_seconds = 900

[compaction]
scan_interval_seconds = 10
working_directory = "/var/lib/elucid/compaction"
working_capacity_bytes = 2147483648
memory_pool_bytes = 268435456
minimum_input_segments = 2
maximum_input_segments = 32
maximum_input_rows = 16000000
maximum_input_uncompressed_bytes = 4294967296
maximum_input_parquet_bytes = 1073741824
target_output_segment_uncompressed_bytes = 268435456
maximum_output_segment_rows = 1000000
maximum_parquet_row_group_rows = 250
maximum_output_segments = 16
maximum_output_parquet_object_bytes = 536870912
maximum_output_parquet_bytes = 2147483648
maximum_concurrent_runs = 1
maximum_cluster_concurrent_runs = 4
maximum_recovery_batch_runs = 100
run_heartbeat_interval_seconds = 5
run_stale_after_seconds = 30
run_timeout_seconds = 900

[garbage_collection]
orphan_grace_period_seconds = 3600
retired_object_grace_period_seconds = 60
scan_interval_seconds = 300
maximum_batch_objects = 100
maximum_concurrent_object_deletions = 4

[retention]
idempotency_retention_seconds = 86400
event_data_retention_seconds = 2592000
dead_letter_retention_seconds = 604800
ingest_provenance_retention_seconds = 2592000
compaction_provenance_retention_seconds = 2592000
scan_interval_seconds = 60
maximum_task_duration_seconds = 30
maximum_expiration_batch_segments = 100
maximum_retry_expiration_batch_requests = 100
maximum_idempotency_expiration_batch_reservations = 100
maximum_provenance_roots_per_batch = 100

[query]
default_output_rows = 1000
maximum_output_rows = 10000
maximum_result_bytes = 16777216
maximum_query_bytes = 65536
maximum_pipeline_stages = 100
maximum_expression_depth = 128
maximum_selected_segments = 10000
maximum_selected_object_bytes = 107374182400
execution_timeout_seconds = 30
memory_pool_bytes = 536870912
spill_directory = "/var/lib/elucid/spill"
spill_capacity_bytes = 2147483648
maximum_concurrent_queries = 2
maximum_queued_queries = 16

[telemetry]
log_format = "JSON"
log_level = "info"
metrics_path = "/metrics"
```

Every size, count, duration, and capacity MUST be parsed into a unit-bearing domain type and validated before dependency access. Required positive values MUST reject zero. Cross-field validation MUST reject an empty or duplicate runtime-role set, a default above its maximum, an owner-stale threshold less than three times its heartbeat interval, a browser origin without `http` or `https`, a page default above its maximum, a record maximum above the ingestion request-body maximum, a dead-letter page maximum not greater than the complete raw capture maximum, `execution_timeout_seconds` above the v0 maximum query snapshot lifetime, `retired_object_grace_period_seconds` not greater than that lifetime, `orphan_grace_period_seconds` not greater than `run_timeout_seconds + object_store.request_timeout_seconds * object_store.maximum_request_attempts`, `minimum_input_segments` below two or above `maximum_input_segments`, `maximum_output_segments` greater than or equal to `maximum_input_segments`, `maximum_output_parquet_object_bytes` above `maximum_output_parquet_bytes`, `maximum_concurrent_runs` above `maximum_cluster_concurrent_runs`, `retention.maximum_task_duration_seconds` greater than or equal to `retention.scan_interval_seconds`, `ingestion.attempt_timeout_seconds` greater than or equal to `retention.idempotency_retention_seconds`, `idempotency_retention_seconds` not greater than `attempt_stale_after_seconds`, `idempotency_retention_seconds` above `ingest_provenance_retention_seconds`, `event_data_retention_seconds` above `ingest_provenance_retention_seconds`, `dead_letter_retention_seconds` above `ingest_provenance_retention_seconds`, or a local capacity below one corresponding maximum request, object, result, or compaction-output allocation. Every arithmetic expression MUST be checked. Compaction working capacity MUST be at least `maximum_concurrent_runs * maximum_output_parquet_bytes`.

Cross-field validation MUST reject `maximum_input_rows` above the checked product of `maximum_output_segments` and `maximum_output_segment_rows` or `maximum_input_uncompressed_bytes` above the checked product of `maximum_output_segments` and `target_output_segment_uncompressed_bytes`.

`server.request_timeout_seconds` bounds HTTP waiting, while `ingestion.attempt_timeout_seconds` bounds durable post-claim execution. Their values are independent because expiry of the former MUST NOT cancel the latter.

Concurrency and local-capacity limits MUST apply independently to each server process. Catalog-application admission, PostgreSQL constraints, and fencing, not an unbounded process-local queue, MUST preserve cluster correctness and overload behavior. Compaction input byte limits apply to registered Parquet bytes and MUST NOT be inferred from object-store listing.

The ingestion `maximum_record_bytes` value is a service-wide safety ceiling. Catalog application MUST reject an ingest-profile revision whose `maximum_record_bytes` exceeds it.

Runtime role MUST be `SERVING` or `MAINTENANCE`; `network_trust` MUST be `LOOPBACK_ONLY`, `LOCAL_CONTAINER`, or `TRUSTED_NETWORK`; `addressing_style` MUST be `PATH` or `VIRTUAL_HOSTED`; `log_format` MUST be `JSON` or `PRETTY`. `operator_bearer_token_environment_variable` is an optional server field and MUST be present exactly under `TRUSTED_NETWORK`. Every present field ending in `_environment_variable` names a required variable. Missing and invalid values MUST produce `CONFIGURATION_SECRET_MISSING` and `CONFIGURATION_SECRET_INVALID`; the decoded cursor HMAC key MUST contain at least 32 bytes. `TRUSTED_NETWORK` MUST require an operator bearer token containing at least 32 visible ASCII bytes and an `https` browser origin.

The `SERVING` role owns the product HTTP API, embedded web application, ingestion execution, and query execution. The `MAINTENANCE` role owns periodic stale-owner recovery, compaction, retention, garbage collection, and provenance pruning. Every instance MUST serve health and metrics endpoints and MAY combine both roles; a deployment MUST run at least one instance of each role.

The effective managed-object root is `s3://{bucket}/{root_prefix}/`. Object-store alias is deployment-facing; authority identifies namespace continuity.

## 3. Lifecycle

Startup phases MUST be `CONFIGURATION`, `HEALTH`, `METASTORE`, `MIGRATIONS`, `OBJECT_STORE`, `RECOVERY`, `RUNTIME`, and `READY`. A phase failure MUST terminate with a stable code and non-zero exit.

During `HEALTH`, the server MUST bind the listener and install health handlers before dependency access. During `MIGRATIONS`, it MUST apply the [Metastore migration protocol](metastore.md#2-migrations) before initializing mutating routes, serving work, or maintenance work. Concurrent compatible servers MUST serialize migration under the advisory lock.

Every server process MUST allocate one UUIDv7 `instance_id`. Concurrent recovery on multiple maintenance instances MUST use the [Ingestion recovery contract](ingestion.md#11-recovery) and [Compaction recovery contract](compaction.md#7-ownership-failure-and-recovery) and MUST NOT require a leader.

Compatible serving instances sharing one metastore and object-store authority MUST support active-active operation behind a load balancer. Request affinity MUST NOT be required; an ingestion retry MAY reach any serving instance and MUST converge through persisted ingest-request state. Any maintenance instance MAY claim eligible recovery, compaction, retention, garbage-collection, or provenance-pruning work, and no maintenance task may require instance affinity.

`GET /health/live` is the process-restart signal. From listener binding until listener close, it MUST perform only a constant-time in-process scheduling check and return `200 {"status":"LIVE"}` while the supervisor and HTTP executor can run the handler. Dependency state, readiness, draining, migration progress, queue saturation, and product-route admission MUST NOT affect it. A critical supervised-task failure that prevents progress MUST terminate the process.

`GET /health/ready` is the configured-role traffic signal. It MUST return `200 {"status":"READY"}` only when PostgreSQL responds within its deadline, migration history exactly matches the embedded manifest, the bucket is addressable, create-only output behavior has been proven, configured-role recovery is complete, and every configured runtime is initialized. A serving instance additionally requires ingestion, query, and object registries; a maintenance instance additionally requires compaction, retention, garbage-collection, and provenance-pruning registries. Otherwise it MUST return `503` with `status = "NOT_READY"` and bounded named checks.

Create-only proof MUST run under a product advisory lock against a fresh UUIDv7 key below `{root_prefix}/.elucid/probes/create-only/`: conditionally create one nonce payload, capture any returned version identity, read and compare the complete payload through a newly constructed object-store client, read and compare one strict non-empty byte range, require a precondition failure when conditionally creating different bytes at the same key, read and compare the first payload again, delete the exact returned version when present or the exact unversioned key otherwise, and prove that the deleted object or version is absent. Inability to address or delete a returned version identity MUST produce `OBJECT_STORE_CAPABILITY_MISSING`. A successful proof MAY be reused by periodic readiness checks.

SIGINT and SIGTERM MUST remove readiness before draining. A serving runtime MUST reject new ingestion requests with `SERVER_DRAINING`, stop new query admission, cancel active queries, allow active ingestion publication transactions to resolve, and fence unfinished attempts. A maintenance runtime MUST stop new recovery, compaction, retention, garbage-collection, and provenance-pruning claims; cancel compaction construction and upload work; resolve active publication and expiration transactions; and fence unfinished runs. The process MUST close listeners and exit within the shutdown deadline. Liveness MUST remain available until listener close.

## 4. HTTP conventions

Product endpoints MUST use `/api/v1`. Health, metrics, and static assets use their unversioned paths.

The HTTP boundary MUST authorize every admitted client as one operator principal. `LOOPBACK_ONLY` MUST reject a bind address containing any non-loopback interface during configuration validation. `LOCAL_CONTAINER` MAY bind another interface, MUST require an `http` browser origin whose host is loopback, and MUST treat every peer that can reach the listener through a container network as trusted. Both local modes MUST admit product and metrics requests without authentication. `TRUSTED_NETWORK` MAY bind another interface and MUST require the configured bearer token on every `/api/v1` and metrics request before reading its body or resolving a product resource. Missing, malformed, or unequal credentials MUST return `401 AUTHENTICATION_REQUIRED` with `WWW-Authenticate: Bearer` and indistinguishable bounded details. Token comparison MUST be constant-time over exact bytes. Health and static-asset requests MUST remain unauthenticated.

A `LOCAL_CONTAINER` deployment MUST restrict listener-reachable container-network membership to trusted workloads and publish the container port only to a host loopback address. Host loopback publication MUST NOT be treated as isolation from peers on that container network. A `TRUSTED_NETWORK` deployment MUST expose product and metrics traffic only through TLS termination and restrict the plaintext bind address to the trusted terminator network. Either non-loopback bind mode MUST emit a startup warning containing the resolved bind addresses, browser origin, and network-trust mode without containing credentials. Origin validation is a browser boundary and MUST NOT be represented as client authentication.

The documented Prometheus scrape configuration for `TRUSTED_NETWORK` MUST use HTTPS and set `authorization.type` to `Bearer` and `authorization.credentials_file` to a mounted secret whose credential matches the configured operator token. It MUST NOT inline the credential in the scrape configuration. Reloading either side with a different credential MAY temporarily fail scrapes with `AUTHENTICATION_REQUIRED` and MUST NOT weaken authorization.

The repository MUST contain a deterministic OpenAPI 3.1 document for every product operation, schema, enum, status, error envelope, and bearer security scheme. Utoipa generation MUST reproduce it exactly in CI. `GET /api/v1/openapi.json` MUST serve the checked-in bytes embedded in the executable.

JSON responses MUST use UTF-8 and `Content-Type: application/json; charset=utf-8`. Requests with media type `application/json` MUST reject duplicate keys, non-finite numbers, trailing non-whitespace bytes, and bodies above `maximum_json_request_body_bytes`. Body limits MUST apply before complete buffering. A validated JSON request MUST be decoded exactly once into a typed transport value.

HTTP ingestion bodies MUST follow Section 7 and the [Ingestion body contract](ingestion.md#3-http-body-staging). They MUST NOT be buffered as JSON transport values.

When `Origin` is present, it MUST equal the configured browser origin; otherwise the service MUST return `403 ORIGIN_NOT_ALLOWED`. Wildcard CORS MUST NOT be emitted.

Timestamps MUST be RFC 3339 UTC strings with exactly three fractional digits. Persistent IDs MUST be lowercase hyphenated UUID strings. Persistent positions, byte sizes, and event counts MUST be non-negative decimal strings. Bounded page counts, row counts, schema versions, and millisecond durations MUST be JSON integers within JavaScript exact range.

A measurement-bearing field MUST spell its English unit in full. Durations and elapsed times MUST include a unit. Examples are `elapsed_milliseconds`, `timeout_seconds`, `selected_bytes`, and `output_rows`.

Every response MUST contain `X-Request-Id`. A valid caller UUID MUST be preserved; otherwise the server MUST generate UUIDv7. Every request log MUST use that identity.

## 5. Ingestion idempotency and pagination

Every HTTP ingestion request MUST require `Idempotency-Key` containing 1 through 128 visible ASCII bytes. The key digest MUST be BLAKE3 over `elucid:http-idempotency-key:v1\0`, the unsigned 32-bit big-endian key-byte length, and the exact key bytes. The digest MUST be scoped by resolved input identity and persisted only in the active `ingest_idempotency_keys` reservation; the raw key MUST NOT be persisted.

While its reservation remains active, the same input, key, body digest, and body byte count MUST converge on one ingest-request identity. A committed match MUST return its result with `Idempotency-Replayed: true`. A terminal failed match MUST replay its failure. A non-stale processing match MUST return `INGEST_REQUEST_IN_PROGRESS` with ingest-request identity and bounded `Retry-After`. Another body under the same input and key MUST return `IDEMPOTENCY_KEY_REUSED`. After reservation expiry, the same key MUST create a new ingest request according to the [Retention contract](retention.md#3-idempotency-and-retry-expiration).

`POST /api/v1/query-executions` MUST reject `Idempotency-Key` as `INVALID_REQUEST` without reading or writing ingestion state.

List endpoints MUST accept optional `page_items` and `cursor` query parameters. On an initial page, `page_items` MUST default to `default_page_items` and fit `[1, maximum_page_items]`. On a continuation page, absent `page_items` MUST use the cursor's page size and a supplied value MUST equal it. Responses MUST use `{"items":[...],"next_cursor":null|string}` with stable ordering. A cursor MUST contain version, endpoint kind, canonical filter digest, page size, and final ordering key, authenticate those bytes with HMAC-SHA256, and use unpadded base64url. Decode, version, endpoint, filter, page-size, and authentication failures MUST return `INVALID_CURSOR`.

## 6. Catalog application and source API

`POST /api/v1/catalog-applications` MUST require `Content-Type: application/yaml`, absent or `identity` `Content-Encoding`, and no `Idempotency-Key`. Another media type MUST return `415 UNSUPPORTED_MEDIA_TYPE`; another content coding MUST return `415 UNSUPPORTED_CONTENT_ENCODING`; an `Idempotency-Key` MUST return `400 INVALID_REQUEST`. The request body MUST be the exact UTF-8 YAML catalog manifest and MUST NOT be rewritten by the client or transport.

The endpoint MUST acquire one process-local catalog-application permit before reading the body. Exhausted admission MUST return `429 CAPACITY_EXHAUSTED` without reading the body. A declared or observed body size above `catalog.maximum_manifest_bytes` MUST return `413 REQUEST_TOO_LARGE`; otherwise the server MUST buffer the complete bounded body, decode it exactly once, and validate and canonicalize it according to the [Catalog](catalog.md#6-manifest).

After document validation, the server MUST execute the [catalog-application transaction](catalog.md#7-catalog-application) through one semantic metastore operation. Any serving instance MAY handle the request; request affinity MUST NOT be required, and the source-name PostgreSQL advisory lock MUST serialize concurrent applications for the same source across the cluster.

A `CREATED` result MUST return `201`; `UPDATED` and `UNCHANGED` MUST return `200`. Every successful response MUST contain the catalog result defined by the Catalog specification and `Location: /api/v1/sources/{source_id}`. If the client cannot determine the response after submitting the complete body, resubmitting the exact manifest MUST preserve committed identities and return the outcome or conflict implied by the current durable catalog.

`GET /api/v1/sources` MUST order sources by name and source identity. Each item MUST contain source identity, name, display name, active schema identity and version, input count, active segment count, event count, and minimum and maximum event time. Empty time bounds MUST be null. The service MUST select the requested source page before computing exact counts and bounds for those sources through grouped reads from catalog rows and `ACTIVE` segments in the same read-only transaction under the configured statement timeout. It MUST NOT update or read a mutable per-source statistics row or issue one query per source.

`GET /api/v1/sources/{source_id}` MUST add every schema-version summary, the complete active schema, and every input summary. A schema summary MUST contain identity, version, `ACTIVE` or `HISTORICAL` status, field count, and creation time. A field MUST contain identity, name, logical type, nullability, role, and description.

An input summary MUST contain identity, name, kind, active ingest-profile revision identity and number, and retained processing, retryable, committed, and failed ingest-request counts. The service MUST compute exact counts for all returned inputs in one grouped query under the configured statement timeout using the metastore access path; it MUST NOT issue one query per input.

## 7. Ingestion API

`POST /api/v1/sources/{source_name}/inputs/{input_name}/events` MUST require `Content-Type: application/x-ndjson`, absent or `identity` `Content-Encoding`, and `Idempotency-Key`. Another content coding MUST return `415 UNSUPPORTED_CONTENT_ENCODING` before reading the body. The request body MUST be the complete ingestion input. Source and input names MUST resolve to one active input before body admission. After complete staging, the endpoint MUST claim the ingest request, pin the current profile and schema, and execute the [Ingestion attempt protocol](ingestion.md#10-attempt-protocol).

Successful `CLAIMED_NEW` processing MUST return `201`. Successful `CLAIMED_RETRY` processing MUST return `200`. A `REPLAY_COMMITTED` result MUST return `200` with `Idempotency-Replayed: true`. A response with rejected records remains successful because their dead-letter object is part of the same commit.

The ingestion operation is synchronous with respect to success: a successful response MUST follow complete object upload, verification, and atomic publication. After claim, execution belongs to a supervised attempt task and is independent of the HTTP waiter's lifetime. If `server.request_timeout_seconds` expires while the transport remains writable, the endpoint MUST return `408 REQUEST_TIMEOUT` with the ingest-request identity in error details and MUST NOT cancel that task. A same-key retry MUST converge through the persisted claim outcomes.

A successful response MUST contain ingest-request, ingest-commit, source, input, pinned profile, and pinned schema identities; `ingest_time`; body byte count; accepted, rejected, and ignored-blank record counts; segment, Parquet-object, and dead-letter-object counts; nullable minimum and maximum event time; elapsed milliseconds; state `COMMITTED`; and self link.

`GET /api/v1/ingest-requests/{ingest_request_id}` MUST return immutable identity, body digest, body byte count, pins, state, committed counters, event-time bounds, ingest time, retry expiry, nullable provenance expiry, a nullable active-attempt summary containing identity, state, deadline, and creation time, terminal failure when present, and lifecycle timestamps. The body digest MUST be lowercase hexadecimal. Raw idempotency keys and their digests MUST NOT appear.

`GET /api/v1/ingest-requests?input_id={id}&state={state}` MUST filter by optional input and state and order by creation time and ingest-request identity.

An ingest-request response MUST include a weak ETag derived from ingest-request update version, active attempt identity, and attempt update version. Heartbeat time MUST NOT appear in the response or participate in that ETag. `If-None-Match` MUST return `304` when unchanged.

`GET /api/v1/ingest-requests/{ingest_request_id}/dead-letter-entries` MUST return the committed request's dead-letter entries in input-position order using the common list envelope. A committed request without a dead-letter object MUST return an empty list. A non-terminal request MUST return `INGEST_REQUEST_NOT_COMMITTED`; a failed request MUST replay `INGEST_REQUEST_FAILED`.

The endpoint MUST read only the directly referenced `PUBLISHED` dead-letter object and MUST NOT expose its bucket, key, or credentials. A `DELETE_PENDING` or `DELETED` object MUST produce `DEAD_LETTER_EXPIRED`. Each item MUST contain the complete entry defined by the [dead-letter contract](ingestion.md#8-dead-letters). Its cursor MUST additionally bind the ingest-request identity, object identity, object digest, and next entry byte offset. The implementation MUST resume at that authenticated line boundary, stream a bounded exact-key range, and stop before exceeding either `page_items` or `maximum_dead_letter_page_bytes`. An entry that cannot fit an otherwise empty page or invalid NDJSON produced by Elucid MUST produce `PUBLISHED_OBJECT_CORRUPT`.

The server MUST apply backpressure before reading a body. Exhausted ingestion concurrency or staging capacity MUST return `429 CAPACITY_EXHAUSTED` with bounded `Retry-After`.

## 8. Query API

`POST /api/v1/query-executions` MUST accept:

```json
{
  "query": "source demo_logs | filter status >= 400 | project @event_time, service, status | sort by -@event_time | take 100",
  "time_range": {
    "start_inclusive": "2026-08-01T00:00:00.000Z",
    "end_exclusive": "2026-08-02T00:00:00.000Z"
  },
  "output_rows": 1000
}
```

Query text and both request bounds are required. Output rows MUST default to the configured default and fit `[1, maximum_output_rows]`. Request bounds MUST be valid UTC millisecond instants with start before end. Language source bounds MAY replace either request bound according to the [language contract](query-language.md#4-time-expressions).

The response MUST contain query identity, resolved source and active-schema identities, effective time range, query reference time, snapshot segment count, object count, stored-schema count, selected bytes, typed columns, rows, diagnostics, completion, truncation, output rows, output bytes, and elapsed milliseconds. Successful diagnostics MUST contain only warnings from the [query diagnostic registry](query-language.md#9-diagnostics).

Completion MUST be `COMPLETE` or `TRUNCATED`. Truncation MUST be null for complete results or contain `reason = ROW_LIMIT` with `limit_rows` or `reason = BYTE_LIMIT` with `limit_bytes`.

Rows MUST be arrays aligned with columns and encode values as follows:

| Logical type | JSON value |
|---|---|
| `null` | Null |
| `bool` | Boolean |
| `int32`, `uint32` | Number |
| `int64`, `uint64` | Decimal string |
| `float32`, `float64` | Finite number |
| `utf8` | String |
| `datetime` | RFC 3339 UTC string with millisecond precision |
| `eid` | 32 lowercase hexadecimal characters |
| `json` | Parsed JSON value |

A logical null and a JSON null in a `json`-typed column both encode as JSON `null`. A query that must preserve their distinction MUST project an existence expression such as `rest_exists("key")` alongside the value.

Every response MUST include `X-Elucid-Query-Id`. Every query error MUST include the query identity in error details.

## 9. Errors

Every non-success API response MUST use:

```json
{
  "error": {
    "code": "QUERY_SEMANTIC_ERROR",
    "message": "The query is invalid.",
    "request_id": "0198f0d3-34b2-7a01-8c01-000000000001",
    "details": {},
    "diagnostics": []
  }
}
```

Code is stable; message is human-readable; details is an object; diagnostics is an array. A query diagnostic MUST contain severity, stable code, message, and optional span with start and end byte, line, and column values.

HTTP mapping MUST follow this table:

| Status | Codes |
|---|---|
| `400` | `INVALID_REQUEST`, `INVALID_CURSOR`, `INVALID_TIME_RANGE`, `CATALOG_MANIFEST_INVALID`, `QUERY_SYNTAX_ERROR`, `QUERY_SEMANTIC_ERROR`, `QUERY_RESOURCE_LIMIT_EXCEEDED` |
| `401` | `AUTHENTICATION_REQUIRED` |
| `403` | `ORIGIN_NOT_ALLOWED` |
| `404` | `SOURCE_NOT_FOUND`, `INPUT_NOT_FOUND`, `INGEST_REQUEST_NOT_FOUND`, `ROUTE_NOT_FOUND` |
| `408` | `REQUEST_TIMEOUT`, `QUERY_TIMEOUT` |
| `409` | `CATALOG_DEFINITION_CONFLICT`, `CATALOG_HISTORY_DIVERGED`, `INGEST_REQUEST_IN_PROGRESS`, `INGEST_REQUEST_NOT_COMMITTED`, `IDEMPOTENCY_KEY_REUSED` |
| `410` | `DEAD_LETTER_EXPIRED` |
| `413` | `REQUEST_TOO_LARGE` |
| `415` | `UNSUPPORTED_MEDIA_TYPE`, `UNSUPPORTED_CONTENT_ENCODING` |
| `422` | `CATALOG_PROFILE_TARGET_MISMATCH`, `CATALOG_SCHEMA_INCOMPATIBLE`, `INGEST_REQUEST_FAILED`, `QUERY_CAST_FAILED`, `QUERY_EVALUATION_FAILED`, `QUERY_RESULT_ROW_TOO_LARGE` |
| `429` | `CAPACITY_EXHAUSTED` |
| `500` | `CATALOG_CORRUPTION`, `QUERY_EXECUTION_FAILED`, `QUERY_RESULT_ENCODING_FAILED`, `PUBLISHED_OBJECT_MISSING`, `PUBLISHED_OBJECT_CORRUPT`, `INTERNAL_ERROR` |
| `503` | `METASTORE_UNAVAILABLE`, `OBJECT_STORE_UNAVAILABLE`, `SERVER_DRAINING` |

`SOURCE_NOT_FOUND` reports failure to resolve an HTTP path resource. `QUERY_SOURCE_NOT_FOUND` is an analyzer diagnostic returned inside a top-level `QUERY_SEMANTIC_ERROR` response.

`INGEST_REQUEST_STATE_CONFLICT` is an internal metastore outcome. The service MUST resolve it by reloading durable state and producing a named claim, replay, or fencing outcome; it MUST NOT appear as an API error code.

A persisted record, attempt, dead-letter-build, or ingestion-publication code MUST reside on its owning ingestion row. A persisted compaction code MUST reside on its owning compaction run. A persisted shared object-upload, fencing, or ambiguous-publication code MUST reside on its owning durable work. These operational codes and migration and garbage-collection codes MAY additionally appear in readiness details or operational telemetry but MUST NOT escape through an unrelated product response. Synchronous dependency failure MUST map to the stable public availability code.

## 10. Limits and telemetry

The server MUST enforce bounded HTTP headers, JSON bodies, catalog manifest bodies, ingestion bodies, dead-letter pages, idle time, HTTP request lifetime, ingestion-attempt lifetime, query text, pipeline stages, expression depth, selected segments, selected bytes, result rows, result bytes, query memory, spill bytes, staging bytes, record bytes, open event-time buckets, concurrent catalog applications, concurrent ingestion requests, concurrent queries, queued queries, compaction inputs, compaction outputs, compaction memory, compaction working bytes, concurrent compaction runs, recovery batches, retry-expiration batches, idempotency-reservation-expiration batches, segment-expiration batches, provenance-pruning batches, garbage-collection batches, and concurrent object deletions.

Structured events MUST cover startup phases, configured runtime roles, migration operations, HTTP requests, authentication outcomes, catalog application, ingest-request claims, ingestion state transitions, compaction claims and state transitions, object upload, publication, recovery, retention expiration, garbage collection, provenance pruning, query phases, and shutdown. Events MUST use stable phase and outcome names and full unit suffixes.

`GET /metrics` MUST return Prometheus text format with process, HTTP, authentication, catalog, query, ingestion, compaction, publication, storage, retention, garbage-collection, provenance-pruning, dependency, and semaphore metrics. Duration metric names MUST end in `_seconds`; byte metric names MUST end in `_bytes`. Labels MUST use bounded vocabularies and MUST NOT contain persistent identities, object keys, query text, request bodies, record data, idempotency keys, bearer tokens, or error messages.
