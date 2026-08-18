# Elucid v0 Showcase Specification

- Status: `DRAFT`
- Depends on: [Elucid v0](README.md)

## 1. Outcome

The showcase MUST provide a reviewable Elucid vertical slice that starts with one command, ingests deterministic security events into immutable Parquet segments, compacts small segments without changing query results, executes typed queries in a browser, survives an Elucid restart, and requires no private infrastructure.

The operator journey MUST be:

1. Build the executable, embedded web assets, and container image from a clean checkout.
2. Start PostgreSQL, MinIO, and a serving-only Elucid instance with one documented command.
3. Wait for readiness.
4. Apply the demo source manifest through `elucid catalog apply` and the product HTTP API.
5. Send the demo NDJSON fixture through the HTTP ingestion endpoint with a stable idempotency key.
6. Observe the ingest request reach `COMMITTED`.
7. Inspect source, schema, event-time bounds, segments, and event count.
8. Observe the deployment start the maintenance Elucid instance after pre-compaction verification succeeds.
9. Observe active segment count decrease without changing event count or event-time bounds.
10. Execute filtering, projection, casting, sorting, limiting, and aggregation queries.
11. Inspect typed rows, statistics, truncation state, and source-span diagnostics.
12. Restart Elucid and obtain identical rows and statistics.
13. Send the same body with the same idempotency key and observe a committed replay without new segments.

## 2. Topology

```text
Browser
  |
  v
Elucid serving instance -----------+
  |-- embedded web application     |
  |-- HTTP API and metrics         |
  |-- catalog and ingestion        |
  `-- DataFusion query runtime     |
                                    +--> PostgreSQL
Elucid maintenance instance -------+--> MinIO
  |-- recovery                     |
  |-- compaction                   |
  |-- retention and pruning        |
  `-- garbage collection ----------+
```

Each Elucid instance MUST be one operating-system process running the same executable with its configured roles. PostgreSQL and MinIO remain external durable services. Local staging and spill storage is replaceable.

The server MUST default to `127.0.0.1:8080` with `network_trust = LOOPBACK_ONLY`. The Compose configuration MUST use `LOCAL_CONTAINER`, bind its container listener explicitly, and publish it only as `127.0.0.1:8080` on the host.

## 3. Repository and stack

The repository root MUST contain sibling `elucid/` and `ui/` directories. `elucid/` contains the Cargo workspace and all Rust code. `ui/` contains frontend source, manifests, lockfile, build configuration, generated transport contracts, and frontend tests.

Every Rust dependency, including development, build, target-specific, and internal path dependencies, MUST be declared once in root `elucid/Cargo.toml` under `[workspace.dependencies]`. Member manifests MUST use `{ workspace = true }` and MAY add only member-specific features. CI MUST reject any member-owned version, path, Git source, registry source, or default-feature policy.

PostgreSQL access MUST use SQLx without an ORM. The frontend MUST use React function components, strict TypeScript, Vite, Zod, CodeMirror 6, npm, and a committed `ui/package-lock.json`.

The release executable MUST embed the complete verified `ui/dist/` build. Runtime MUST require neither Node.js nor a frontend filesystem directory.

## 4. Demo catalog

The repository MUST contain one current manifest for source `demo_logs` with this semantic declaration:

```yaml
format_version: 1
source:
  name: demo_logs
  display_name: Demo Logs
  active_schema_version: 1
  schemas:
    - version: 1
      fields:
        - { name: source, logical_type: utf8, nullability: NON_NULL }
        - { name: service, logical_type: utf8, nullability: NON_NULL }
        - { name: host, logical_type: utf8, nullability: NON_NULL }
        - { name: severity, logical_type: utf8, nullability: NON_NULL }
        - { name: status, logical_type: int32, nullability: NULLABLE }
        - { name: method, logical_type: utf8, nullability: NULLABLE }
        - { name: path, logical_type: utf8, nullability: NULLABLE }
        - { name: user, logical_type: utf8, nullability: NULLABLE }
        - { name: src_ip, logical_type: utf8, nullability: NULLABLE }
        - { name: bytes, logical_type: int64, nullability: NULLABLE }
        - { name: message, logical_type: utf8, nullability: NON_NULL }
  inputs:
    - name: demo_http
      kind: HTTP_NDJSON
      active_ingest_profile_revision: 1
      ingest_profile_revisions:
        - revision: 1
          target_schema_version: 1
          parser_kind: NDJSON
          encoding: UTF8
          line_boundary_policy: LF_WITH_OPTIONAL_CR
          maximum_record_bytes: 10485760
          conversion_policy: STRICT
          unknown_field_policy: CAPTURE_TOP_LEVEL_REMAINDER
          event_time_mapping: { json_pointer: /timestamp, format: RFC3339 }
          mappings:
            - { target_field: source, json_pointer: /source }
            - { target_field: service, json_pointer: /service }
            - { target_field: host, json_pointer: /host }
            - { target_field: severity, json_pointer: /severity }
            - { target_field: status, json_pointer: /status }
            - { target_field: method, json_pointer: /method }
            - { target_field: path, json_pointer: /path }
            - { target_field: user, json_pointer: /user }
            - { target_field: src_ip, json_pointer: /src_ip }
            - { target_field: bytes, json_pointer: /bytes }
            - { target_field: message, json_pointer: /message }
```

The manifest MUST omit generated identities. Applying it twice MUST preserve every identity and return `UNCHANGED` on the second application.

## 5. Demo fixture

The fixture MUST contain 1,204 framed records and fewer than 8,388,608 bytes: 1,200 accepted events, one malformed JSON record, one schema-invalid record, and two blank records. It MUST place 600 accepted events in each UTC day beginning `2026-08-01` and `2026-08-02`.

The fixture MUST contain successful requests, client errors, server errors, authentication failures, two byte-identical accepted records at different positions, and top-level unknown values of string, number, object, array, and JSON-null kinds captured in `@rest`. Exactly one accepted event MUST contain numeric unknown field `experimental_status` with value `503`; every other record MUST omit that key. Exactly one accepted event MUST contain `explicit_null` with JSON null; every other record MUST omit that key.

Under the showcase configuration, ingestion MUST produce one ingest commit, four ingestion-origin Parquet segments, six row groups, one dead-letter object with two entries, 1,200 accepted records, two rejected records, and two ignored blank records. The 16,777,216-byte ingestion-segment target MUST exceed the independently calculated estimate for each 600-row day so the 500-row ingestion target alone determines the four ingestion segments. The 268,435,456-byte compaction target and 1,000,000-row compaction maximum MUST produce one output segment per fixture day.

Automatic compaction MUST commit exactly two runs, one for each fixture day. Each run MUST consume one 500-row and one 100-row ingestion segment and publish one 600-row compaction segment with three row groups. After both publications, the source MUST contain two `ACTIVE` compaction segments and four `SUPERSEDED` ingestion segments while preserving 1,200 events and the original event-time bounds.

The canonical fixture interval is `[2026-08-01T00:00:00.000Z, 2026-08-03T00:00:00.000Z)`. Checked-in expectations MUST contain the exact fixture BLAKE3 digest, counts, time bounds, and rows for every documented query. Tests MUST read these expectations rather than derive them from production code.

## 6. Web application

The application MUST expose one workspace at `/` containing:

- a source list with active schema version, event count, time bounds, and ingestion state;
- a header with active source, readiness, and repository link;
- a UTC picker with inclusive start and exclusive end;
- a CodeMirror query editor initialized with a valid source query;
- a Run button and `Cmd+Enter` or `Ctrl+Enter` shortcut;
- typed result columns, bounded rows, completion, elapsed milliseconds, output rows, selected segments, and selected bytes;
- source-span error and warning diagnostics rendered as code frames;
- ingest-request history, committed counts, terminal state, and paginated dead-letter entries.

For a non-empty source, picker start MUST default to source minimum event time and picker end to one millisecond after inclusive source maximum event time. For an empty source, both defaults MUST derive from one captured UTC instant: end is that instant and start is one hour earlier.

Source selection MUST load catalog detail and picker defaults without executing a query. Query text remains independently editable. Successful execution MUST update the visual source to the source resolved from query text.

All HTTP responses, error bodies, URLs, and browser storage values MUST enter the frontend as `unknown` and be parsed exactly once through Zod before application state. Transport types MUST use `z.infer`; handwritten duplicate wire interfaces MUST NOT exist.

When a product request returns `AUTHENTICATION_REQUIRED`, the application MUST request the operator bearer token, retain it only in memory, and attach it to subsequent product requests. It MUST NOT place the token in a URL, browser storage, log, diagnostic, or rendered error.

OpenAPI generation MUST deterministically produce checked-in Zod transport schemas. CI MUST fail on generated drift. A decode failure MUST become a bounded frontend contract error and MUST NOT expose the rejected value or raw Zod issue tree.

Application decisions, state transitions, result transformations, diagnostic offset conversion, and persistence decoding SHOULD be pure. Network, storage, CodeMirror, focus, and DOM effects MUST remain explicit boundaries.

The result view MUST preserve server order and virtualize or paginate DOM rows. It MUST render UTC datetime by default, preserve 64-bit integer strings, distinguish null, provide a bounded expandable JSON viewer, and display truncation reason and limit prominently.

UTF-8 byte spans MUST convert correctly to CodeMirror document offsets for non-ASCII text. Operational errors MUST display stable code, request identity, and concise recovery action without Rust debug output, SQL, stack traces, or dependency errors.

The browser MAY persist query text, selected source identity, time range, and datetime presentation under versioned keys. It MUST NOT persist event rows, credentials, payloads, or API error details. Invalid stored state MUST be discarded.

Interactive controls MUST be keyboard reachable, visibly focused, and accessibly named. State MUST be conveyed by text and color. Text and essential indicators MUST meet WCAG 2.2 AA contrast. Query, ingestion, completion, and diagnostic changes MUST use scoped ARIA live regions.

At 1,280 CSS pixels and wider, source list, editor, and results MUST remain simultaneously usable. At narrower widths, the source list MAY collapse and tables MAY scroll horizontally. Loading, empty, error, truncated, and complete states MUST preserve editor and Run-control position.

## 7. Static assets

Fingerprint-named assets MUST use `Cache-Control: public, max-age=31536000, immutable`; `index.html` MUST use `Cache-Control: no-cache`. Unknown non-API GET routes MUST return the application shell; unknown API routes MUST return `ROUTE_NOT_FOUND` JSON.

The server MUST emit `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, `X-Frame-Options: DENY`, and `Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'`.

## 8. Local environment and packaging

The repository MUST contain one Compose definition with health-checked PostgreSQL major version `16`, MinIO, bucket initialization, one serving Elucid instance, one HTTP catalog-application client, one HTTP fixture-ingestion client, pre-compaction verification, and one maintenance Elucid instance. The catalog-application client MUST depend on serving readiness and MUST receive neither PostgreSQL nor object-store credentials. The maintenance instance MUST depend on successful pre-compaction verification. Container images MUST be pinned to immutable digests. Dependencies MUST use health or successful one-shot completion, not container start order.

The documented startup command MUST converge on repeated execution. Catalog application, bucket creation, and fixture ingestion with the fixed key `showcase-demo-fixture-v1` MUST treat existing matching state as success.

PostgreSQL and MinIO MUST use persistent named volumes. Recreating Elucid while retaining those volumes MUST preserve source visibility, compaction provenance, and query results. Staging, spill, and compaction working storage MAY use replaceable storage.

The container MUST run as a non-root user, contain the executable and CA certificates, expose the HTTP port, use readiness for its health check, and own writable staging, spill, and compaction working directories.

The build MUST use committed Rust and npm lockfiles. It MUST run `npm ci`, generate and compare OpenAPI and Zod contracts, build `ui/dist/`, and then embed those exact assets. Generated production assets MUST NOT be committed.

`elucid --version --output json` and `GET /api/v1/system/version` MUST expose semantic version, Git revision when available, build profile, frontend asset revision, storage format version, and supported metastore migration range.

## 9. Documentation

The repository README MUST state the product thesis, show topology, provide the clean-checkout startup command, link the v0 specifications, document demo queries and CLI equivalents, explain configuration and secrets, and describe recovery from unavailable PostgreSQL, unavailable object storage, and stale local volumes.

Documentation MUST state tested prerequisites, build and verification commands, startup and shutdown, runtime roles, network trust and bearer-token configuration, authenticated Prometheus scraping through `authorization.credentials_file`, HTTP catalog application, HTTP fixture ingestion, post-claim task ownership, HTTP-waiter and attempt timeouts, sender retry ownership, recommended batching, compaction capacity planning, retention task draining, reclamation, provenance pruning, dead-letter inspection, browser workflow, crate responsibilities, and the exact commit used for published screenshots or recordings. Every quickstart command MUST run in automated verification.

## 10. Verification

The complete showcase MUST pass this clean-state scenario:

1. Build frontend, Rust workspace, executable, and image with warnings treated as errors.
2. Start clean PostgreSQL and MinIO, then start one serving-only Elucid instance and observe `LIVE` before `READY`.
3. Verify automatic migrations and create-only, exact-read, range-read, and exact-delete object-store capabilities.
4. Apply the demo manifest twice through `elucid catalog apply` and `POST /api/v1/catalog-applications`, compare persistent identities, and verify that the catalog client has no PostgreSQL or object-store credentials.
5. Send the fixture to `POST /api/v1/sources/demo_logs/inputs/demo_http/events` with key `showcase-demo-fixture-v1` and verify a new `COMMITTED` ingest request.
6. Verify body digest and byte count, committed counts, four ingestion-origin segments, six ingestion row groups, object digests, footer metadata, direct references, computed source summary, and paginated dead-letter entries through the HTTP API.
7. Verify every `@ingest_time` equals the ingest request's durable creation time and identical records at different positions have distinct event identities.
8. Verify successful pre-compaction verification releases the Compose dependency for one maintenance-only Elucid instance, wait for exactly two committed compaction runs, and verify two active 600-row output segments plus four superseded ingestion segments without changing rows or event-time bounds.
9. Execute `source demo_logs | filter status >= 400 | project @event_time, service, status, path | sort by -@event_time | take 100` and compare exact typed rows.
10. Execute `source demo_logs | project dynamic_status = try_cast(experimental_status as int32) | filter dynamic_status >= 500` and verify exactly one non-null `int32` value equal to `503` plus one `QUERY_FIELD_RESOLVED_FROM_REMAINDER` warning at the field span; execute the equivalent expression with `rest("experimental_status")` and verify the same row without that warning. Verify that `rest_exists("explicit_null")` is true and `rest("explicit_null") != null` is true for the present JSON-null value while both expressions distinguish an absent key.
11. Execute `source demo_logs | summarize events = count() by severity | sort by -events` and compare exact group-key and measure order and values.
12. Execute `source demo_logs | take 10001 | summarize events = count()` with `output_rows = 1` and verify one complete row; execute `take 0` followed by the same aggregation and verify zero.
13. Reject an unaliased aggregate, an unaliased computed projection, and an unknown system field with exact diagnostic codes and UTF-8 spans.
14. Verify row and byte truncation, selected-segment and selected-byte limits, query timeout, cancellation cleanup, and typed empty-source aggregation.
15. Restart Elucid after deleting staging and spill contents and repeat successful queries with identical rows and statistics.
16. Send the same body and key again and verify a replay with unchanged ingest-request identity, event count, segment count, and stored-object count; send another body under that key and verify `IDEMPOTENCY_KEY_REUSED`.
17. Interrupt ingestion at every [failure-outcome boundary](ingestion.md#12-failure-outcomes), retry the complete body with the same key, and verify the required durable result, publication atomicity, fencing, stable event identities, and exact-key orphan cleanup.
18. Run two serving Elucid instances against the same PostgreSQL and MinIO services, route concurrent new requests and retries across both, terminate an active owner, and verify one ingest request and at most one commit per active input-scoped idempotency reservation.
19. Make a required dependency unavailable and verify readiness becomes `NOT_READY` while liveness remains `LIVE`, then recovers without process restart.
20. Open the application in Chromium, run filtering and aggregation queries, inspect a JSON remainder field and dead-letter entries, verify error and warning code frames, and observe completed ingestion.
21. In isolated catalog state, apply schema version `1` with non-null `message`, ingest one event, apply active schema version `2` adding nullable `region` while the active profile still targets version `1`, and verify stable `message` field identity plus typed null `region`; then activate profile revision `2` targeting schema version `2`, ingest one event with `region = "eu-west-1"`, and verify one null and one value in a mixed-schema snapshot. An attempted active schema changing `message` from `utf8` to `int64` MUST fail without mutation.
22. In isolated catalog state, publish one event, capture a query snapshot, publish a second request whose event time precedes the first segment, and verify that the captured snapshot returns only the first event while a new snapshot returns both; the original segment and object MUST remain unchanged and source minimum event time MUST move to the late event.
23. In isolated storage state, create the four fixture segments under a serving-only instance, capture a query snapshot, then start two maintenance instances and verify exactly two committed compaction runs with disjoint day-scoped inputs. Verify that the captured snapshot reads the four input objects, a new snapshot reads the two output objects, both return identical explicitly sorted rows, and no snapshot observes a partial replacement. In a separate state with four eligible segments sharing one event-time bucket and one data-expiry bucket and `maximum_input_segments = 2`, verify that two runs may concurrently reserve disjoint inputs.
24. Verify exact compaction provenance, unchanged event identities and field values, equal input and output row counts, four `SUPERSEDED` input segments, two `ACTIVE` output segments, unchanged source event count and bounds, and updated active segment, object, and byte statistics. Under isolated profiles, verify that the uncompressed-byte target and row maximum independently cause output splits. Before each reclamation time, verify every input object remains `PUBLISHED`; for an eligible superseded fixture, verify exact-key deletion and retained PostgreSQL provenance.
25. Interrupt compaction at every [failure-outcome boundary](compaction.md#8-failure-outcomes), terminate an active maintenance owner, and verify fencing, reservation release, unchanged input visibility before publication, atomic visibility after publication, abandoned-output cleanup, and recovery by another maintenance instance.
26. In isolated storage state, create compaction candidates for one source, schema, and event-time day across two UTC data-expiry buckets. Verify that no run mixes buckets, each output deadline equals its maximum input deadline, and repeated compaction within one bucket never moves a deadline into the next bucket or extends any input deadline by one day or more.
27. Pause a new ingest attempt after claim until `server.request_timeout_seconds` expires, verify `408 REQUEST_TIMEOUT` identifies the ingest request while the attempt retains its staging resources and continues, obtain `IN_PROGRESS` from a concurrent same-key retry, release the attempt, and verify one commit plus `REPLAY_COMMITTED` without a replacement attempt.
28. Under an isolated short attempt timeout, prevent pre-publication progress until `deadline_at`, verify the attempt becomes `FAILED` with `INGEST_ATTEMPT_TIMEOUT`, unpublished outputs and local resources are released, the request becomes `RETRYABLE`, and a same-key retry creates one replacement attempt before retry expiry. Repeat with `deadline_at = retry_expires_at` and verify terminal `INGEST_RETRY_WINDOW_EXPIRED`.
29. Under an isolated short retention profile, verify a `RETRYABLE` request becomes `FAILED` with `INGEST_RETRY_WINDOW_EXPIRED`, its terminal replay remains stable until idempotency-reservation expiry, maintenance deletes the expired reservation without deleting the request, and the same input and key create a new ingest-request identity.
30. Under an isolated short retention profile, capture a query snapshot before segment expiration and verify it completes from the original exact object while a later snapshot excludes the atomically expired segment. Verify object reclamation only after the query-snapshot grace period and source summaries derived from remaining `ACTIVE` segments.
31. Configure each retention batch limit to two and create five eligible items of every kind. Verify one invocation drains each kind through three bounded transactions. Advance a controlled monotonic clock to the task-duration bound after one batch, verify no second batch begins, and verify the next scheduled invocation resumes the backlog.
32. Verify a dead-letter object remains readable before its retention expiry, returns `DEAD_LETTER_EXPIRED` after `DELETE_PENDING` or `DELETED`, and retains its commit reference until provenance pruning.
33. Build a closed two-level compaction provenance graph, expire and reclaim every output, then verify bounded leaf-to-root pruning deletes compaction metadata before ingestion metadata, never deletes a referenced or active row, and makes a pruned ingest-request identity return `INGEST_REQUEST_NOT_FOUND`.
34. Validate all network-trust modes. Verify `LOOPBACK_ONLY` rejects a non-loopback bind, `LOCAL_CONTAINER` requires a loopback browser origin, both local modes admit unauthenticated product and metrics requests, and `TRUSTED_NETWORK` requires an HTTPS browser origin plus a bearer secret. Verify missing and invalid bearer credentials produce indistinguishable `AUTHENTICATION_REQUIRED` responses before ingestion body admission and metrics collection, valid credentials admit both, and the documented Prometheus configuration loads the credential through `authorization.credentials_file` without embedding it.

Repository verification MUST run Rust formatting, compiler warnings as errors, Clippy, selected unit tests for pure parsing and boundary logic, PostgreSQL integration tests for ingestion and compaction transactional invariants, MinIO integration tests for object and reclamation semantics, end-to-end API verification, frontend type checking and linting, focused frontend behavior tests, one browser smoke test, deterministic contract generation, and documentation smoke verification.

Language contract tests MUST cover every keyword classification, quoted reserved identifiers in source, field, and alias positions, exact `now`, implicit and explicit remainder access, present JSON null, absent remainder keys, warning spans, and every diagnostic registry entry.

Every automated test MUST protect an observable contract, survive correct internal refactoring, and execute at the cheapest reliable level. Coverage percentage MUST NOT be an acceptance target.
