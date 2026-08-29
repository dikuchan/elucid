# Local Elucid example

This directory runs a complete local Elucid flow: PostgreSQL, MinIO, one release Elucid server with its embedded UI, catalog bootstrap, and a pinned Vector instance that continuously submits NDJSON batches.

The stack uses fixed development credentials, binds its host ports to loopback, and stores state in Docker volumes. Do not reuse these credentials or expose the stack outside a local development machine.

## Start

Run from this directory:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose up --build --wait
```

The first build downloads public container images, frontend packages, and Rust crates. Later builds reuse Docker and BuildKit caches. Startup applies embedded SQLx migrations, creates the private MinIO bucket, starts Elucid, creates the initial `demo_logs` catalog only when that source is absent, and starts Vector after the catalog is available. Unsetting `DOCKER_DEFAULT_PLATFORM` prevents a shell-wide platform override from being applied to the native Elucid image.

The local endpoints are:

- Elucid UI and API: `http://127.0.0.1:58080`
- Swagger UI: `http://127.0.0.1:58080/swagger` (`http://127.0.0.1:58080/openapi.json` for the raw document)
- Elucid metrics: `http://127.0.0.1:58080/metrics`
- PostgreSQL: `127.0.0.1:55432`
- MinIO API: `http://127.0.0.1:59000`
- MinIO console: `http://127.0.0.1:59001`

The MinIO console credentials are `elucid` / `elucid-example`.

Check the running services and Elucid readiness with:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose ps --all
curl --fail --show-error http://127.0.0.1:58080/health/ready
curl --fail --show-error http://127.0.0.1:58080/api/v1/status
```

With `maintenance.mode = "AUTOMATIC"`, one server owns the PostgreSQL advisory lock, recovers unfinished compactions, and reports maintenance as `UP` while its bounded loop is healthy. A competing server reports `STANDBY` ownership and `DEGRADED`; `DISABLED` also reports `DEGRADED`. Maintenance state does not by itself close ingestion or query readiness.

## Continuous ingestion

Vector generates randomized HTTP logs at a nominal rate of 500 events per second. Each event has varying request, host, method, protocol, status, byte count, referer, and user values; the transform adds a UUIDv7 trace ID, assigns a low-cardinality service and region, converts status to an integer, and formats event time at Elucid's millisecond precision. The actual rate depends on the host and on downstream backpressure.

The HTTP sink writes newline-delimited JSON with an explicit `application/x-ndjson` content type and batches at most 5,000 events, 4 MiB, or two seconds. Elucid keeps builders open across these transport batches and seals them after at most ten seconds, so an unconstrained run produces roughly 5,000-row segments instead of one Parquet object per HTTP request. Vector retains a bounded 512 MiB disk buffer with blocking backpressure.

A separate fixture emits one record with an invalid status at startup and every 60 seconds. This keeps dead-letter handling continuously observable without making object publication for rejected records dominate the normal ingestion workload. Generated fields not declared by the active schema, including the HTTP request details, trace ID, and initial region value, are preserved in `@rest`.

This workload would generate 43.2 million events per day at its nominal unconstrained rate. It is intended for bounded local runs; stop the stack when it is not being exercised and use the cleanup command below when its retained data is no longer needed.

Allow about 15 seconds for the first age-sealed segment, then inspect ingestion and publication:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose logs --tail=100 vector elucid
curl --fail --show-error http://127.0.0.1:58080/metrics
curl --fail --show-error http://127.0.0.1:58080/api/v1/sources
```

Open the UI and select `demo_logs`. The Operations workspace shows the bounded segment and dead-letter lists. The initial schema can query the unpromoted region directly from the remainder:

```text
source demo_logs
| filter status >= 400
| project @event_time, message, service, status, raw_region = try_cast(rest("region") as utf8)
| sort by -@event_time
| take 100
```

Use a UTC range that includes the current time. The UI starts with the preceding 24 hours.

## Promote `region`

Catalog manifests are complete histories. First activate schema version 2, which declares nullable `region` and its historical `/region` remainder pointer while leaving Vector on ingestion profile revision 1:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose run --rm --no-deps --entrypoint elucid catalog-init catalog apply --endpoint http://elucid:58080 --file /example/catalog/demo-logs-region.yaml
```

The logical field is immediately queryable across existing schema-version-1 segments because the query adapter reads their stored `@rest` value:

```text
source demo_logs
| project @event_time, message, region
| sort by -@event_time
| take 100
```

Then activate ingestion profile revision 2 so new events physically store `region` in schema-version-2 segments:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose run --rm --no-deps --entrypoint elucid catalog-init catalog apply --endpoint http://elucid:58080 --file /example/catalog/demo-logs-region-promoted.yaml
```

After the next segment publication, the Operations workspace shows both schema versions. The same query reads old remainder-backed values and new promoted values through one typed `region` column. Re-running `docker compose up` does not overwrite an existing catalog or undo this promotion.

## Restart and dependency recovery

Restart Elucid and wait for the stack to become ready again:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose restart elucid
env -u DOCKER_DEFAULT_PLATFORM docker compose up --wait
```

The Elucid spool, PostgreSQL catalog and publication metadata, MinIO objects, and Vector disk buffer survive container replacement because each uses a persistent volume. A batch already acknowledged by Elucid remains owned by its spool and is recovered by the ingestion worker after restart.

To exercise sender-side buffering, stop Elucid for longer than the Vector batch interval, then start it again:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose stop elucid
env -u DOCKER_DEFAULT_PLATFORM docker compose up --wait elucid
```

While Elucid is unavailable, a new Vector process cannot pass its required readiness health check and the running sink cannot deliver batches, so events remain in its disk buffer. Delivery resumes after Elucid becomes ready.

## Delivery semantics

Elucid returns `202 DURABLY_QUEUED` after a complete request body is fsynced to its local spool. Vector treats the successful response as delivered. Network failures, `429`, `503`, and retryable server failures remain in the bounded sender buffer and are retried with at most 30 seconds between attempts. Permanent client errors such as `400`, `404`, `413`, and `415` are not retried.

This flow is at least once, not exactly once. If Elucid durably appends a batch but the response is lost, Vector may retry the same events as a new batch. Both occurrences are retained because the HTTP interface has no idempotency key and ingestion does not deduplicate them.

## Logs and cleanup

Follow the application and sender logs with:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose logs --follow elucid vector
```

Stop containers while preserving all local data:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose down
```

Delete all data created by this example, including the Elucid spool and Vector disk buffer:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose down --volumes
```

The built image and BuildKit caches are not removed by either command.
