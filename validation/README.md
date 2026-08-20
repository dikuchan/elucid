# Local validation with PostgreSQL and MinIO

This directory contains a Docker Compose setup for running Elucid against local PostgreSQL and MinIO instances. Elucid itself runs on the host and connects to both services through loopback ports.

The containers use fixed development credentials and persist their data in Docker volumes. Do not reuse these credentials or expose these ports outside a local development machine.

The services are available at:

- PostgreSQL: `127.0.0.1:55432`
- MinIO API: `http://127.0.0.1:59000`
- MinIO console: `http://127.0.0.1:59001`
- Elucid: `http://127.0.0.1:58080`

The MinIO console credentials are `elucid` / `elucid-validation`.

## Start the dependencies

Run from the repository root:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose -f validation/docker-compose.yaml up -d --wait
env -u DOCKER_DEFAULT_PLATFORM docker compose -f validation/docker-compose.yaml run --rm minio-init
```

The first command starts PostgreSQL and MinIO and waits until both are healthy. The second command creates the private `elucid` bucket if it does not already exist. Unsetting `DOCKER_DEFAULT_PLATFORM` prevents a shell-wide platform override from being applied to this stack.

Inspect the containers and their logs with:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose -f validation/docker-compose.yaml ps
env -u DOCKER_DEFAULT_PLATFORM docker compose -f validation/docker-compose.yaml logs postgres minio
```

## Run Elucid

Export the local credentials in the shell that will run Elucid:

```shell
export ELUCID_METASTORE__POSTGRESQL_URL=postgresql://elucid:elucid-validation@127.0.0.1:55432/elucid
export ELUCID_OBJECT_STORE__ACCESS_KEY_ID=elucid
export ELUCID_OBJECT_STORE__SECRET_ACCESS_KEY=elucid-validation
```

Start the server from the repository root:

```shell
cargo run --manifest-path elucid/Cargo.toml -p elucid-cli -- server --config validation/elucid.toml
```

The configuration stores the local spool and query scratch data under `/tmp/elucid-validation`.

Check the service from another shell:

```shell
curl --fail --show-error http://127.0.0.1:58080/health/live
curl --fail --show-error http://127.0.0.1:58080/health/ready
curl --fail --show-error http://127.0.0.1:58080/api/v1/status
```

## Check dependency failures

Stop MinIO and verify that readiness returns `503 SERVER_NOT_READY` while liveness remains available:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose -f validation/docker-compose.yaml stop minio
curl --include http://127.0.0.1:58080/health/ready
curl --fail --show-error http://127.0.0.1:58080/health/live
env -u DOCKER_DEFAULT_PLATFORM docker compose -f validation/docker-compose.yaml up -d --wait minio
```

Use the same sequence with `postgres` to check metastore failure and recovery. Restarting MinIO does not require another `minio-init` run because the bucket is stored in the persistent MinIO volume.

## Stop or reset the dependencies

Stop the containers without deleting PostgreSQL or MinIO data:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose -f validation/docker-compose.yaml down
```

To delete all PostgreSQL and MinIO data created by this setup, remove the containers and their volumes:

```shell
env -u DOCKER_DEFAULT_PLATFORM docker compose -f validation/docker-compose.yaml down --volumes
```

Docker Compose does not remove the local Elucid data under `/tmp/elucid-validation`.
