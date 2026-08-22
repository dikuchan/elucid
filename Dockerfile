# syntax=docker/dockerfile:1.7

FROM node:26.3.0-bookworm-slim AS ui-builder

WORKDIR /workspace

COPY ui/package.json ui/package-lock.json ui/
RUN --mount=type=cache,target=/root/.npm npm --prefix ui ci

COPY ui/ ui/
RUN npm --prefix ui run build

FROM rust:1.95.0-bookworm AS rust-builder

WORKDIR /workspace

COPY elucid/ elucid/
COPY --from=ui-builder /workspace/elucid/elucid-service/ui-assets/ elucid/elucid-service/ui-assets/

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/workspace/elucid/target \
    cargo build --manifest-path elucid/Cargo.toml --locked --release --package elucid-cli \
    && cp elucid/target/release/elucid /tmp/elucid

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 elucid \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin elucid \
    && install --directory --owner=elucid --group=elucid /etc/elucid /var/lib/elucid/spool /var/lib/elucid/scratch

COPY --from=rust-builder /tmp/elucid /usr/local/bin/elucid

USER 10001:10001

EXPOSE 58080

ENTRYPOINT ["elucid"]
CMD ["server", "--config", "/etc/elucid/elucid.toml"]
