# syntax=docker/dockerfile:1

# ---------- build stage ----------
FROM rust:1.91-bookworm AS build
RUN rustup target add wasm32-wasip2
WORKDIR /src
COPY . .
# Native binary (rustls TLS -> no OpenSSL needed at runtime).
RUN cargo build --release -p hp-cli
# Guest WASM modules (separate workspace under modules/).
RUN cd modules && cargo build --release --target wasm32-wasip2

# ---------- runtime stage ----------
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -r -u 10001 -m -d /home/hyperpipe hyperpipe

COPY --from=build /src/target/release/hyperpipe /usr/local/bin/hyperpipe
COPY --from=build /src/modules/target/wasm32-wasip2/release/*.wasm /opt/hyperpipe/modules/

ENV HYPERPIPE_MODULE_DIR=/opt/hyperpipe/modules \
    HYPERPIPE_HEALTH_PORT=9000 \
    RUST_LOG=hyperpipe=info

# /state holds the SQLite checkpoint — mount a volume here in production.
RUN mkdir -p /state /etc/hyperpipe && chown -R hyperpipe:hyperpipe /state /opt/hyperpipe
USER hyperpipe
WORKDIR /work
EXPOSE 9000

# Pipeline YAML is expected at /etc/hyperpipe/pipeline.yaml (mount a ConfigMap).
ENTRYPOINT ["hyperpipe"]
CMD ["run", "/etc/hyperpipe/pipeline.yaml"]
