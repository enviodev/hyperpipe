# Deployment

Three ways to run HyperPipe: bare binary, Docker, Kubernetes. Same binary, same YAML,
different packaging.

## 1. Bare binary on a machine

### The launcher (recommended)

```bash
cp examples/pipeline.env.example pipeline.env    # token + secrets
./scripts/run.sh <pipeline.yaml> pipeline.env    # [-- extra args, e.g. -- --debug-stdout]
```

`scripts/run.sh` loads the env file, builds the binary and WASM modules if missing, validates,
then `exec`s `hyperpipe run` in the foreground (ctrl-c drains and checkpoints). Knobs:
`HYPERPIPE_PROFILE=release|debug`, `HYPERPIPE_MODULE_PROFILE`, `HYPERPIPE_REBUILD=1`.
Details: [RUN.md](../RUN.md).

### Manual

```bash
cargo build --release
(cd modules && cargo build --release --target wasm32-wasip2)

export HYPERPIPE_MODULE_DIR="$PWD/modules/target/wasm32-wasip2/release"
export HYPERSYNC_BEARER_TOKEN=...
export HYPERPIPE_SECRET_PG_MAIN_DSN=postgres://...

./target/release/hyperpipe validate pipeline.yaml
./target/release/hyperpipe run pipeline.yaml
```

The binary needs exactly three things at runtime: the pipeline YAML, the module directory
(`HYPERPIPE_MODULE_DIR`), and env vars for token/secrets. Full env reference:
[operations.md](./operations.md).

### As a service (systemd sketch)

```ini
[Unit]
Description=HyperPipe pipeline
After=network-online.target

[Service]
User=hyperpipe
EnvironmentFile=/etc/hyperpipe/pipeline.env
Environment=HYPERPIPE_MODULE_DIR=/opt/hyperpipe/modules
Environment=HYPERPIPE_HEALTH_PORT=9000
ExecStart=/usr/local/bin/hyperpipe run /etc/hyperpipe/pipeline.yaml
Restart=on-failure
# SIGTERM triggers a clean drain + final checkpoint; give it time.
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Keep the checkpoint path (`runtime.checkpoint.path`) on persistent disk — that file **is** the
resume state.

## 2. Docker

### Build the image

```bash
docker build -t hyperpipe:latest .
```

Multi-stage [`Dockerfile`](../Dockerfile): the build stage compiles the release binary + WASM
modules; the runtime stage is `debian:bookworm-slim` + `ca-certificates`, non-root (uid 10001),
with modules baked in at `/opt/hyperpipe/modules` and the health server preset on `:9000`.
TLS is rustls — no OpenSSL in the image.

### Run a container

The container expects: pipeline at `/etc/hyperpipe/pipeline.yaml` (mount it), env vars for
secrets, and a volume at `/state` for the checkpoint.

```bash
docker run -d --name my-pipeline \
  -v "$PWD/pipeline.yaml:/etc/hyperpipe/pipeline.yaml:ro" \
  -v hyperpipe-state:/state \
  --env-file pipeline.env \
  -p 9000:9000 \
  hyperpipe:latest
```

Notes:

- **Inline your ABIs** in the YAML (`abi:` instead of `file:`) — the container has no project
  filesystem. See [`deploy/pipeline.example.yaml`](../deploy/pipeline.example.yaml).
- Point the checkpoint at the volume: `runtime.checkpoint.path: /state/checkpoint.db`.
- `docker stop` sends SIGTERM → the pipeline drains, flushes sinks, writes a final checkpoint,
  and the next start resumes exactly there. Default 10 s stop timeout is fine; use
  `--stop-timeout 30` for heavy sinks.
- Health: `curl localhost:9000/healthz` / `readyz`.
- Override the default command freely: `docker run … hyperpipe:latest validate /etc/hyperpipe/pipeline.yaml`.

### Docker Compose (pipeline + Postgres)

```bash
cp deploy/pipeline.example.yaml deploy/pipeline.yaml
echo "HYPERSYNC_BEARER_TOKEN=eyJ..." > deploy/hyperpipe.env
docker compose -f deploy/docker-compose.yaml up --build
```

[`deploy/docker-compose.yaml`](../deploy/docker-compose.yaml) wires a Postgres service, injects
its DSN as the `PG_MAIN_DSN` secret, mounts the pipeline read-only, persists `/state` and
`pgdata` in named volumes, and health-checks both containers. Verify:

```bash
docker compose -f deploy/docker-compose.yaml exec postgres \
  psql -U postgres -d hyperpipe -c 'SELECT count(*) FROM usdc_transfers;'
```

## 3. Kubernetes

Full guide: [K8S.md](../K8S.md). Summary: a pipeline is a **single-writer stateful process**
(SQLite checkpoint) → **StatefulSet, replicas: 1, PVC at `/state`**; pipeline YAML in a
ConfigMap; secrets via a Secret + `envFrom`; `/healthz`+`/readyz` probes; SIGTERM drain.
Manifests in [`deploy/k8s/`](../deploy/k8s), apply with `kubectl apply -k deploy/k8s`.

## Scaling model (all deployment styles)

- **One pipeline = one process = one writer.** Never run two instances against the same
  checkpoint file.
- Scale **up** within a pipeline via `runtime.resource_size` (s/m/l).
- Scale **out** by running more pipelines — split by chain or contract set, each with its own
  YAML + checkpoint (+ StatefulSet in K8s).
