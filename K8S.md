# Running HyperPipe on Kubernetes

HyperPipe ships as a single container: the `hyperpipe` binary plus the built-in
WASM modules. A pipeline is a **long-running, stateful, single-writer** process
(one SQLite checkpoint), so the workload is a **StatefulSet with a PersistentVolume**
for `/state` — not a horizontally-scaled Deployment.

## TL;DR

```bash
# 1. build + push the image
docker build -t ghcr.io/your-org/hyperpipe:0.1 .
docker push ghcr.io/your-org/hyperpipe:0.1

# 2. create the secret (token + ${secret:...} values)
kubectl create secret generic hyperpipe-secrets \
  --from-literal=HYPERSYNC_BEARER_TOKEN=eyJ... \
  --from-literal=HYPERPIPE_SECRET_PG_MAIN_DSN=postgres://user:pw@postgres:5432/hyperpipe

# 3. put your pipeline YAML in the ConfigMap (deploy/k8s/configmap-pipeline.yaml)
#    then apply everything
kubectl apply -k deploy/k8s
```

Manifests live in [`deploy/k8s/`](./deploy/k8s): `statefulset.yaml`, `service.yaml`
(headless), `configmap-pipeline.yaml` (the pipeline), `secret.example.yaml`, and a
`kustomization.yaml`.

## The container

Multi-stage [`Dockerfile`](./Dockerfile):
- **build**: compiles the release binary and the WASM modules (`wasm32-wasip2`).
- **runtime**: `debian:bookworm-slim` + `ca-certificates`, non-root user `10001`.
  - binary at `/usr/local/bin/hyperpipe`
  - modules at `/opt/hyperpipe/modules` (`HYPERPIPE_MODULE_DIR` preset)
  - health server on `:9000` (`HYPERPIPE_HEALTH_PORT` preset)
  - `ENTRYPOINT ["hyperpipe"]`, default `CMD ["run", "/etc/hyperpipe/pipeline.yaml"]`

TLS uses rustls, so there's no OpenSSL dependency at runtime.

## How the pieces map

| Concern | Kubernetes | Detail |
|---|---|---|
| Pipeline definition | **ConfigMap** mounted at `/etc/hyperpipe/pipeline.yaml` | keep the ABI inline (`abi:`) — pods have no project filesystem |
| Secrets / token | **Secret** via `envFrom` | `HYPERSYNC_BEARER_TOKEN`, `HYPERPIPE_SECRET_<NAME>`, `AWS_*` |
| Checkpoint state | **PVC** (`volumeClaimTemplate`) at `/state` | `checkpoint.path: /state/checkpoint.db`; survives restarts |
| Liveness | `httpGet /healthz :9000` | 200 while alive, 503 once draining |
| Readiness | `httpGet /readyz :9000` | 200 once the pipeline is wired and running |
| Graceful stop | SIGTERM → drain | binary catches SIGTERM, flushes sinks + checkpoints; `terminationGracePeriodSeconds: 30` |
| Logs | stdout (`tracing`) | `RUST_LOG=hyperpipe=info`; `kubectl logs` |

## Health & lifecycle

The binary runs a tiny HTTP health server when `HYPERPIPE_HEALTH_PORT` is set:
- `GET /healthz` — liveness; 503 only while shutting down.
- `GET /readyz` — readiness; 503 until the DAG is running.

On **SIGTERM** (pod deletion, rollout) it stops sources, drains channels, flushes
every sink, and writes a final checkpoint — so a redeploy resumes exactly where it
left off (at-least-once; idempotent sinks collapse any duplicates).

## Scaling model (important)

A single pipeline is **one writer** — its SQLite checkpoint cannot be shared. So:

- **Do not** raise `replicas` above 1 on one StatefulSet. Two pods on the same PVC
  would corrupt the checkpoint (and `ReadWriteOnce` blocks it anyway).
- **Scale by running more pipelines**: one StatefulSet per pipeline/shard (e.g. one
  per chain, or partitioned by contract). Each gets its own ConfigMap + PVC. Copy the
  manifests and change the name/labels, or use Kustomize overlays.
- Within a pipeline, throughput scales with `runtime.resource_size` (`s`/`m`/`l` →
  more WASM instances per stage, bigger channels). Set container `resources` to match:
  `l` wants ~2 CPU / 1Gi.

For shared/HA checkpoint state (multiple readers, failover), a Postgres checkpoint
backend (`checkpoint.store: postgres`) is reserved in the schema but not implemented
yet; until then the PVC is the single point of state.

## Storage

- `volumeClaimTemplate` requests `5Gi` `ReadWriteOnce`. The checkpoint DB is small
  (cursors + module KV); size for WAL headroom and your retention, not data volume —
  pipeline data goes to the sinks, not the PVC.
- Use a durable StorageClass (not `emptyDir`) so checkpoints survive rescheduling.

## Multiple pipelines (example)

```bash
# render a second pipeline as its own StatefulSet
cp -r deploy/k8s deploy/k8s-base
kubectl kustomize deploy/k8s | sed 's/hyperpipe/hyperpipe-base/g' | kubectl apply -f -
```

Or add a Kustomize overlay per pipeline that patches `metadata.name`, the ConfigMap
contents, and the Secret ref.

## Verifying a rollout

```bash
kubectl get statefulset hyperpipe
kubectl logs -f statefulset/hyperpipe           # look for "running (...)" then status lines
kubectl exec -it hyperpipe-0 -- wget -qO- localhost:9000/readyz   # -> ready
```

## Notes & gotchas

- Keep the ABI **inline** in the ConfigMap (`abi:` under `abis[]`), not `file:` —
  the module sandbox has no filesystem, and the engine only substitutes `file:` when
  the file exists in the project.
- Put `${secret:...}` on its own YAML line (block style), never inside a flow map
  `{ ... }` (the `{` breaks flow-mapping parse).
- A crash-looping pod usually means a bad DSN/token or an unreachable sink — check
  `kubectl logs`; validation errors name the exact YAML path.
