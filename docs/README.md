# HyperPipe documentation

WASM data pipelines on Envio HyperSync: one YAML file declares chain sources, a DAG of WASM
processors (ABI decode, filter, custom), and one or more WASM sinks. The engine streams,
sandboxes every module, and recovers with at-least-once delivery.

## Where to start

| You want to… | Read |
|---|---|
| Build and run your first pipeline | [getting-started.md](./getting-started.md) |
| Write or tweak a pipeline YAML | [pipeline-reference.md](./pipeline-reference.md) |
| Configure a specific built-in module (decoder, postgres, s3, …) | [modules.md](./modules.md) |
| Write your own WASM module | [modules.md § Authoring custom modules](./modules.md#authoring-custom-modules) |
| Understand the code (crates, data flow, invariants) | [codebase.md](./codebase.md) |
| Run in production: bare binary, Docker, Kubernetes | [deployment.md](./deployment.md) |
| Operate it: env vars, checkpoints, health, troubleshooting | [operations.md](./operations.md) |

## Design documents (repo root)

- [`ARCHITECTURE.md`](../ARCHITECTURE.md) — the full system design and rationale. Section refs
  (§) throughout the code and docs point here.
- [`ACTION_PLAN.md`](../ACTION_PLAN.md) — milestone plan + current status.
- [`DEMO.md`](../DEMO.md) — scripted end-to-end demo (works offline, no API token).
- [`K8S.md`](../K8S.md) — Kubernetes deployment (StatefulSet, probes, scaling model).

## The 60-second picture

```
pipeline.yaml
     │  hyperpipe run pipeline.yaml
     ▼
┌────────────┐   ┌───────────────────────────────┐   ┌──────────────────────┐
│ HyperSync  │──▶│ WASM processors (wasmtime)    │──▶│ WASM sinks           │
│ sources    │   │ evm-abi-decoder → filter → …  │   │ postgres / webhook / │
│ (N chains) │   │ (sandboxed, compute-only)     │   │ s3-parquet / stdout  │
└────────────┘   └───────────────────────────────┘   └──────────────────────┘
      ▲                                                    │ acks
      └────────────── SQLite checkpoint (cursors) ◀────────┘
                      crash → resume, at-least-once
```

- Modules do **compute only**; all IO goes through capability-gated host imports
  (allowlisted HTTP, granted DB/object-store connections). Secrets never enter WASM.
- Kill the process at any point; on restart each source resumes from the minimum
  durable cursor of its sinks. Idempotent sinks collapse the replayed duplicates.
