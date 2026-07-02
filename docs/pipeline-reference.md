# Pipeline YAML reference

The complete schema. Unknown keys are **errors** (typo protection), so this page is exhaustive.
Design rationale lives in [ARCHITECTURE.md](../ARCHITECTURE.md) §3–§4.

## Top level

```yaml
name: my-pipeline        # required — pipeline identifier
version: 1               # optional, default 1
runtime: { ... }         # optional
sources: [ ... ]         # required, >= 1
processors: [ ... ]      # optional
sinks: [ ... ]           # required, >= 1
connections: { ... }     # optional — named IO connections the host opens
```

## `runtime`

```yaml
runtime:
  resource_size: s              # s (default) | m | l — see table below
  checkpoint:
    store: sqlite               # sqlite (default) | postgres (reserved, phase 1)
    path: ./state/hyperpipe.db  # sqlite file path (default shown)
```

| `resource_size` | WASM instances/stage | channel capacity | default `max_records` | WASM mem/instance | call deadline |
|---|---|---|---|---|---|
| `s` | 1 | 4 | 1,000 | 128 MB | 5 s |
| `m` | 2 | 8 | 5,000 | 256 MB | 5 s |
| `l` | 4 | 16 | 10,000 | 512 MB | 10 s |

## `sources[]`

Each source is one independent HyperSync stream (one chain).

```yaml
sources:
  - name: eth_usdc            # unique node name; referenced by `inputs`
    type: hypersync           # default; only supported type
    chain: ethereum           # named chain from the registry…
    # chain_id: 1             # …or explicit id + url (overrides)
    # url: https://eth.hypersync.xyz
    mode: live                # live (default) | backfill | both
    from_block: 19000000      # optional; live default = head, backfill default = 0
    to_block: 19100000        # REQUIRED for backfill, FORBIDDEN for live
    confirmations: 10         # blocks to lag behind head (reorg window; default 10)
    batch:
      max_records: 5000       # max records per batch (default from resource_size)
      max_interval_ms: 500    # reserved (latency bound; not yet enforced)
    query:                    # mirrors HyperSync query semantics
      logs:
        - address: ["0x…"]              # OR-set of contract addresses
          topics: [["0x…"], ["0x…"]]    # outer = topic position, inner = OR-set
      field_selection:
        log: [address, topic0, topic1, topic2, topic3, data, block_number, log_index, transaction_hash]
        block: [number, timestamp, hash]      # if present, block data is joined into each log
        transaction: []                       # reserved
```

- **Chain registry** (built-in names): `ethereum`, `base`, `arbitrum`, `optimism`, `polygon`.
  Any other chain: give `chain_id:` + `url:` (any HyperSync endpoint, or a mock).
- **Modes** — `live`: follow the head forever. `backfill`: fixed range, emits EOF at
  `to_block` and (when all sources are done) the process exits 0 — job mode. `both`:
  backfill then keep following.
- **Confirmations** — the engine never emits blocks newer than `head − confirmations`.
- **Block join** — with `field_selection.block`, each log record gets `block_timestamp` and
  `block_hash` injected; downstream modules never join.

## `processors[]`

```yaml
processors:
  - name: decode
    module: builtin/evm-abi-decoder@1    # builtin/<name>@<major>…
    # module: { file: ./my/custom.wasm } # …or a path to your own component
    inputs: [eth_usdc, base_usdc]        # >= 1 source or processor names (fan-in merge)
    permissions:
      http: ["api.example.com"]          # hostname allowlist for the host `http` import
    config: { ... }                      # module-specific, passed to init() as JSON
```

Per-module `config` schemas: [modules.md](./modules.md).

## `sinks[]`

```yaml
sinks:
  - name: pg
    module: builtin/postgres@1
    inputs: [decode]                     # what this sink consumes
    connections: [pg_main]               # connection grants (capability, see below)
    permissions:
      http: ["hooks.slack.com"]          # only needed by http-using sinks (webhook)
    config: { ... }
```

## `connections`

Named IO resources. The **host** opens them at startup; modules reference them by name and
can only use ones granted via the node's `connections:` list. Credentials never enter WASM.

```yaml
connections:
  pg_main:
    type: postgres
    dsn: ${secret:PG_MAIN_DSN}     # keep secret refs in block style, never inside { }
    pool: { max: 8 }               # connection pool size (default 8)

  lake:
    type: s3
    bucket: my-data-lake           # required unless local_path is set
    region: us-east-1
    endpoint: https://…            # optional — R2 / MinIO override
    prefix: usdc/                  # key prefix prepended to every object
    local_path: ./state/lake       # dev/test: write to a local dir, no credentials

  # type: kafka                    # parses, wired in phase 1
```

S3 credentials come from the host environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`, `AWS_REGION`).

## Secrets

Any string value may contain `${secret:NAME}` — resolved at startup from the env var
`HYPERPIPE_SECRET_NAME`. Missing secrets fail validation, all reported at once.

**Gotcha:** put `${secret:…}` on its own line (block style). Inside a flow map
(`{ dsn: ${secret:X} }`) the `{` breaks YAML parsing.

## The DAG

- Edges are declared by each node's `inputs:`. Sources have no inputs; sinks have no consumers.
- **Fan-out** is free: any node's output can feed many consumers.
- **Fan-in** (multiple `inputs`) merges streams: strict block order *within* one source, no
  ordering guarantee *across* sources.
- Records carry `chain_id`, so multi-chain fan-in stays distinguishable.

## Validation rules (all checked before any data flows)

1. DAG well-formed: no cycles, all `inputs` resolve to a source/processor, ≥ 1 source, ≥ 1 sink.
2. No orphans: every processor's output and every source is consumed by something.
3. Every `${secret:NAME}` resolvable.
4. Module refs valid (`builtin/<name>@<major>` or existing file); modules loadable; `init(config)` dry-run passes (on `run`).
5. `backfill` requires `to_block`; `live` forbids it; `to_block > from_block`.
6. A postgres sink's `config.connection` must appear in its `connections:` grants.
7. Unknown YAML keys anywhere are errors.
8. Node names unique across sources + processors + sinks.

Error messages name the failing node/key (e.g. `sources.eth_usdc: mode backfill requires to_block`).
