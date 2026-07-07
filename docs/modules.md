# Module reference

Every processor and sink is a WASM component. Built-ins ship with the repo (compiled from
`modules/`), custom modules are loaded from a `.wasm` path. Both use the exact same interface —
the WIT contract in [`wit/hyperpipe.wit`](../wit/hyperpipe.wit).

## Built-in processors

### `builtin/evm-abi-decoder@1` — raw logs → decoded events

```yaml
config:
  abis:
    - file: ./abis/erc20.json     # ABI JSON file (engine inlines it at load time)…
      # abi: [ …inline ABI… ]     # …or inline (required in K8s ConfigMaps — no filesystem)
      events: [Transfer]          # optional filter: decode only these events
  on_undecodable: drop            # drop (default) | passthrough | error
```

- Builds a `topic0 → event` table across all ABIs at init.
- Decodes indexed params from topics and the rest from `data`.
- Output records (`kind: decoded`): `chain_id`, `block_number`, `block_hash?`,
  `block_timestamp?`, `transaction_hash`, `log_index`, `address`, `event`, `signature`,
  `params{…}`.
- **All integers ≥ 2^53 are decimal strings** (uint256/int256 survive JSON).
- Unnamed ABI params become `arg0`, `arg1`, ….
- `on_undecodable`: `drop` skips unknown/broken logs, `passthrough` forwards them raw,
  `error` fails the batch (engine retries, then pauses the branch).

### `builtin/filter@1` — predicate filter

```yaml
config:
  all:                                  # every predicate must hold (AND)
    - { field: params.value, op: gte, value: "1000000000000" }
  any:                                  # optional: at least one must hold (OR)
    - { field: event, op: eq, value: Transfer }
```

- `field` is a dotted path into the record (`params.value`).
- `op`: `eq` `ne` `gt` `gte` `lt` `lte` `in` (`in` takes an array `value`).
- Comparisons are **decimal-string aware and sign-aware**: `"5000000000000" >= "1000000000000"`
  compares numerically (no float loss), negatives (int256) order correctly.
- A missing field **fails closed** (record dropped).
- Batches where everything is filtered out still propagate (empty), so downstream cursors
  keep advancing.

## Built-in sinks

### `builtin/postgres@1`

```yaml
connections: [pg_main]                  # grant — must include config.connection
config:
  connection: pg_main
  table: usdc_transfers
  mode: upsert                          # insert | upsert
  unique_key: [chain_id, block_number, log_index]   # required for upsert
  create_table: true                    # opt-in auto-DDL from the first batch
  rollback: true                        # reorg handling: delete rows past the fork
  # rollback:                           # …or with explicit columns:
  #   block_number_column: block_number # default
  #   chain_id_column: chain_id         # default; null = no chain filter
  column_map:                           # dotted record path -> column name
    params.from: from_address
    params.value: amount
```

- One batch = **one transaction** (`sql-batch` host import).
- Columns = all top-level scalar fields, plus `column_map` entries (nested paths, renames).
  Mixed-shape batches are split into one statement per column signature; auto-DDL uses the
  union of columns across the batch.
- Auto-DDL infers types (`bigint`, `numeric` for decimal strings, `boolean`, `double
  precision`, `text`) and logs the generated `CREATE TABLE` loudly. Decimal-string params are
  bound with a `::numeric` cast.
- Upsert = `INSERT … ON CONFLICT (unique_key) DO UPDATE` → replays are idempotent.
- **Reorg rollback** (`rollback: true`): on a `rollback` control record the sink runs
  `DELETE FROM table WHERE block_number > $fork AND chain_id = $chain` before the source
  replays the corrected blocks. Without it, rollbacks are logged and ignored (stale rows
  remain until an upsert replay overwrites them — rows that vanished in the reorg linger).

### `builtin/webhook@1`

```yaml
permissions: { http: ["hooks.slack.com"] }   # REQUIRED — deny-by-default allowlist
config:
  url: ${secret:WEBHOOK_URL}
  max_batch: 500                             # records per POST (re-chunks larger batches)
  forward_rollbacks: true                    # default: POST reorg rollback records too
```

- POSTs records as NDJSON (`application/x-ndjson`); non-2xx = error → engine retries ×3 with
  backoff, then pauses the branch.
- Delivery is at-least-once — consumers dedupe on record identity
  (`chain_id`,`block_number`,`log_index`) or the batch id.
- On a reorg the sink POSTs the control record itself
  (`{"control":"rollback","chain_id":…,"invalidate_after_block":…}`) so the consumer can
  invalidate what it already received; disable with `forward_rollbacks: false`.

### `builtin/s3@1` — buffered Parquet/NDJSON objects

```yaml
connections: [lake]
config:
  connection: lake
  format: parquet                # parquet (default) | ndjson
  flush_rows: 100000             # flush a chain's buffer when it reaches N rows
```

- Buffers rows **per chain** and flushes an object when: the buffer hits `flush_rows`, a
  checkpoint barrier fires (default every ~2 s), or the pipeline shuts down.
- Deterministic keys: `{connection.prefix}{chain_id}/{first_block}-{last_block}-{rows}.{ext}` —
  a replay overwrites the same object (idempotent).
- Parquet schema: every column `Utf8` nullable — the union of keys across all buffered
  records (sorted). uint256 values stay decimal strings.
- Cursor safety: acks are only persisted **after** buffers flush (§8.2 rule 5), so a crash
  between buffering and flushing replays those blocks.
- Reorg rollback: buffered (not yet uploaded) records past the fork are purged. Objects
  already in S3 are not rewritten — an append-only limitation; the replayed range
  overwrites an object only when it produces the identical key. If you need strict reorg
  correctness in the lake, raise the source's `confirmations` above the chain's reorg depth.

### `builtin/stdout@1` / `builtin/blackhole@1`

No config. NDJSON to stdout (debug/demo) and drop-everything (throughput baseline).

---

## Authoring custom modules

Template: [`examples/modules/enrich/`](../examples/modules/enrich/) — copy it, it builds
standalone.

```rust
use hyperpipe_sdk::{export_processor, Batch, InitInfo, Processor, serde_json::Value};

struct MyProc { threshold: f64 }

impl Processor for MyProc {
    fn init(config: Value, _ctx: &InitInfo) -> Result<Self, String> {
        Ok(MyProc { threshold: config["threshold"].as_f64().unwrap_or(0.0) })
    }
    fn process(&mut self, batch: Batch) -> Result<Vec<Batch>, String> {
        // 0 batches = drop, 1 = map, N = split. Control batches never reach you.
        Ok(vec![batch])
    }
}
export_processor!(MyProc);        // or: impl Sink + export_sink!(MySink)

// Sinks may additionally override `on_control` to react to reorg rollbacks:
//   fn on_control(&mut self, ctrl: ControlRecord) -> Result<(), String> {
//       if let ControlRecord::Rollback { chain_id, invalidate_after_block } = ctrl {
//           /* invalidate everything past invalidate_after_block */
//       }
//       Ok(())
//   }
// Default: rollbacks are ignored (fine for stateless/append-only sinks).
```

```bash
cargo build --target wasm32-wasip2 --release
```

```yaml
processors:
  - name: mine
    module: { file: ./path/to/my_module.wasm }
    inputs: [decode]
    config: { threshold: 5 }
```

What the SDK gives you:

- **Envelope handling** — you receive decoded `Batch` values (`chain_id`, `block_range`,
  `records: Vec<serde_json::Value>`, …); encoding and control-batch passthrough are automatic.
- **Identity helpers** — produce outputs with `batch.derive(kind, records)` to preserve
  `batch_id`/`ack_block` (required for downstream dedupe and checkpoint correctness).
- **Host imports** via `hp_host::…` (inside the macro scope): `log_info/log_warn`,
  `metric_add`, `kv_get`/`kv_set` (durable, committed atomically with the checkpoint —
  replay-consistent state), `http(method, url, headers, body)`, `sql_batch(conn, stmts)`,
  `blob_put(conn, key, bytes)`.

The sandbox contract (§6.3): no filesystem, no sockets, no env. `http` only reaches hosts in
your node's `permissions.http`; `sql_batch`/`blob_put` only reach connections in its
`connections:` grants. Returning `Err` from `process`/`write` → engine retries the batch 3×,
then pauses that branch (nothing is silently dropped). A hung module is killed at the epoch
deadline (5–10 s); memory is capped per `resource_size`.

Rust is supported today; TypeScript (via jco) and Go (TinyGo) target the same WIT contract
and are on the phase-1 roadmap.
