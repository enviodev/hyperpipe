# enrich — custom module template

A minimal, out-of-tree HyperPipe processor. Copy this directory to author your own.

## What it does

Adds `usd_estimate` (human-readable USDC amount) and a `whale` flag to decoded
USDC transfers. Pure compute — no network.

## Author flow

1. Implement `Processor` (or `Sink`) from `hyperpipe-sdk` on your type.
2. Call `export_processor!(YourType)` (or `export_sink!`).
3. Build a component:

   ```bash
   cargo build --target wasm32-wasip2 --release
   # -> target/wasm32-wasip2/release/enrich.wasm
   ```

4. Reference it from a pipeline by path:

   ```yaml
   processors:
     - name: enrich
       module: { file: ./path/to/enrich.wasm }
       inputs: [decode]
       config: { whale_usd: 1000000 }
   ```

## Notes

- The SDK owns the envelope codec, init/state plumbing, and automatic
  pass-through of `control` batches — your code only sees decoded `Batch` values.
- This crate has its own `[workspace]` and pulls the SDK by path, so it builds
  standalone. External users would instead depend on the published `hyperpipe-sdk`
  crate and vendor `wit/hyperpipe.wit` (here it lives at `examples/wit/`).
- **Network access is capability-gated.** To call an external API, grant the host
  in the pipeline and use the host import:

  ```yaml
  permissions: { http: ["api.coingecko.com"] }
  ```
  ```rust
  let (status, body) = hp_host::http("GET", "https://api.coingecko.com/...", &[], None)?;
  ```
  The module can only reach hosts the YAML allows — everything else is denied.
