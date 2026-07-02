//! Example custom processor (out-of-tree). Adds a human-readable `usd_estimate`
//! and a `whale` flag to decoded USDC transfers.
//!
//! This is the template for authoring your own module: implement `Processor`
//! (or `Sink`) on the SDK, call `export_processor!`, build for wasm32-wasip2,
//! and reference the `.wasm` via `module: { file: ... }` in your pipeline.
//!
//! Build:  cargo build --target wasm32-wasip2 --release
//!
//! Network enrichment (e.g. a live price API) is possible too: grant the host
//! in the pipeline `permissions.http` and call `hp_host::http(...)` — the module
//! can only reach hosts the YAML allows.

#[cfg(target_arch = "wasm32")]
mod wasm_glue {
    use hyperpipe_sdk::serde_json::{json, Value};
    use hyperpipe_sdk::{export_processor, Batch, InitInfo, Processor};

    struct Enrich {
        whale_usd: f64,
    }

    impl Processor for Enrich {
        fn init(config: Value, _ctx: &InitInfo) -> Result<Self, String> {
            let whale_usd = config.get("whale_usd").and_then(|v| v.as_f64()).unwrap_or(1_000_000.0);
            Ok(Enrich { whale_usd })
        }

        fn process(&mut self, batch: Batch) -> Result<Vec<Batch>, String> {
            let mut out = Vec::with_capacity(batch.records.len());
            for rec in &batch.records {
                let mut rec = rec.clone();
                // USDC has 6 decimals; the decoded value is a decimal string.
                let usd = rec
                    .get("params")
                    .and_then(|p| p.get("value"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .map(|raw| raw / 1_000_000.0);
                if let (Some(usd), Some(obj)) = (usd, rec.as_object_mut()) {
                    obj.insert("usd_estimate".into(), json!(format!("{usd:.2}")));
                    obj.insert("whale".into(), json!(usd >= self.whale_usd));
                }
                out.push(rec);
            }
            Ok(vec![batch.derive(batch.kind, out)])
        }
    }

    export_processor!(Enrich);
}
