//! stdout sink — prints each record as NDJSON. Debugging + demo (§7).

#[cfg(target_arch = "wasm32")]
mod wasm_glue {
    use hyperpipe_sdk::serde_json::Value;
    use hyperpipe_sdk::{export_sink, Batch, InitInfo, Sink};

    struct StdoutSink;

    impl Sink for StdoutSink {
        fn init(_config: Value, _ctx: &InitInfo) -> Result<Self, String> {
            Ok(StdoutSink)
        }

        fn write(&mut self, batch: Batch) -> Result<(), String> {
            use std::io::Write;
            let stdout = std::io::stdout();
            let mut out = stdout.lock();
            for record in &batch.records {
                let line =
                    hyperpipe_sdk::serde_json::to_string(record).map_err(|e| e.to_string())?;
                writeln!(out, "{line}").map_err(|e| e.to_string())?;
            }
            Ok(())
        }

        fn flush(&mut self) -> Result<(), String> {
            use std::io::Write;
            std::io::stdout().flush().map_err(|e| e.to_string())
        }
    }

    export_sink!(StdoutSink);
}
