//! s3 sink — buffers records and flushes Parquet/NDJSON objects after N rows or
//! on `flush()` (§7.1). Pure logic in [`buffer`]; WASM glue is component-only.

pub mod buffer;

#[cfg(target_arch = "wasm32")]
mod wasm_glue {
    use crate::buffer::{ChainBuffers, S3Config};
    use hyperpipe_sdk::serde_json::Value;
    use hyperpipe_sdk::{export_sink, Batch, ControlRecord, InitInfo, Sink};

    struct S3Sink {
        cfg: S3Config,
        buffers: ChainBuffers,
    }

    impl S3Sink {
        fn put_all(&mut self, objects: Vec<(String, Vec<u8>)>) -> Result<(), String> {
            for (key, bytes) in objects {
                let size = bytes.len();
                hp_host::blob_put(&self.cfg.connection, &key, &bytes)?;
                hp_host::log_info(&format!("s3 flushed {key} ({size} bytes)"));
                hp_host::metric_add("s3.objects", 1);
            }
            Ok(())
        }
    }

    impl Sink for S3Sink {
        fn init(config: Value, _ctx: &InitInfo) -> Result<Self, String> {
            Ok(S3Sink {
                cfg: S3Config::from_json(&config)?,
                buffers: ChainBuffers::default(),
            })
        }

        fn write(&mut self, batch: Batch) -> Result<(), String> {
            self.buffers.push(
                batch.chain_id,
                batch.block_range.from(),
                batch.block_range.to(),
                &batch.records,
            );
            let ready = self.buffers.take_ready(self.cfg.format, self.cfg.flush_rows)?;
            self.put_all(ready)
        }

        // Called by the engine before every checkpoint barrier and on shutdown.
        fn flush(&mut self) -> Result<(), String> {
            let all = self.buffers.take_all(self.cfg.format)?;
            self.put_all(all)
        }

        /// Reorg rollback: purge not-yet-flushed records past the fork so they
        /// never reach an object. Objects already uploaded are immutable —
        /// replayed ranges overwrite them only if the same key is produced
        /// (documented append-only limitation).
        fn on_control(&mut self, ctrl: ControlRecord) -> Result<(), String> {
            if let ControlRecord::Rollback {
                chain_id,
                invalidate_after_block,
            } = ctrl
            {
                self.buffers.rollback(chain_id, invalidate_after_block);
                hp_host::log_warn(&format!(
                    "s3: rollback (chain {chain_id}, blocks > {invalidate_after_block}) — \
                     purged buffered records; already-uploaded objects are not rewritten"
                ));
            }
            Ok(())
        }
    }

    export_sink!(S3Sink);
}
