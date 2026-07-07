//! webhook sink — POSTs each batch as NDJSON via the host `http` import (§7).
//! At-least-once: the consumer dedupes on `batch_id`; the engine retries on Err.

mod body;

#[cfg(target_arch = "wasm32")]
mod wasm_glue {
    use crate::body::ndjson;
    use hyperpipe_sdk::serde_json::{self, Value};
    use hyperpipe_sdk::{export_sink, Batch, ControlRecord, InitInfo, Sink};

    struct Webhook {
        url: String,
        max_batch: usize,
        headers: Vec<(String, String)>,
        forward_rollbacks: bool,
    }

    impl Sink for Webhook {
        fn init(config: Value, _ctx: &InitInfo) -> Result<Self, String> {
            let url = config
                .get("url")
                .and_then(|v| v.as_str())
                .ok_or("webhook: config.url required")?
                .to_string();
            let max_batch = config.get("max_batch").and_then(|v| v.as_u64()).unwrap_or(500) as usize;
            let forward_rollbacks = config
                .get("forward_rollbacks")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            Ok(Webhook {
                url,
                max_batch: max_batch.max(1),
                headers: vec![("content-type".into(), "application/x-ndjson".into())],
                forward_rollbacks,
            })
        }

        fn write(&mut self, batch: Batch) -> Result<(), String> {
            for chunk in batch.records.chunks(self.max_batch) {
                let payload = ndjson(chunk)?;
                let (status, _resp) =
                    hp_host::http("POST", &self.url, &self.headers, Some(payload))?;
                if !(200..300).contains(&status) {
                    return Err(format!("webhook POST {} -> HTTP {status}", self.url));
                }
                hp_host::metric_add("webhook.posted", chunk.len() as u64);
            }
            Ok(())
        }

        /// Reorg rollback: forward the control record itself (one NDJSON line,
        /// `{"control":"rollback","chain_id":..,"invalidate_after_block":..}`)
        /// so the consumer can invalidate what it already received. Opt out
        /// with `forward_rollbacks: false`.
        fn on_control(&mut self, ctrl: ControlRecord) -> Result<(), String> {
            let ControlRecord::Rollback { .. } = ctrl else {
                return Ok(());
            };
            if !self.forward_rollbacks {
                return Ok(());
            }
            let mut payload = serde_json::to_vec(&ctrl).map_err(|e| e.to_string())?;
            payload.push(b'\n');
            let (status, _resp) = hp_host::http("POST", &self.url, &self.headers, Some(payload))?;
            if !(200..300).contains(&status) {
                return Err(format!("webhook rollback POST {} -> HTTP {status}", self.url));
            }
            Ok(())
        }
    }

    export_sink!(Webhook);
}
