//! evm-abi-decoder — raw logs -> `decoded` records (§7). Flagship processor.
//!
//! The pure decode logic lives in [`decode`] and is unit-tested natively; the
//! WASM glue below is compiled only for the component target.

mod decode;

#[cfg(target_arch = "wasm32")]
mod wasm_glue {
    use crate::decode::Decoder;
    use hyperpipe_sdk::serde_json::Value;
    use hyperpipe_sdk::{export_processor, Batch, BatchKind, InitInfo, Processor};

    struct AbiDecoder {
        decoder: Decoder,
    }

    impl Processor for AbiDecoder {
        fn init(config: Value, _ctx: &InitInfo) -> Result<Self, String> {
            Ok(AbiDecoder {
                decoder: Decoder::from_config(&config)?,
            })
        }

        fn process(&mut self, batch: Batch) -> Result<Vec<Batch>, String> {
            let mut out = Vec::with_capacity(batch.records.len());
            for rec in &batch.records {
                if let Some(decoded) = self.decoder.decode_record(batch.chain_id, rec)? {
                    out.push(decoded);
                }
            }
            if out.is_empty() {
                return Ok(vec![]);
            }
            Ok(vec![batch.derive(BatchKind::Decoded, out)])
        }
    }

    export_processor!(AbiDecoder);
}
