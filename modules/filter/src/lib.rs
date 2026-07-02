//! filter — drops records that fail a predicate (§7). Pure logic in [`filter`];
//! WASM glue is compiled only for the component target.

mod filter;

#[cfg(target_arch = "wasm32")]
mod wasm_glue {
    use crate::filter::Filter;
    use hyperpipe_sdk::serde_json::Value;
    use hyperpipe_sdk::{export_processor, Batch, InitInfo, Processor};

    struct FilterProc {
        filter: Filter,
    }

    impl Processor for FilterProc {
        fn init(config: Value, _ctx: &InitInfo) -> Result<Self, String> {
            Ok(FilterProc {
                filter: Filter::from_config(&config)?,
            })
        }

        fn process(&mut self, batch: Batch) -> Result<Vec<Batch>, String> {
            let kept: Vec<Value> = batch
                .records
                .iter()
                .filter(|r| self.filter.passes(r))
                .cloned()
                .collect();
            if kept.is_empty() {
                return Ok(vec![]);
            }
            Ok(vec![batch.derive(batch.kind, kept)])
        }
    }

    export_processor!(FilterProc);
}
