//! blackhole sink — drops every batch. Throughput baseline (§7).
//!
//! Uses raw `wit-bindgen` (no SDK) as a reference for the component ABI. The
//! glue is component-only so `cargo test` can build the crate natively.

#[cfg(target_arch = "wasm32")]
mod wasm_glue {
    wit_bindgen::generate!({
        world: "sink",
        path: "../../wit",
    });

    use envio::hyperpipe::types::{Batch, InitCtx};
    use exports::envio::hyperpipe::sink_impl::Guest;

    struct Component;

    impl Guest for Component {
        fn init(_ctx: InitCtx) -> Result<(), String> {
            Ok(())
        }
        fn write(_input: Batch) -> Result<(), String> {
            Ok(())
        }
        fn flush() -> Result<(), String> {
            Ok(())
        }
    }

    export!(Component);
}
