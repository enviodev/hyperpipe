//! Guest SDK for authoring HyperPipe modules.
//!
//! Implement [`Processor`] or [`Sink`] on your type, then call
//! [`export_processor!`] / [`export_sink!`] to wire it to the WASM component
//! ABI. The macro owns the envelope codec, the init/state plumbing, and
//! automatic pass-through of `control` batches, so module code only deals in
//! decoded [`Batch`] values.
//!
//! ```ignore
//! use hyperpipe_sdk::{Batch, InitInfo, Processor, export_processor, serde_json::Value};
//!
//! struct Passthrough;
//! impl Processor for Passthrough {
//!     fn init(_cfg: Value, _ctx: &InitInfo) -> Result<Self, String> { Ok(Passthrough) }
//!     fn process(&mut self, batch: Batch) -> Result<Vec<Batch>, String> { Ok(vec![batch]) }
//! }
//! export_processor!(Passthrough);
//! ```

pub use hp_encoding;
pub use hp_encoding::{Batch, BatchKind, BlockRange, ControlRecord, DecodedLog, Encoding};
pub use serde_json;

/// Read-only context handed to a module's `init`.
pub struct InitInfo {
    pub module_name: String,
    pub pipeline_name: String,
}

/// A processor: transforms batches. Return zero batches to drop, one to map,
/// many to split. `control` batches never reach here — the SDK passes them
/// through automatically.
pub trait Processor: Sized + Send {
    fn init(config: serde_json::Value, ctx: &InitInfo) -> Result<Self, String>;
    fn process(&mut self, batch: Batch) -> Result<Vec<Batch>, String>;
}

/// A sink: delivers batches to an external system. Returning `Ok` from `write`
/// means the batch is durable enough for the engine to advance the cursor.
pub trait Sink: Sized + Send {
    fn init(config: serde_json::Value, ctx: &InitInfo) -> Result<Self, String>;
    fn write(&mut self, batch: Batch) -> Result<(), String>;
    fn flush(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// Decode wire bytes (with a numeric encoding tag) into a [`Batch`].
pub fn decode_batch(tag: u8, data: &[u8]) -> Result<Batch, String> {
    let enc = Encoding::from_u8(tag).ok_or_else(|| format!("unknown encoding tag {tag}"))?;
    hp_encoding::decode(enc, data).map_err(|e| e.to_string())
}

/// Encode a [`Batch`] to JSON wire bytes.
pub fn encode_batch(b: &Batch) -> Result<Vec<u8>, String> {
    hp_encoding::encode(Encoding::Json, b).map_err(|e| e.to_string())
}

/// Wire a [`Processor`] type to the `processor` world's component exports.
#[macro_export]
macro_rules! export_processor {
    ($ty:ty) => {
        // Generated bindings live in a private module so their `Batch` /
        // `Encoding` / `InitCtx` types do not collide with the SDK's own.
        #[allow(dead_code, clippy::all)]
        mod __hp_bindings {
            wit_bindgen::generate!({
                world: "processor",
                path: "../../wit",
                pub_export_macro: true,
            });
            // Batch / InitCtx / Output are already re-exported at the world root
            // by `use types.{...}`; only Encoding needs lifting.
            pub use self::envio::hyperpipe::types::Encoding;

            // Typed host-import wrappers for module code. Only linked into the
            // component if actually used (wit-bindgen tree-shakes imports).
            pub mod hp_host {
                use super::envio::hyperpipe::host as h;
                #[allow(dead_code)]
                pub fn log_info(msg: &str) {
                    h::log(h::LogLevel::Info, msg);
                }
                #[allow(dead_code)]
                pub fn log_warn(msg: &str) {
                    h::log(h::LogLevel::Warn, msg);
                }
                #[allow(dead_code)]
                pub fn metric_add(name: &str, value: u64) {
                    h::metric_add(name, value);
                }
                #[allow(dead_code)]
                pub fn kv_get(key: &str) -> ::core::option::Option<::std::vec::Vec<u8>> {
                    h::kv_get(key)
                }
                #[allow(dead_code)]
                pub fn kv_set(key: &str, value: &[u8]) {
                    h::kv_set(key, value);
                }
                #[allow(dead_code)]
                pub fn http(
                    method: &str,
                    url: &str,
                    headers: &[(::std::string::String, ::std::string::String)],
                    body: ::core::option::Option<::std::vec::Vec<u8>>,
                ) -> ::core::result::Result<(u16, ::std::vec::Vec<u8>), ::std::string::String> {
                    let req = h::HttpRequest {
                        method: method.to_string(),
                        url: url.to_string(),
                        headers: headers.to_vec(),
                        body,
                    };
                    h::http(&req).map(|r| (r.status, r.body))
                }
                #[allow(dead_code)]
                pub fn sql_batch(
                    conn: &str,
                    statements: &[(::std::string::String, ::std::vec::Vec<u8>)],
                ) -> ::core::result::Result<u64, ::std::string::String> {
                    h::sql_batch(conn, statements)
                }
                #[allow(dead_code)]
                pub fn blob_put(
                    conn: &str,
                    key: &str,
                    data: &[u8],
                ) -> ::core::result::Result<(), ::std::string::String> {
                    h::blob_put(conn, key, data)
                }
            }
        }

        #[allow(unused_imports)]
        use __hp_bindings::hp_host;

        struct __HpComponent;
        static __HP_STATE: ::std::sync::Mutex<::core::option::Option<$ty>> =
            ::std::sync::Mutex::new(::core::option::Option::None);

        fn __hp_enc_tag(e: &__hp_bindings::Encoding) -> u8 {
            match e {
                __hp_bindings::Encoding::Json => 0,
                __hp_bindings::Encoding::Cbor => 1,
                __hp_bindings::Encoding::ArrowIpc => 2,
            }
        }

        impl __hp_bindings::Guest for __HpComponent {
            fn init(
                ctx: __hp_bindings::InitCtx,
            ) -> ::core::result::Result<(), ::std::string::String> {
                let config = if ctx.config.is_empty() {
                    $crate::serde_json::Value::Null
                } else {
                    $crate::serde_json::from_slice(&ctx.config)
                        .map_err(|e| ::std::format!("config json: {e}"))?
                };
                let info = $crate::InitInfo {
                    module_name: ctx.module_name,
                    pipeline_name: ctx.pipeline_name,
                };
                let inst = <$ty as $crate::Processor>::init(config, &info)?;
                *__HP_STATE.lock().unwrap() = ::core::option::Option::Some(inst);
                ::core::result::Result::Ok(())
            }

            fn process(
                input: __hp_bindings::Batch,
            ) -> ::core::result::Result<__hp_bindings::Output, ::std::string::String> {
                let tag = __hp_enc_tag(&input.encoding);
                let batch = $crate::decode_batch(tag, &input.data)?;
                if batch.is_control() {
                    return ::core::result::Result::Ok(__hp_bindings::Output::Batches(
                        ::std::vec![input],
                    ));
                }
                let mut guard = __HP_STATE.lock().unwrap();
                let inst = guard
                    .as_mut()
                    .ok_or_else(|| ::std::string::String::from("process called before init"))?;
                let outs = <$ty as $crate::Processor>::process(inst, batch)?;
                let mut wire = ::std::vec::Vec::with_capacity(outs.len());
                for b in &outs {
                    wire.push(__hp_bindings::Batch {
                        encoding: __hp_bindings::Encoding::Json,
                        data: $crate::encode_batch(b)?,
                    });
                }
                ::core::result::Result::Ok(__hp_bindings::Output::Batches(wire))
            }
        }

        __hp_bindings::export!(__HpComponent with_types_in __hp_bindings);
    };
}

/// Wire a [`Sink`] type to the `sink` world's component exports.
#[macro_export]
macro_rules! export_sink {
    ($ty:ty) => {
        #[allow(dead_code, clippy::all)]
        mod __hp_bindings {
            wit_bindgen::generate!({
                world: "sink",
                path: "../../wit",
                pub_export_macro: true,
            });
            pub use self::envio::hyperpipe::types::Encoding;

            // Typed host-import wrappers for module code. Only linked into the
            // component if actually used (wit-bindgen tree-shakes imports).
            pub mod hp_host {
                use super::envio::hyperpipe::host as h;
                #[allow(dead_code)]
                pub fn log_info(msg: &str) {
                    h::log(h::LogLevel::Info, msg);
                }
                #[allow(dead_code)]
                pub fn log_warn(msg: &str) {
                    h::log(h::LogLevel::Warn, msg);
                }
                #[allow(dead_code)]
                pub fn metric_add(name: &str, value: u64) {
                    h::metric_add(name, value);
                }
                #[allow(dead_code)]
                pub fn kv_get(key: &str) -> ::core::option::Option<::std::vec::Vec<u8>> {
                    h::kv_get(key)
                }
                #[allow(dead_code)]
                pub fn kv_set(key: &str, value: &[u8]) {
                    h::kv_set(key, value);
                }
                #[allow(dead_code)]
                pub fn http(
                    method: &str,
                    url: &str,
                    headers: &[(::std::string::String, ::std::string::String)],
                    body: ::core::option::Option<::std::vec::Vec<u8>>,
                ) -> ::core::result::Result<(u16, ::std::vec::Vec<u8>), ::std::string::String> {
                    let req = h::HttpRequest {
                        method: method.to_string(),
                        url: url.to_string(),
                        headers: headers.to_vec(),
                        body,
                    };
                    h::http(&req).map(|r| (r.status, r.body))
                }
                #[allow(dead_code)]
                pub fn sql_batch(
                    conn: &str,
                    statements: &[(::std::string::String, ::std::vec::Vec<u8>)],
                ) -> ::core::result::Result<u64, ::std::string::String> {
                    h::sql_batch(conn, statements)
                }
                #[allow(dead_code)]
                pub fn blob_put(
                    conn: &str,
                    key: &str,
                    data: &[u8],
                ) -> ::core::result::Result<(), ::std::string::String> {
                    h::blob_put(conn, key, data)
                }
            }
        }

        #[allow(unused_imports)]
        use __hp_bindings::hp_host;

        struct __HpComponent;
        static __HP_STATE: ::std::sync::Mutex<::core::option::Option<$ty>> =
            ::std::sync::Mutex::new(::core::option::Option::None);

        fn __hp_enc_tag(e: &__hp_bindings::Encoding) -> u8 {
            match e {
                __hp_bindings::Encoding::Json => 0,
                __hp_bindings::Encoding::Cbor => 1,
                __hp_bindings::Encoding::ArrowIpc => 2,
            }
        }

        impl __hp_bindings::Guest for __HpComponent {
            fn init(
                ctx: __hp_bindings::InitCtx,
            ) -> ::core::result::Result<(), ::std::string::String> {
                let config = if ctx.config.is_empty() {
                    $crate::serde_json::Value::Null
                } else {
                    $crate::serde_json::from_slice(&ctx.config)
                        .map_err(|e| ::std::format!("config json: {e}"))?
                };
                let info = $crate::InitInfo {
                    module_name: ctx.module_name,
                    pipeline_name: ctx.pipeline_name,
                };
                let inst = <$ty as $crate::Sink>::init(config, &info)?;
                *__HP_STATE.lock().unwrap() = ::core::option::Option::Some(inst);
                ::core::result::Result::Ok(())
            }

            fn write(
                input: __hp_bindings::Batch,
            ) -> ::core::result::Result<(), ::std::string::String> {
                let tag = __hp_enc_tag(&input.encoding);
                let batch = $crate::decode_batch(tag, &input.data)?;
                let mut guard = __HP_STATE.lock().unwrap();
                let inst = guard
                    .as_mut()
                    .ok_or_else(|| ::std::string::String::from("write called before init"))?;
                // Sinks handle control batches themselves if they care; default
                // behavior is to ignore them (they carry no data records).
                if batch.is_control() {
                    return ::core::result::Result::Ok(());
                }
                <$ty as $crate::Sink>::write(inst, batch)
            }

            fn flush() -> ::core::result::Result<(), ::std::string::String> {
                let mut guard = __HP_STATE.lock().unwrap();
                match guard.as_mut() {
                    ::core::option::Option::Some(inst) => <$ty as $crate::Sink>::flush(inst),
                    ::core::option::Option::None => ::core::result::Result::Ok(()),
                }
            }
        }

        __hp_bindings::export!(__HpComponent with_types_in __hp_bindings);
    };
}
