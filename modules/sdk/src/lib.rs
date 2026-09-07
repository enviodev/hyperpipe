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
    /// Pipeline control signal (reorg rollback / EOF). Default: ignore. Sinks
    /// that keep block-addressed state should handle
    /// [`ControlRecord::Rollback`] by invalidating everything past
    /// `invalidate_after_block` — return `Ok` only once that is durable.
    fn on_control(&mut self, _ctrl: ControlRecord) -> Result<(), String> {
        Ok(())
    }
}

/// Lock the module's state mutex, recovering from poisoning. A guest panic
/// traps the instance and the host discards it, but a poisoned lock must never
/// turn every later call on a surviving instance into a second panic — the
/// state itself is only ever replaced wholesale in `init`, so recovery is safe.
#[doc(hidden)]
pub fn lock_state<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn batch() -> Batch {
        Batch::new(
            "eth",
            1,
            BlockRange(100, 200),
            0,
            BatchKind::Log,
            vec![json!({ "value": "5000000000000" })],
        )
    }

    #[test]
    fn encode_decode_roundtrip() {
        let b = batch();
        let wire = encode_batch(&b).unwrap();
        let back = decode_batch(Encoding::Json.as_u8(), &wire).unwrap();
        assert_eq!(back.batch_id, b.batch_id);
        assert_eq!(back.block_range, b.block_range);
        assert_eq!(back.records[0]["value"], "5000000000000");
    }

    #[test]
    fn decode_batch_rejects_an_unknown_tag() {
        let wire = encode_batch(&batch()).unwrap();
        let e = decode_batch(9, &wire).err().unwrap();
        assert_eq!(e, "unknown encoding tag 9");
    }

    #[test]
    fn decode_batch_rejects_unwired_encodings() {
        let wire = encode_batch(&batch()).unwrap();
        // Tags 1 (cbor) and 2 (arrow-ipc) are valid in the WIT enum but not
        // implemented — the module must say so rather than mis-parse.
        for tag in [1u8, 2u8] {
            let e = decode_batch(tag, &wire).err().unwrap();
            assert!(e.contains("unsupported encoding"), "tag {tag}: {e}");
        }
    }

    #[test]
    fn decode_batch_reports_malformed_payloads() {
        let e = decode_batch(0, b"{ not json").err().unwrap();
        assert!(e.starts_with("json: "), "got {e}");
    }

    #[test]
    fn lock_state_recovers_from_a_poisoned_mutex() {
        // A guest panic inside `process` traps the instance, but a surviving
        // instance must not turn every later call into a poison panic.
        let m = std::sync::Arc::new(std::sync::Mutex::new(vec![1u8]));
        let m2 = m.clone();
        let _ = std::thread::spawn(move || {
            let _g = m2.lock().unwrap();
            panic!("poison it");
        })
        .join();
        assert!(m.lock().is_err(), "the mutex should now be poisoned");

        let mut guard = lock_state(&m);
        assert_eq!(*guard, vec![1u8], "state survives the poisoning");
        guard.push(2);
        drop(guard);
        assert_eq!(*lock_state(&m), vec![1u8, 2]);
    }
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
            // The exported interface re-uses the shared `types`; lift them to
            // the bindings root so the glue below reads the same for both worlds.
            pub use self::envio::hyperpipe::types::{Batch, Encoding, InitCtx, Output};

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

        impl __hp_bindings::exports::envio::hyperpipe::processor_impl::Guest for __HpComponent {
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
                *$crate::lock_state(&__HP_STATE) = ::core::option::Option::Some(inst);
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
                let mut guard = $crate::lock_state(&__HP_STATE);
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
            pub use self::envio::hyperpipe::types::{Batch, Encoding, InitCtx};

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

        impl __hp_bindings::exports::envio::hyperpipe::sink_impl::Guest for __HpComponent {
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
                *$crate::lock_state(&__HP_STATE) = ::core::option::Option::Some(inst);
                ::core::result::Result::Ok(())
            }

            fn write(
                input: __hp_bindings::Batch,
            ) -> ::core::result::Result<(), ::std::string::String> {
                let tag = __hp_enc_tag(&input.encoding);
                let batch = $crate::decode_batch(tag, &input.data)?;
                let mut guard = $crate::lock_state(&__HP_STATE);
                let inst = guard
                    .as_mut()
                    .ok_or_else(|| ::std::string::String::from("write called before init"))?;
                // Control batches route to the sink's on_control hook (default:
                // ignore); rollback-aware sinks invalidate state there.
                if batch.is_control() {
                    return match batch.as_control() {
                        ::core::option::Option::Some(ctrl) => {
                            <$ty as $crate::Sink>::on_control(inst, ctrl)
                        }
                        ::core::option::Option::None => ::core::result::Result::Ok(()),
                    };
                }
                <$ty as $crate::Sink>::write(inst, batch)
            }

            fn flush() -> ::core::result::Result<(), ::std::string::String> {
                let mut guard = $crate::lock_state(&__HP_STATE);
                match guard.as_mut() {
                    ::core::option::Option::Some(inst) => <$ty as $crate::Sink>::flush(inst),
                    ::core::option::Option::None => ::core::result::Result::Ok(()),
                }
            }
        }

        __hp_bindings::export!(__HpComponent with_types_in __hp_bindings);
    };
}
