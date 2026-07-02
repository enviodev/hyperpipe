//! End-to-end host test: load the real decoder + stdout components and run a
//! batch through the actual wasmtime runtime. Requires the guest modules to be
//! built first:
//!
//!   (cd modules && cargo build --target wasm32-wasip2)
//!
//! If the artifacts are missing the test soft-skips so `cargo test` stays green
//! in environments without the wasm toolchain.

use std::path::PathBuf;
use std::sync::Arc;

use hp_encoding::{Batch, BatchKind, BlockRange};
use hp_wasm_host::{build_services_bare, MemKv, NodeCtx, Runtime, RuntimeConfig};
use serde_json::json;

fn module_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../modules/target/wasm32-wasip2/debug")
        .join(name)
}

fn decoder_config() -> Vec<u8> {
    let abi = json!([
        {"anonymous":false,"type":"event","name":"Transfer","inputs":[
            {"indexed":true,"name":"from","type":"address"},
            {"indexed":true,"name":"to","type":"address"},
            {"indexed":false,"name":"value","type":"uint256"}]}
    ]);
    let cfg = json!({ "abis": [{ "abi": abi, "events": ["Transfer"] }], "on_undecodable": "drop" });
    serde_json::to_vec(&cfg).unwrap()
}

fn log_batch() -> Batch {
    let rec = json!({
        "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "topic1": "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "topic2": "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "data": format!("0x{:064x}", 1_250_000_000u128),
        "block_number": 19000042,
        "log_index": 12,
        "transaction_hash": "0xdead",
        "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    });
    Batch::new(
        "eth_usdc_logs",
        1,
        BlockRange(19000042, 19000043),
        0,
        BatchKind::Log,
        vec![rec],
    )
}

/// Build a runtime backed by a dedicated tokio runtime for host services. The
/// returned tokio `Runtime` must be kept alive for the duration of the test.
/// Wasm calls run on THIS (non-async-context) thread, mirroring how the engine
/// runs stages on `spawn_blocking` threads.
fn runtime() -> (Runtime, tokio::runtime::Runtime) {
    let tk = tokio::runtime::Runtime::new().expect("tokio");
    let services = build_services_bare(tk.handle().clone(), Arc::new(MemKv::default()));
    let mut cfg = RuntimeConfig::default();
    cfg.cache_dir = std::env::temp_dir().join("hyperpipe-test-cache");
    cfg.instances_per_stage = 2;
    (Runtime::new(cfg, services).expect("runtime"), tk)
}

#[test]
fn decoder_then_stdout_roundtrip() {
    let decoder = module_path("evm_abi_decoder.wasm");
    let stdout = module_path("stdout_sink.wasm");
    if !decoder.exists() || !stdout.exists() {
        eprintln!("SKIP: guest modules not built ({decoder:?}); run `cd modules && cargo build --target wasm32-wasip2`");
        return;
    }

    let (rt, _tk) = runtime();

    let proc = rt
        .load_processor(
            &std::fs::read(&decoder).unwrap(),
            NodeCtx {
                module_name: "decode".into(),
                pipeline_name: "test".into(),
                config_json: decoder_config(),
                http_allow: vec![],
                granted_conns: vec![],
            },
        )
        .expect("load decoder");

    let outputs = proc.process(&log_batch()).expect("process");
    assert_eq!(outputs.len(), 1, "one decoded batch");
    let decoded = &outputs[0];
    assert_eq!(decoded.kind, BatchKind::Decoded);
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(decoded.records[0]["event"], "Transfer");
    assert_eq!(decoded.records[0]["params"]["value"], "1250000000");
    // batch_id identity is preserved across the processing stage.
    assert_eq!(decoded.batch_id, "eth_usdc_logs:19000042:19000043:0");

    let sink = rt
        .load_sink(
            &std::fs::read(&stdout).unwrap(),
            NodeCtx {
                module_name: "out".into(),
                pipeline_name: "test".into(),
                config_json: vec![],
                http_allow: vec![],
                granted_conns: vec![],
            },
        )
        .expect("load stdout sink");

    sink.write(decoded).expect("sink write");
    sink.flush().expect("flush");
}

#[test]
fn control_batch_passes_through_processor() {
    let decoder = module_path("evm_abi_decoder.wasm");
    if !decoder.exists() {
        eprintln!("SKIP: decoder module not built");
        return;
    }
    let (rt, _tk) = runtime();
    let proc = rt
        .load_processor(
            &std::fs::read(&decoder).unwrap(),
            NodeCtx {
                module_name: "decode".into(),
                pipeline_name: "test".into(),
                config_json: decoder_config(),
                http_allow: vec![],
                granted_conns: vec![],
            },
        )
        .expect("load decoder");

    let ctrl = Batch::control(
        "eth_usdc_logs",
        1,
        hp_encoding::ControlRecord::Eof {
            source: "eth_usdc_logs".into(),
            at_block: 19000043,
        },
    );
    let out = proc.process(&ctrl).expect("process control");
    assert_eq!(out.len(), 1);
    assert!(out[0].is_control(), "control batch must pass through unchanged");
}
