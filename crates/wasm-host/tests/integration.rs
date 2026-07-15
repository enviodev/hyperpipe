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

use hp_encoding::{Batch, BatchKind, BlockRange, ControlRecord};
use hp_wasm_host::{
    build_services_async, build_services_bare, BlobConnCfg, MemKv, NodeCtx, Runtime, RuntimeConfig,
};
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

// ---------------------------------------------------------------------------
// Shared helpers for the module conformance matrix (§6)
// ---------------------------------------------------------------------------

/// Skip-with-a-shout when guest modules aren't built. CI greps for `SKIP:`.
macro_rules! need_modules {
    ($($name:expr),+ $(,)?) => {{
        let mut missing = Vec::new();
        $(if !module_path($name).exists() { missing.push($name); })+
        if !missing.is_empty() {
            eprintln!(
                "SKIP: guest modules not built ({missing:?}); run `cd modules && cargo build --target wasm32-wasip2`"
            );
            return;
        }
    }};
}

fn wasm(name: &str) -> Vec<u8> {
    std::fs::read(module_path(name)).expect("module readable")
}

fn node(name: &str, config: serde_json::Value) -> NodeCtx {
    NodeCtx {
        module_name: name.into(),
        pipeline_name: "test".into(),
        config_json: if config.is_null() { vec![] } else { serde_json::to_vec(&config).unwrap() },
        http_allow: vec![],
        granted_conns: vec![],
    }
}

fn tmpdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("hp-it-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// A decoded record, as the decoder would emit it.
fn decoded_rec(block: u64, value: &str) -> serde_json::Value {
    json!({
        "chain_id": 1, "block_number": block, "log_index": 0,
        "transaction_hash": format!("0x{block:064x}"), "address": "0xusdc",
        "event": "Transfer", "signature": "Transfer(address,address,uint256)",
        "params": { "from": "0xaaa", "to": "0xbbb", "value": value }
    })
}

fn decoded_batch(blocks: &[(u64, &str)]) -> Batch {
    let lo = blocks.first().map(|b| b.0).unwrap_or(0);
    let hi = blocks.last().map(|b| b.0 + 1).unwrap_or(1);
    Batch::new(
        "eth",
        1,
        BlockRange(lo, hi),
        0,
        BatchKind::Decoded,
        blocks.iter().map(|(b, v)| decoded_rec(*b, v)).collect(),
    )
}

fn rollback(after: u64) -> Batch {
    Batch::control(
        "eth",
        1,
        ControlRecord::Rollback { chain_id: 1, invalidate_after_block: after },
    )
}

// ---------------------------------------------------------------------------
// §5.5 — runtime: compile cache, init errors, instance pool
// ---------------------------------------------------------------------------

#[test]
fn compiling_a_component_populates_and_reuses_the_cache() {
    need_modules!("filter.wasm");
    let cache = tmpdir("compile-cache");
    let bytes = wasm("filter.wasm");

    let build = || {
        let tk = tokio::runtime::Runtime::new().unwrap();
        let services = build_services_bare(tk.handle().clone(), Arc::new(MemKv::default()));
        let cfg = RuntimeConfig { cache_dir: cache.clone(), ..RuntimeConfig::default() };
        (Runtime::new(cfg, services).unwrap(), tk)
    };
    let filter_cfg = json!({ "all": [{ "field": "params.value", "op": "gte", "value": "1" }] });

    let (rt, _tk) = build();
    rt.load_processor(&bytes, node("f", filter_cfg.clone())).expect("first load");
    let cached: Vec<_> = std::fs::read_dir(&cache)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "cwasm"))
        .collect();
    assert_eq!(cached.len(), 1, "the first compile writes one .cwasm");
    drop(rt);

    // Second runtime, same bytes -> the cached artifact is deserialized.
    let (rt2, _tk2) = build();
    rt2.load_processor(&bytes, node("f", filter_cfg.clone())).expect("cached load");
    assert_eq!(
        std::fs::read_dir(&cache).unwrap().count(),
        1,
        "no second artifact for the same module"
    );
    drop(rt2);

    // A corrupted cache file must fall back to recompiling, not fail the load.
    std::fs::write(cached[0].path(), b"garbage").unwrap();
    let (rt3, _tk3) = build();
    rt3.load_processor(&bytes, node("f", filter_cfg)).expect("recompile after corruption");
}

// ---------------------------------------------------------------------------
// §5.5 — the host's defences against a misbehaving module (test-chaos)
// ---------------------------------------------------------------------------

/// A runtime with a single-instance pool, so which instance served a call is
/// unambiguous, plus a short epoch deadline for the runaway test.
fn chaos_runtime(deadline_secs: u64, mem_bytes: usize) -> (Runtime, tokio::runtime::Runtime) {
    let tk = tokio::runtime::Runtime::new().expect("tokio");
    let services = build_services_bare(tk.handle().clone(), Arc::new(MemKv::default()));
    let cfg = RuntimeConfig {
        cache_dir: std::env::temp_dir().join("hyperpipe-test-cache"),
        instances_per_stage: 1,
        epoch_deadline_secs: deadline_secs,
        wasm_mem_bytes: mem_bytes,
    };
    (Runtime::new(cfg, services).expect("runtime"), tk)
}

/// The call counter the chaos module stamps on each record: 1 means it was
/// served by a fresh instance.
fn chaos_calls(b: &Batch) -> u64 {
    b.records[0]["chaos_calls"].as_u64().expect("chaos_calls tag")
}

#[test]
fn a_trapping_module_is_recycled_and_the_next_call_succeeds() {
    // The core resilience claim: a guest panic poisons its store, the host
    // throws that instance away, and the stage keeps working on a fresh one.
    need_modules!("test_chaos.wasm");
    let (rt, _tk) = chaos_runtime(5, 256 << 20);
    let proc = rt
        .load_processor(&wasm("test_chaos.wasm"), node("chaos", json!({ "trap_on_batch": 2 })))
        .expect("load");

    let ok = proc.process(&decoded_batch(&[(100, "1")])).expect("first call is clean");
    assert_eq!(chaos_calls(&ok[0]), 1);

    // Second call panics inside the guest.
    let e = proc.process(&decoded_batch(&[(101, "2")])).err().expect("the trap must surface");
    let msg = format!("{e:#}");
    assert!(msg.contains("call process"), "the error should name the failed call: {msg}");

    // Third call: the poisoned instance is gone, so a fresh one serves it —
    // its counter restarts at 1, which is the recycling made visible.
    let after = proc.process(&decoded_batch(&[(102, "3")])).expect("must recover on a fresh instance");
    assert_eq!(
        chaos_calls(&after[0]),
        1,
        "a recycled instance starts over; a reused (poisoned) one would not be usable at all"
    );
}

#[test]
fn a_graceful_error_keeps_the_instance_and_its_state() {
    // The counterpart: an `Err` return is not corruption. Discarding the
    // instance would throw away a buffering sink's un-flushed rows, so the host
    // must keep it — proven by the call counter continuing rather than resetting.
    need_modules!("test_chaos.wasm");
    let (rt, _tk) = chaos_runtime(5, 256 << 20);
    let proc = rt
        .load_processor(&wasm("test_chaos.wasm"), node("chaos", json!({ "error_on_batch": 2 })))
        .expect("load");

    assert_eq!(chaos_calls(&proc.process(&decoded_batch(&[(100, "1")])).unwrap()[0]), 1);

    let e = proc.process(&decoded_batch(&[(101, "2")])).err().expect("call 2 refuses");
    assert!(format!("{e:#}").contains("module process error"), "got {e:#}");

    let after = proc.process(&decoded_batch(&[(102, "3")])).expect("call 3 works");
    assert_eq!(
        chaos_calls(&after[0]),
        3,
        "state must survive a graceful Err — a reset counter would mean the instance was dropped"
    );
}

#[test]
fn a_runaway_module_is_stopped_by_the_epoch_deadline() {
    // Without this the host has no answer to `loop {}` in a guest: the worker
    // thread would be wedged forever.
    need_modules!("test_chaos.wasm");
    let (rt, _tk) = chaos_runtime(2, 256 << 20); // 2s deadline
    // Spin on the *second* call: a recycled instance restarts its counter, so
    // targeting call 1 would make the recovery call spin all over again.
    let proc = rt
        .load_processor(&wasm("test_chaos.wasm"), node("chaos", json!({ "loop_on_batch": 2 })))
        .expect("load");

    assert_eq!(chaos_calls(&proc.process(&decoded_batch(&[(100, "1")])).unwrap()[0]), 1);

    let start = std::time::Instant::now();
    let e = proc.process(&decoded_batch(&[(101, "2")])).err().expect("the loop must be interrupted");
    let took = start.elapsed();

    assert!(took < std::time::Duration::from_secs(15), "took {took:?} — the deadline did not fire");
    let msg = format!("{e:#}");
    assert!(msg.contains("call process"), "got {msg}");

    // The host is still standing: a fresh instance serves the next batch.
    let after = proc.process(&decoded_batch(&[(102, "3")])).expect("host survives the interrupt");
    assert_eq!(chaos_calls(&after[0]), 1, "the interrupted instance was recycled");
}

#[test]
fn a_memory_hog_is_capped_and_the_host_survives() {
    // The limiter denies the growth inside the guest's own store, so the guest
    // aborts and the host's memory is never at risk.
    need_modules!("test_chaos.wasm");
    let (rt, _tk) = chaos_runtime(30, 64 << 20); // 64 MiB cap
    // Hog on the second call, so the fresh instance that replaces it (counter
    // back at 1) can prove the host is still usable.
    let proc = rt
        .load_processor(
            &wasm("test_chaos.wasm"),
            // ask for far more than the cap
            node("chaos", json!({ "alloc_on_batch": 2, "alloc_bytes": 512u64 << 20 })),
        )
        .expect("load");

    assert_eq!(chaos_calls(&proc.process(&decoded_batch(&[(100, "1")])).unwrap()[0]), 1);

    let e = proc
        .process(&decoded_batch(&[(101, "2")]))
        .err()
        .expect("allocating past the cap must fail the call, not the process");
    assert!(format!("{e:#}").contains("call process"), "got {e:#}");

    let after = proc.process(&decoded_batch(&[(102, "3")])).expect("host survives the OOM");
    assert_eq!(chaos_calls(&after[0]), 1, "the aborted instance was recycled");
}

#[test]
fn a_module_that_stays_under_the_cap_is_left_alone() {
    // The cap must not be so eager that a legitimately hungry module dies.
    need_modules!("test_chaos.wasm");
    let (rt, _tk) = chaos_runtime(30, 256 << 20);
    let proc = rt
        .load_processor(
            &wasm("test_chaos.wasm"),
            node("chaos", json!({ "alloc_on_batch": 1, "alloc_bytes": 8u64 << 20 })),
        )
        .expect("load");
    let out = proc.process(&decoded_batch(&[(100, "1")])).expect("8 MiB under a 256 MiB cap is fine");
    assert_eq!(chaos_calls(&out[0]), 1);
}

#[test]
fn a_chaos_init_error_surfaces_at_load() {
    need_modules!("test_chaos.wasm");
    let (rt, _tk) = chaos_runtime(5, 256 << 20);
    let e = rt
        .load_processor(&wasm("test_chaos.wasm"), node("chaos", json!({ "init_error": true })))
        .err()
        .expect("init must fail");
    let msg = format!("{e:#}");
    assert!(msg.contains("module init:"), "got {msg}");
    assert!(msg.contains("failed on purpose"), "the module's own message should survive: {msg}");
}

#[test]
fn a_module_whose_init_errors_fails_the_load() {
    // `validate`'s dry-run depends on this surfacing at load time.
    need_modules!("filter.wasm");
    let (rt, _tk) = runtime();
    // The filter module rejects a config with no predicates.
    let e = rt
        .load_processor(&wasm("filter.wasm"), node("f", json!({})))
        .err()
        .expect("init must fail");
    let msg = format!("{e:#}");
    assert!(msg.contains("module init:"), "got {msg}");
    assert!(msg.contains("`all` and/or `any`"), "the module's own message should survive: {msg}");
}

#[test]
fn concurrent_calls_beyond_the_pool_instantiate_fresh_instances() {
    // instances_per_stage = 1 with two threads in `process`: the second call
    // finds the pool empty and must build its own instance rather than fail.
    need_modules!("filter.wasm");
    let tk = tokio::runtime::Runtime::new().unwrap();
    let services = build_services_bare(tk.handle().clone(), Arc::new(MemKv::default()));
    let cfg = RuntimeConfig {
        cache_dir: std::env::temp_dir().join("hyperpipe-test-cache"),
        instances_per_stage: 1,
        ..RuntimeConfig::default()
    };
    let rt = Runtime::new(cfg, services).unwrap();
    let proc = rt
        .load_processor(
            &wasm("filter.wasm"),
            node("f", json!({ "all": [{ "field": "params.value", "op": "gte", "value": "1" }] })),
        )
        .unwrap();

    let batch = decoded_batch(&[(100, "5"), (101, "7")]);
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let (p, b) = (proc.clone(), batch.clone());
            std::thread::spawn(move || p.process(&b).map(|o| o[0].len()))
        })
        .collect();
    for h in handles {
        assert_eq!(h.join().unwrap().expect("both calls succeed"), 2);
    }
}

// ---------------------------------------------------------------------------
// §6 — module conformance matrix
// ---------------------------------------------------------------------------

#[test]
fn filter_keeps_passing_records_and_drops_the_rest() {
    need_modules!("filter.wasm");
    let (rt, _tk) = runtime();
    let whale = json!({ "all": [{ "field": "params.value", "op": "gte", "value": "1000000" }] });
    let proc = rt.load_processor(&wasm("filter.wasm"), node("whales", whale)).unwrap();

    let out = proc
        .process(&decoded_batch(&[(100, "999999"), (101, "1000000"), (102, "5000000")]))
        .expect("process");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].len(), 2, "only the two whales survive");
    assert_eq!(out[0].records[0]["block_number"], 101);
    assert_eq!(out[0].batch_id, "eth:100:103:0", "identity survives the stage");
    assert_eq!(out[0].kind, BatchKind::Decoded);
}

#[test]
fn filter_dropping_everything_emits_no_batches() {
    // The module returns zero batches; propagating an empty one is the
    // *engine's* job (processor_task), not the module's.
    need_modules!("filter.wasm");
    let (rt, _tk) = runtime();
    let whale = json!({ "all": [{ "field": "params.value", "op": "gte", "value": "1000000" }] });
    let proc = rt.load_processor(&wasm("filter.wasm"), node("whales", whale)).unwrap();
    let out = proc.process(&decoded_batch(&[(100, "1"), (101, "2")])).expect("process");
    assert!(out.is_empty(), "got {out:?}");
}

#[test]
fn filter_passes_control_batches_through_untouched() {
    // SDK-level behaviour: a processor never sees control records.
    need_modules!("filter.wasm");
    let (rt, _tk) = runtime();
    let whale = json!({ "all": [{ "field": "params.value", "op": "gte", "value": "1000000" }] });
    let proc = rt.load_processor(&wasm("filter.wasm"), node("whales", whale)).unwrap();
    let out = proc.process(&rollback(19_000_000)).expect("process control");
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].as_control(),
        Some(ControlRecord::Rollback { chain_id: 1, invalidate_after_block: 19_000_000 })
    );
}

#[test]
fn decoder_in_error_mode_propagates_a_module_error() {
    need_modules!("evm_abi_decoder.wasm");
    let (rt, _tk) = runtime();
    let abi = json!([
        {"anonymous":false,"type":"event","name":"Transfer","inputs":[
            {"indexed":true,"name":"from","type":"address"},
            {"indexed":true,"name":"to","type":"address"},
            {"indexed":false,"name":"value","type":"uint256"}]}
    ]);
    let cfg = json!({ "abis": [{ "abi": abi, "events": ["Transfer"] }], "on_undecodable": "error" });
    let proc = rt.load_processor(&wasm("evm_abi_decoder.wasm"), node("decode", cfg)).unwrap();

    // A log for an event the decoder doesn't know.
    let batch = Batch::new(
        "eth",
        1,
        BlockRange(100, 101),
        0,
        BatchKind::Log,
        vec![json!({ "topic0": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
                     "block_number": 100 })],
    );
    let e = proc.process(&batch).err().expect("error mode must fail the batch");
    assert!(format!("{e:#}").contains("module process error"), "got {e:#}");
}

#[test]
fn stdout_and_blackhole_sinks_accept_writes_and_flushes() {
    need_modules!("stdout_sink.wasm", "blackhole_sink.wasm");
    let (rt, _tk) = runtime();
    for name in ["stdout_sink.wasm", "blackhole_sink.wasm"] {
        let sink = rt.load_sink(&wasm(name), node("out", serde_json::Value::Null)).unwrap();
        sink.write(&decoded_batch(&[(100, "5")])).unwrap_or_else(|e| panic!("{name}: {e:#}"));
        sink.write(&rollback(99)).unwrap_or_else(|e| panic!("{name} control: {e:#}"));
        sink.flush().unwrap_or_else(|e| panic!("{name} flush: {e:#}"));
    }
}

#[test]
fn the_enrich_example_module_adds_its_fields() {
    // The extensibility claim: an out-of-tree module built against the SDK.
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/modules/enrich.wasm");
    if !path.exists() {
        eprintln!("SKIP: examples/modules/enrich.wasm not built");
        return;
    }
    let (rt, _tk) = runtime();
    let proc = rt
        .load_processor(&std::fs::read(&path).unwrap(), node("enrich", json!({ "whale_usd": 1.0 })))
        .expect("load enrich");

    // 5 USDC (6 decimals) is not a whale at a $1M threshold... but 2_000_000 raw
    // units = $2.00, which clears a $1 threshold.
    let out = proc.process(&decoded_batch(&[(100, "500000"), (101, "2000000")])).unwrap();
    let recs = &out[0].records;
    assert_eq!(recs[0]["usd_estimate"], "0.50");
    assert_eq!(recs[0]["whale"], json!(false));
    assert_eq!(recs[1]["usd_estimate"], "2.00");
    assert_eq!(recs[1]["whale"], json!(true));
}

// ---- s3 sink (local object store) ---------------------------------------

/// A runtime whose `lake` connection writes to `dir`.
fn runtime_with_lake(dir: &std::path::Path) -> (Runtime, tokio::runtime::Runtime) {
    let tk = tokio::runtime::Runtime::new().expect("tokio");
    let blob = std::collections::HashMap::from([(
        "lake".to_string(),
        BlobConnCfg {
            local_path: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        },
    )]);
    let services = tk
        .block_on(build_services_async(
            tk.handle().clone(),
            std::collections::HashMap::new(),
            blob,
            Arc::new(MemKv::default()),
        ))
        .expect("services");
    let cfg = RuntimeConfig {
        cache_dir: std::env::temp_dir().join("hyperpipe-test-cache"),
        ..RuntimeConfig::default()
    };
    (Runtime::new(cfg, services).expect("runtime"), tk)
}

fn lake_node(config: serde_json::Value) -> NodeCtx {
    NodeCtx {
        module_name: "lake_sink".into(),
        pipeline_name: "test".into(),
        config_json: serde_json::to_vec(&config).unwrap(),
        http_allow: vec![],
        granted_conns: vec!["lake".into()],
    }
}

fn objects_in(dir: &std::path::Path) -> Vec<String> {
    fn walk(dir: &std::path::Path, base: &std::path::Path, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for e in entries.filter_map(|e| e.ok()) {
            let p = e.path();
            if p.is_dir() {
                walk(&p, base, out);
            } else if !p.to_string_lossy().contains("#") {
                // object_store writes atomically via a `#` temp file; ignore those
                out.push(p.strip_prefix(base).unwrap().to_string_lossy().into_owned());
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

#[test]
fn s3_sink_buffers_until_flush_rows_then_writes_a_deterministic_key() {
    need_modules!("s3_sink.wasm");
    let dir = tmpdir("s3-flush");
    let (rt, _tk) = runtime_with_lake(&dir);
    let sink = rt
        .load_sink(
            &wasm("s3_sink.wasm"),
            lake_node(json!({ "connection": "lake", "format": "ndjson", "flush_rows": 3 })),
        )
        .unwrap();

    // Below the threshold: nothing is written yet.
    sink.write(&decoded_batch(&[(100, "1"), (101, "2")])).unwrap();
    assert!(objects_in(&dir).is_empty(), "buffered, not written");

    // Crossing it flushes one object with the deterministic key.
    sink.write(&decoded_batch(&[(102, "3")])).unwrap();
    assert_eq!(objects_in(&dir), vec!["1/100-103-3.ndjson"]);

    // The next range starts a fresh object; the barrier flushes the remainder.
    sink.write(&decoded_batch(&[(200, "4")])).unwrap();
    sink.flush().unwrap();
    assert_eq!(objects_in(&dir), vec!["1/100-103-3.ndjson", "1/200-201-1.ndjson"]);
}

#[test]
fn s3_sink_flush_of_an_empty_buffer_writes_nothing() {
    need_modules!("s3_sink.wasm");
    let dir = tmpdir("s3-empty-flush");
    let (rt, _tk) = runtime_with_lake(&dir);
    let sink = rt
        .load_sink(&wasm("s3_sink.wasm"), lake_node(json!({ "connection": "lake" })))
        .unwrap();
    sink.flush().expect("an empty flush is a no-op, not an error");
    assert!(objects_in(&dir).is_empty());
}

#[test]
fn s3_sink_purges_buffered_rows_on_a_rollback() {
    need_modules!("s3_sink.wasm");
    let dir = tmpdir("s3-rollback");
    let (rt, _tk) = runtime_with_lake(&dir);
    let sink = rt
        .load_sink(
            &wasm("s3_sink.wasm"),
            lake_node(json!({ "connection": "lake", "format": "ndjson", "flush_rows": 100 })),
        )
        .unwrap();

    sink.write(&decoded_batch(&[(100, "1"), (105, "2")])).unwrap();
    // Reorg: everything after 102 is void, and it never reaches an object.
    sink.write(&rollback(102)).unwrap();
    sink.flush().unwrap();

    let objects = objects_in(&dir);
    assert_eq!(objects.len(), 1);
    let body = std::fs::read_to_string(dir.join(&objects[0])).unwrap();
    assert!(body.contains("\"block_number\":100"));
    assert!(!body.contains("\"block_number\":105"), "the forked row must be purged");
}

#[test]
fn s3_sink_write_without_the_connection_grant_fails() {
    // Capability enforcement through the full stack: config alone is not a grant.
    need_modules!("s3_sink.wasm");
    let dir = tmpdir("s3-ungranted");
    let (rt, _tk) = runtime_with_lake(&dir);
    let mut node = lake_node(json!({ "connection": "lake", "format": "ndjson", "flush_rows": 1 }));
    node.granted_conns = vec![]; // the YAML forgot `connections: [lake]`
    let sink = rt.load_sink(&wasm("s3_sink.wasm"), node).unwrap();

    let e = sink.write(&decoded_batch(&[(100, "1")])).err().expect("must be denied");
    assert!(format!("{e:#}").contains("not granted"), "got {e:#}");
    assert!(objects_in(&dir).is_empty(), "nothing may be written");
}

// ---- webhook sink --------------------------------------------------------

fn webhook_node(url: &str, allow: Vec<String>, extra: serde_json::Value) -> NodeCtx {
    let mut config = json!({ "url": url });
    if let (Some(o), Some(e)) = (config.as_object_mut(), extra.as_object()) {
        for (k, v) in e {
            o.insert(k.clone(), v.clone());
        }
    }
    NodeCtx {
        module_name: "hook".into(),
        pipeline_name: "test".into(),
        config_json: serde_json::to_vec(&config).unwrap(),
        http_allow: allow,
        granted_conns: vec![],
    }
}

#[test]
fn webhook_posts_ndjson_and_chunks_at_max_batch() {
    need_modules!("webhook_sink.wasm");
    let (rt, tk) = runtime();
    let srv = tk.block_on(hp_testutil::MockServer::start(|_p, _b, _i| {
        hp_testutil::Reply::json(json!({ "ok": true }))
    }));
    let sink = rt
        .load_sink(
            &wasm("webhook_sink.wasm"),
            webhook_node(&format!("{}/hook", srv.url), vec!["127.0.0.1".into()], json!({ "max_batch": 2 })),
        )
        .unwrap();

    sink.write(&decoded_batch(&[(100, "1"), (101, "2"), (102, "3")])).expect("delivered");
    assert_eq!(srv.call_count(), 2, "3 records at max_batch 2 -> two POSTs");
}

#[test]
fn webhook_forwards_a_rollback_as_one_control_line() {
    need_modules!("webhook_sink.wasm");
    let (rt, tk) = runtime();
    let srv = tk.block_on(hp_testutil::MockServer::start(|_p, _b, _i| {
        hp_testutil::Reply::json(json!({ "ok": true }))
    }));
    let sink = rt
        .load_sink(
            &wasm("webhook_sink.wasm"),
            webhook_node(&format!("{}/hook", srv.url), vec!["127.0.0.1".into()], json!({})),
        )
        .unwrap();

    sink.write(&rollback(19_000_000)).expect("forwarded");
    assert_eq!(srv.call_count(), 1);
    // The consumer needs the control record itself to invalidate what it has.
    let body = &srv.requests()[0].body;
    assert_eq!(body["control"], "rollback");
    assert_eq!(body["invalidate_after_block"], 19_000_000);
    assert_eq!(body["chain_id"], 1);
}

#[test]
fn webhook_can_opt_out_of_forwarding_rollbacks() {
    need_modules!("webhook_sink.wasm");
    let (rt, tk) = runtime();
    let srv = tk.block_on(hp_testutil::MockServer::start(|_p, _b, _i| {
        hp_testutil::Reply::json(json!({ "ok": true }))
    }));
    let sink = rt
        .load_sink(
            &wasm("webhook_sink.wasm"),
            webhook_node(
                &format!("{}/hook", srv.url),
                vec!["127.0.0.1".into()],
                json!({ "forward_rollbacks": false }),
            ),
        )
        .unwrap();
    sink.write(&rollback(1)).expect("silently ignored");
    assert_eq!(srv.call_count(), 0);
}

#[test]
fn webhook_treats_a_non_2xx_as_a_retryable_error() {
    need_modules!("webhook_sink.wasm");
    let (rt, tk) = runtime();
    let srv = tk.block_on(hp_testutil::MockServer::start(|_p, _b, _i| {
        hp_testutil::Reply::status(500, json!({ "err": "nope" }))
    }));
    let sink = rt
        .load_sink(
            &wasm("webhook_sink.wasm"),
            webhook_node(&format!("{}/hook", srv.url), vec!["127.0.0.1".into()], json!({})),
        )
        .unwrap();
    let e = sink.write(&decoded_batch(&[(100, "1")])).err().expect("500 must not ack");
    assert!(format!("{e:#}").contains("HTTP 500"), "got {e:#}");
}

#[test]
fn webhook_to_a_host_outside_the_allowlist_is_denied() {
    need_modules!("webhook_sink.wasm");
    let (rt, tk) = runtime();
    let srv = tk.block_on(hp_testutil::MockServer::start(|_p, _b, _i| {
        hp_testutil::Reply::json(json!({ "ok": true }))
    }));
    let sink = rt
        .load_sink(
            &wasm("webhook_sink.wasm"),
            // permissions.http grants a different host than config.url targets
            webhook_node(&format!("{}/hook", srv.url), vec!["api.example.com".into()], json!({})),
        )
        .unwrap();

    let e = sink.write(&decoded_batch(&[(100, "1")])).err().expect("denied");
    assert!(format!("{e:#}").contains("http denied"), "got {e:#}");
    assert_eq!(srv.call_count(), 0, "the request must never leave the host");
}

#[test]
fn webhook_requires_a_url() {
    need_modules!("webhook_sink.wasm");
    let (rt, _tk) = runtime();
    let e = rt
        .load_sink(&wasm("webhook_sink.wasm"), node("hook", json!({})))
        .err()
        .expect("init must fail");
    assert!(format!("{e:#}").contains("config.url required"), "got {e:#}");
}

// ---- postgres sink (needs HP_TEST_PG_DSN) --------------------------------

fn pg_runtime(table: &str) -> Option<(Runtime, tokio::runtime::Runtime)> {
    let dsn = match std::env::var("HP_TEST_PG_DSN") {
        Ok(d) => d,
        Err(_) => {
            eprintln!("SKIP: set HP_TEST_PG_DSN to run the postgres sink conformance tests");
            return None;
        }
    };
    let tk = tokio::runtime::Runtime::new().unwrap();
    let pg = std::collections::HashMap::from([("main".to_string(), (dsn, 2u32))]);
    let services = tk
        .block_on(build_services_async(
            tk.handle().clone(),
            pg,
            std::collections::HashMap::new(),
            Arc::new(MemKv::default()),
        ))
        .expect("HP_TEST_PG_DSN is set but unreachable");
    let pool = services.sql.get("main").unwrap().clone();
    tk.block_on(async {
        sqlx::query(&format!("DROP TABLE IF EXISTS {table}")).execute(&pool).await.unwrap();
    });
    let cfg = RuntimeConfig {
        cache_dir: std::env::temp_dir().join("hyperpipe-test-cache"),
        ..RuntimeConfig::default()
    };
    Some((Runtime::new(cfg, services).unwrap(), tk)
    )
}

fn pg_count(rt: &Runtime, tk: &tokio::runtime::Runtime, table: &str) -> i64 {
    let pool = rt.services().sql.get("main").unwrap().clone();
    tk.block_on(async {
        sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .unwrap()
    })
}

fn pg_node(table: &str, granted: bool, extra: serde_json::Value) -> NodeCtx {
    let mut config = json!({
        "connection": "main", "table": table, "mode": "upsert",
        "unique_key": ["chain_id", "block_number", "log_index"],
        "create_table": true,
        "column_map": { "params.value": "amount" }
    });
    if let (Some(o), Some(e)) = (config.as_object_mut(), extra.as_object()) {
        for (k, v) in e {
            o.insert(k.clone(), v.clone());
        }
    }
    NodeCtx {
        module_name: "pg".into(),
        pipeline_name: "test".into(),
        config_json: serde_json::to_vec(&config).unwrap(),
        http_allow: vec![],
        granted_conns: if granted { vec!["main".into()] } else { vec![] },
    }
}

#[test]
fn postgres_sink_auto_creates_and_upserts_idempotently() {
    let table = "hp_conf_upsert";
    let Some((rt, tk)) = pg_runtime(table) else { return };
    let sink = rt.load_sink(&wasm("postgres_sink.wasm"), pg_node(table, true, json!({}))).unwrap();

    let batch = decoded_batch(&[(100, "5000000000000"), (101, "7")]);
    sink.write(&batch).expect("auto-DDL + insert");
    assert_eq!(pg_count(&rt, &tk, table), 2);

    // Replay of the identical batch (a crash-restart) must not duplicate rows.
    sink.write(&batch).expect("replay");
    assert_eq!(pg_count(&rt, &tk, table), 2, "upsert on the unique key");

    // The uint256 landed in a numeric column with full precision.
    let pool = rt.services().sql.get("main").unwrap().clone();
    let amount: String = tk.block_on(async {
        sqlx::query_scalar(&format!("SELECT amount::text FROM {table} WHERE block_number = 100"))
            .fetch_one(&pool)
            .await
            .unwrap()
    });
    assert_eq!(amount, "5000000000000");
}

#[test]
fn postgres_sink_deletes_rows_past_the_fork_on_a_rollback() {
    let table = "hp_conf_rollback";
    let Some((rt, tk)) = pg_runtime(table) else { return };
    let sink = rt
        .load_sink(&wasm("postgres_sink.wasm"), pg_node(table, true, json!({ "rollback": true })))
        .unwrap();

    sink.write(&decoded_batch(&[(100, "1"), (105, "2"), (110, "3")])).unwrap();
    assert_eq!(pg_count(&rt, &tk, table), 3);

    sink.write(&rollback(102)).expect("rollback applied");
    assert_eq!(pg_count(&rt, &tk, table), 1, "only block 100 survives");
}

#[test]
fn postgres_sink_without_rollback_config_keeps_stale_rows() {
    // Documented behaviour: rollback handling is opt-in; the module warns
    // rather than failing the control batch.
    let table = "hp_conf_norollback";
    let Some((rt, tk)) = pg_runtime(table) else { return };
    let sink = rt.load_sink(&wasm("postgres_sink.wasm"), pg_node(table, true, json!({}))).unwrap();

    sink.write(&decoded_batch(&[(100, "1"), (105, "2")])).unwrap();
    sink.write(&rollback(102)).expect("must not error");
    assert_eq!(pg_count(&rt, &tk, table), 2, "no delete without config.rollback");
}

#[test]
fn postgres_sink_without_the_connection_grant_fails() {
    let table = "hp_conf_ungranted";
    let Some((rt, _tk)) = pg_runtime(table) else { return };
    let sink = rt.load_sink(&wasm("postgres_sink.wasm"), pg_node(table, false, json!({}))).unwrap();
    let e = sink.write(&decoded_batch(&[(100, "1")])).err().expect("denied");
    assert!(format!("{e:#}").contains("not granted"), "got {e:#}");
}

#[test]
fn postgres_sink_flush_error_propagates_through_the_host() {
    // A write to a table that cannot exist: the module's Err must reach the
    // engine so the branch retries rather than acking lost data.
    let table = "hp_conf_broken";
    let Some((rt, _tk)) = pg_runtime(table) else { return };
    let sink = rt
        .load_sink(
            &wasm("postgres_sink.wasm"),
            // create_table off + a table nobody created = every write errors
            pg_node(table, true, json!({ "create_table": false })),
        )
        .unwrap();
    let e = sink.write(&decoded_batch(&[(100, "1")])).err().expect("must fail");
    assert!(format!("{e:#}").contains("module write error"), "got {e:#}");
}
