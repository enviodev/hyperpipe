//! DAG runtime (M3): wire sources -> wasm processors -> wasm sinks with bounded
//! channels, fan-out, backpressure, and graceful shutdown. Checkpointing (M7)
//! and reorg handling (phase 2) layer on top of this.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use std::collections::VecDeque;

use anyhow::{anyhow, Context, Result};
use hp_encoding::{Batch, ControlRecord, KvStore};
use hp_engine::config::{Config, ModuleRef, NodeKind};
use hp_engine::CheckpointStore;
use hp_source_hypersync::HyperSyncSource;
use hp_wasm_host::{build_services_async, build_services_bare, NodeCtx, Runtime, WasmProcessor, WasmSink};
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// A sink is either a real WASM module or the built-in native stdout tap
/// (used by `--debug-stdout` so pipelines run without configuring real sinks).
#[derive(Clone)]
enum SinkImpl {
    Wasm(WasmSink),
    Stdout,
}

impl SinkImpl {
    fn write(&self, batch: &Batch) -> Result<()> {
        match self {
            SinkImpl::Wasm(s) => s.write(batch),
            SinkImpl::Stdout => {
                if batch.is_control() {
                    return Ok(());
                }
                use std::io::Write;
                let stdout = std::io::stdout();
                let mut out = stdout.lock();
                for r in &batch.records {
                    let _ = writeln!(out, "{}", serde_json::to_string(r).unwrap_or_default());
                }
                Ok(())
            }
        }
    }
    fn flush(&self) -> Result<()> {
        match self {
            SinkImpl::Wasm(s) => s.flush(),
            SinkImpl::Stdout => Ok(()),
        }
    }
}

#[derive(Default)]
struct Stats {
    per_source: Mutex<HashMap<String, Arc<SourceStat>>>,
}

#[derive(Default)]
struct SourceStat {
    records: AtomicU64,
    batches: AtomicU64,
}

/// Run a pipeline to completion (backfill EOF) or until ctrl-c.
pub async fn run(config_path: &Path, debug_stdout: bool) -> Result<()> {
    let text = std::fs::read_to_string(config_path)
        .with_context(|| format!("read {}", config_path.display()))?;
    let cfg = Config::load_str(&text, &hp_engine::config::EnvSecrets)
        .map_err(|e| anyhow!("invalid pipeline: {e}"))?;
    let base_dir = config_path
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    let profile = cfg.profile();
    let handle = tokio::runtime::Handle::current();

    // Durable checkpoint store (cursors + module KV) — also the KvStore backend.
    let store = Arc::new(
        CheckpointStore::open(&cfg.runtime.checkpoint.path, handle.clone())
            .await
            .context("open checkpoint store")?,
    );
    let kv: Arc<dyn KvStore> = store.clone();

    // Open postgres pools only when a real (non-debug) sink needs them.
    let services = if debug_stdout {
        build_services_bare(handle.clone(), kv.clone())
    } else {
        let mut pg = HashMap::new();
        let mut blob = HashMap::new();
        for (name, conn) in &cfg.connections {
            match conn.r#type {
                hp_engine::config::ConnectionKind::Postgres => {
                    if let Some(dsn) = conn.dsn.clone() {
                        pg.insert(name.clone(), (dsn, conn.pool.max));
                    }
                }
                hp_engine::config::ConnectionKind::S3 => {
                    blob.insert(
                        name.clone(),
                        hp_wasm_host::BlobConnCfg {
                            bucket: conn.bucket.clone(),
                            region: conn.region.clone(),
                            endpoint: conn.endpoint.clone(),
                            prefix: conn.prefix.clone(),
                            local_path: conn.local_path.clone(),
                        },
                    );
                }
                hp_engine::config::ConnectionKind::Kafka => {}
            }
        }
        build_services_async(handle.clone(), pg, blob, kv.clone())
            .await
            .context("open connections")?
    };

    let rt = Arc::new(Runtime::new(
        hp_wasm_host::RuntimeConfig {
            cache_dir: default_cache_dir(),
            wasm_mem_bytes: profile.wasm_mem_bytes,
            epoch_deadline_secs: profile.epoch_deadline_secs,
            instances_per_stage: profile.instances_per_stage,
        },
        services,
    )?);

    // ---- load modules ----
    let module_dir = module_dir();
    let mut processors: HashMap<String, WasmProcessor> = HashMap::new();
    for p in &cfg.processors {
        let wasm = resolve_module(&p.module, &base_dir, &module_dir)
            .with_context(|| format!("resolve module for processor `{}`", p.name))?;
        let config_json = preprocess_config(&p.module, &p.config, &base_dir)?;
        let node = NodeCtx {
            module_name: p.name.clone(),
            pipeline_name: cfg.name.clone(),
            config_json,
            http_allow: p.permissions.http.clone(),
            granted_conns: vec![],
        };
        let wp = rt
            .load_processor(&wasm, node)
            .with_context(|| format!("load processor `{}`", p.name))?;
        processors.insert(p.name.clone(), wp);
    }

    let mut sinks: HashMap<String, SinkImpl> = HashMap::new();
    for s in &cfg.sinks {
        if debug_stdout {
            sinks.insert(s.name.clone(), SinkImpl::Stdout);
            continue;
        }
        let wasm = resolve_module(&s.module, &base_dir, &module_dir)
            .with_context(|| format!("resolve module for sink `{}`", s.name))?;
        let config_json = serde_json::to_vec(&s.config).unwrap_or_default();
        let node = NodeCtx {
            module_name: s.name.clone(),
            pipeline_name: cfg.name.clone(),
            config_json,
            http_allow: s.permissions.http.clone(),
            granted_conns: s.connections.clone(),
        };
        let ws = rt
            .load_sink(&wasm, node)
            .with_context(|| format!("load sink `{}`", s.name))?;
        sinks.insert(s.name.clone(), SinkImpl::Wasm(ws));
    }

    // ---- wiring: input channel per consumer node; producers hold sender clones ----
    let cap = profile.channel_capacity;
    let mut input_tx: HashMap<String, Sender<Batch>> = HashMap::new();
    let mut input_rx: HashMap<String, Receiver<Batch>> = HashMap::new();
    for name in cfg
        .processors
        .iter()
        .map(|p| &p.name)
        .chain(cfg.sinks.iter().map(|s| &s.name))
    {
        let (tx, rx) = channel::<Batch>(cap);
        input_tx.insert(name.clone(), tx);
        input_rx.insert(name.clone(), rx);
    }

    // producer -> consumer senders
    let consumers_of = build_consumer_map(&cfg);
    let senders_for = |producer: &str| -> Vec<Sender<Batch>> {
        consumers_of
            .get(producer)
            .map(|cs| cs.iter().filter_map(|c| input_tx.get(c).cloned()).collect())
            .unwrap_or_default()
    };

    let reachable = reachable_sinks_per_source(&cfg);
    let stop = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(Stats::default());
    let mut tasks = Vec::new();

    // ---- sink tasks ----
    for s in &cfg.sinks {
        let rx = input_rx.remove(&s.name).expect("sink rx");
        let sink = sinks.get(&s.name).cloned().expect("sink impl");
        let name = s.name.clone();
        tasks.push(tokio::spawn(sink_task(name, sink, rx, store.clone())));
    }

    // ---- processor tasks ----
    for p in &cfg.processors {
        let rx = input_rx.remove(&p.name).expect("proc rx");
        let wp = processors.get(&p.name).cloned().expect("proc impl");
        let outs = senders_for(&p.name);
        let name = p.name.clone();
        tasks.push(tokio::spawn(processor_task(name, wp, rx, outs)));
    }

    // ---- source tasks ----
    for src_cfg in &cfg.sources {
        let source = HyperSyncSource::from_config(src_cfg, profile.default_max_records);
        let outs = senders_for(&src_cfg.name);
        let stat = Arc::new(SourceStat::default());
        stats.per_source.lock().unwrap().insert(src_cfg.name.clone(), stat.clone());
        let stop_c = stop.clone();
        let name = src_cfg.name.clone();
        // Resume from the checkpoint (min cursor over reachable sinks), never
        // below the configured from_block.
        let sinks_for_src = reachable.get(&src_cfg.name).cloned().unwrap_or_default();
        let start = store
            .restore(&src_cfg.name, &sinks_for_src)
            .map(|v| v.max(source.default_start()))
            .unwrap_or_else(|| source.default_start());
        if start > source.default_start() {
            tracing::info!(source = %name, resume_block = start, "resuming from checkpoint");
        }
        tasks.push(tokio::spawn(source_task(
            name,
            source,
            start,
            outs,
            stop_c,
            stat,
            kv.clone(),
        )));
    }

    // drop the wiring map so channels close once producers finish
    drop(input_tx);

    // ---- checkpoint task: flush sinks, then persist cursors, every 2s ----
    let ckpt_store = store.clone();
    let ckpt_sinks: Vec<SinkImpl> = sinks.values().cloned().collect();
    let ckpt_stop = stop.clone();
    let ckpt = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            checkpoint_once(&ckpt_store, &ckpt_sinks).await;
            if ckpt_stop.load(Ordering::Relaxed) {
                break;
            }
        }
    });

    // ---- status line ----
    let status_stop = stop.clone();
    let status_stats = stats.clone();
    let status = tokio::spawn(status_task(status_stats, status_stop));

    // ---- health server (K8s probes) ----
    let health = crate::health::maybe_spawn();
    crate::health::set_state(crate::health::RUNNING);

    tracing::info!(pipeline = %cfg.name, "running ({} sources, {} processors, {} sinks){}",
        cfg.sources.len(), cfg.processors.len(), cfg.sinks.len(),
        if debug_stdout { " [debug-stdout]" } else { "" });

    // ---- wait for ctrl-c or natural completion ----
    let all = futures_join_all(tasks);
    tokio::select! {
        _ = shutdown_signal() => {
            tracing::info!("shutdown signal (SIGINT/SIGTERM): draining...");
            crate::health::set_state(crate::health::STOPPING);
            stop.store(true, Ordering::Relaxed);
        }
        _ = all => {
            tracing::info!("pipeline finished (all sources reached EOF)");
            crate::health::set_state(crate::health::STOPPING);
        }
    }
    if let Some(h) = health {
        h.abort();
    }

    // Final checkpoint: flush all sinks, then persist cursors durably.
    ckpt.abort();
    let final_sinks: Vec<SinkImpl> = sinks.values().cloned().collect();
    checkpoint_once(&store, &final_sinks).await;
    status.abort();
    Ok(())
}

/// One checkpoint: snapshot watermarks, flush every sink (durability), then
/// persist the snapshot. Snapshot is taken before flush so only durable batches
/// advance (§8.2 rule 5).
async fn checkpoint_once(store: &Arc<CheckpointStore>, sinks: &[SinkImpl]) {
    let snapshot = store.snapshot();
    for s in sinks {
        let s2 = s.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || s2.flush()).await.unwrap_or(Ok(())) {
            tracing::warn!("sink flush error during checkpoint: {e}");
            return; // don't advance cursors if a flush failed
        }
    }
    if let Err(e) = store.persist_snapshot(snapshot).await {
        tracing::warn!("checkpoint persist failed: {e}");
    }
}

/// For each source, the set of sink nodes reachable from it in the DAG. Used to
/// compute the safe restart cursor (min over these sinks).
fn reachable_sinks_per_source(cfg: &Config) -> HashMap<String, Vec<String>> {
    let consumers = build_consumer_map(cfg);
    let sink_names: HashSet<&str> = cfg.sinks.iter().map(|s| s.name.as_str()).collect();
    let mut out = HashMap::new();
    for src in &cfg.sources {
        let mut seen = HashSet::new();
        let mut q = VecDeque::new();
        let mut sinks = Vec::new();
        q.push_back(src.name.clone());
        while let Some(node) = q.pop_front() {
            if let Some(cs) = consumers.get(&node) {
                for c in cs {
                    if seen.insert(c.clone()) {
                        if sink_names.contains(c.as_str()) {
                            sinks.push(c.clone());
                        }
                        q.push_back(c.clone());
                    }
                }
            }
        }
        out.insert(src.name.clone(), sinks);
    }
    out
}

async fn source_task(
    name: String,
    source: HyperSyncSource,
    start: u64,
    outs: Vec<Sender<Batch>>,
    stop: Arc<AtomicBool>,
    stat: Arc<SourceStat>,
    kv: Arc<dyn KvStore>,
) {
    // Meter + fan-out via an internal channel between the source loop and consumers.
    let (tx, mut rx) = channel::<Batch>(8);
    let src_stop = stop.clone();
    let runner = tokio::spawn(async move {
        if let Err(e) = source.run(start, tx, src_stop, Some(kv)).await {
            tracing::error!(source = %name, "source error: {e}");
        }
    });
    while let Some(batch) = rx.recv().await {
        stat.records.fetch_add(batch.len() as u64, Ordering::Relaxed);
        stat.batches.fetch_add(1, Ordering::Relaxed);
        for s in &outs {
            if s.send(batch.clone()).await.is_err() {
                stop.store(true, Ordering::Relaxed);
                break;
            }
        }
    }
    let _ = runner.await;
}

async fn processor_task(
    name: String,
    wp: WasmProcessor,
    mut rx: Receiver<Batch>,
    outs: Vec<Sender<Batch>>,
) {
    while let Some(batch) = rx.recv().await {
        let wp2 = wp.clone();
        let result =
            tokio::task::spawn_blocking(move || (wp2.process(&batch), batch)).await;
        let (produced, input) = match result {
            Ok((Ok(v), input)) => (v, input),
            Ok((Err(e), _)) => {
                tracing::error!(processor = %name, "process error: {e}");
                continue;
            }
            Err(e) => {
                tracing::error!(processor = %name, "process task panicked: {e}");
                continue;
            }
        };
        // A processor that drops every record must still propagate an empty
        // batch: downstream sinks ack it, so cursors keep advancing on quiet
        // branches (otherwise a whale-filter with no whales would freeze its
        // sink's cursor and force huge replays on restart).
        let produced = if produced.is_empty() && !input.is_control() {
            vec![input.derive(input.kind, Vec::new())]
        } else {
            produced
        };
        for ob in produced {
            for s in &outs {
                if s.send(ob.clone()).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn sink_task(name: String, sink: SinkImpl, mut rx: Receiver<Batch>, store: Arc<CheckpointStore>) {
    while let Some(batch) = rx.recv().await {
        let source = batch.source.clone();
        // §8.2 rule 1: chunked ranges pin the ack to the range start until the
        // final chunk; ack_block() returns range.to() for ordinary batches.
        let next_block = batch.ack_block();
        // A rollback control batch rewinds the cursor instead of acking, but
        // only AFTER the sink has accepted it (so e.g. the postgres sink has
        // deleted the invalidated rows before the watermark moves back).
        let rewind_to = match batch.as_control() {
            Some(ControlRecord::Rollback {
                invalidate_after_block,
                ..
            }) => Some(invalidate_after_block.saturating_add(1)),
            _ => None,
        };
        let mut attempt = 0;
        loop {
            let sink2 = sink.clone();
            let b2 = batch.clone();
            let res = tokio::task::spawn_blocking(move || sink2.write(&b2)).await;
            match res {
                Ok(Ok(())) => {
                    match rewind_to {
                        Some(to) => {
                            if let Err(e) = store.rewind(&source, &name, to).await {
                                tracing::error!(sink = %name, "cursor rewind failed: {e}; pausing branch");
                                return; // don't keep acking past a failed rewind
                            }
                            tracing::info!(sink = %name, source = %source, rewind_to = to, "reorg rollback applied");
                        }
                        // Ack: this sink has accepted `source` up to next_block.
                        None => store.ack(&source, &name, next_block),
                    }
                    break;
                }
                Ok(Err(e)) => {
                    attempt += 1;
                    if attempt >= 3 {
                        tracing::error!(sink = %name, "write failed after {attempt} attempts: {e}; pausing branch");
                        return;
                    }
                    tracing::warn!(sink = %name, "write error (attempt {attempt}): {e}; retrying");
                    tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
                }
                Err(e) => {
                    tracing::error!(sink = %name, "write task panicked: {e}");
                    return;
                }
            }
        }
    }
}

async fn status_task(stats: Arc<Stats>, stop: Arc<AtomicBool>) {
    let mut last: HashMap<String, u64> = HashMap::new();
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    let start = Instant::now();
    loop {
        tick.tick().await;
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let map = stats.per_source.lock().unwrap();
        for (name, stat) in map.iter() {
            let total = stat.records.load(Ordering::Relaxed);
            let prev = last.get(name).copied().unwrap_or(0);
            let rate = (total - prev) / 5;
            last.insert(name.clone(), total);
            tracing::info!(
                source = %name,
                records = total,
                "{} rec/s | {} batches | uptime {}s",
                rate,
                stat.batches.load(Ordering::Relaxed),
                start.elapsed().as_secs()
            );
        }
    }
}

/// Resolve on SIGINT (ctrl-c) or SIGTERM (Kubernetes pod termination).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Minimal join-all without pulling in the `futures` crate.
async fn futures_join_all(tasks: Vec<tokio::task::JoinHandle<()>>) {
    for t in tasks {
        let _ = t.await;
    }
}

fn build_consumer_map(cfg: &Config) -> HashMap<String, Vec<String>> {
    let mut m: HashMap<String, Vec<String>> = HashMap::new();
    for p in &cfg.processors {
        for inp in &p.inputs {
            m.entry(inp.clone()).or_default().push(p.name.clone());
        }
    }
    for s in &cfg.sinks {
        for inp in &s.inputs {
            m.entry(inp.clone()).or_default().push(s.name.clone());
        }
    }
    let _ = NodeKind::Source; // keep import meaningful
    m
}

// ---------------------------------------------------------------------------
// Module resolution
// ---------------------------------------------------------------------------

fn builtin_wasm_file(name: &str) -> Option<&'static str> {
    Some(match name {
        "evm-abi-decoder" => "evm_abi_decoder.wasm",
        "filter" => "filter.wasm",
        "stdout" => "stdout_sink.wasm",
        "blackhole" => "blackhole_sink.wasm",
        "postgres" => "postgres_sink.wasm",
        "webhook" => "webhook_sink.wasm",
        "s3" => "s3_sink.wasm",
        _ => return None,
    })
}

fn module_dir() -> PathBuf {
    std::env::var_os("HYPERPIPE_MODULE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("modules/target/wasm32-wasip2/debug"))
}

fn resolve_module(m: &ModuleRef, base_dir: &Path, module_dir: &Path) -> Result<Vec<u8>> {
    match m {
        ModuleRef::Builtin(s) => {
            let (name, _major) = m
                .builtin_parts()
                .ok_or_else(|| anyhow!("bad builtin ref `{s}`"))?;
            let file = builtin_wasm_file(&name)
                .ok_or_else(|| anyhow!("unknown builtin module `{name}`"))?;
            let path = module_dir.join(file);
            std::fs::read(&path).with_context(|| {
                format!(
                    "read builtin `{name}` at {}; build modules first (cd modules && cargo build --target wasm32-wasip2) or set HYPERPIPE_MODULE_DIR",
                    path.display()
                )
            })
        }
        ModuleRef::File { file } => {
            let path = base_dir.join(file);
            std::fs::read(&path).with_context(|| format!("read module file {}", path.display()))
        }
    }
}

/// For the ABI decoder, substitute `config.abis[].file` with the file's parsed
/// `abi` contents (modules have no filesystem — §6.3).
fn preprocess_config(module: &ModuleRef, config: &serde_json::Value, base_dir: &Path) -> Result<Vec<u8>> {
    let is_decoder = matches!(module.builtin_parts(), Some((n, _)) if n == "evm-abi-decoder");
    if !is_decoder {
        return Ok(serde_json::to_vec(config).unwrap_or_default());
    }
    let mut cfg = config.clone();
    if let Some(abis) = cfg.get_mut("abis").and_then(|v| v.as_array_mut()) {
        for entry in abis {
            if let Some(file) = entry.get("file").and_then(|v| v.as_str()).map(String::from) {
                let path = base_dir.join(&file);
                let text = std::fs::read_to_string(&path)
                    .with_context(|| format!("read abi file {}", path.display()))?;
                let abi: serde_json::Value =
                    serde_json::from_str(&text).with_context(|| format!("parse abi {file}"))?;
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("abi".into(), abi);
                }
            }
        }
    }
    Ok(serde_json::to_vec(&cfg).unwrap_or_default())
}

fn default_cache_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(|h| PathBuf::from(h).join(".cache/hyperpipe"))
        .unwrap_or_else(|| PathBuf::from(".cache/hyperpipe"))
}
