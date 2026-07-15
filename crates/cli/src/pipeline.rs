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

/// How many times a sink write is retried before the branch pauses, and the
/// base backoff between attempts (attempt N sleeps N * this).
const SINK_MAX_ATTEMPTS: u32 = 3;
const SINK_RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// A sink is either a real WASM module or the built-in native stdout tap
/// (used by `--debug-stdout` so pipelines run without configuring real sinks).
#[derive(Clone)]
enum SinkImpl {
    Wasm(WasmSink),
    Stdout,
    /// Test double: records what it was handed and can be told to fail.
    #[cfg(test)]
    Mock(Arc<tests::MockSink>),
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
            #[cfg(test)]
            SinkImpl::Mock(m) => m.write(batch),
        }
    }
    fn flush(&self) -> Result<()> {
        match self {
            SinkImpl::Wasm(s) => s.flush(),
            SinkImpl::Stdout => Ok(()),
            #[cfg(test)]
            SinkImpl::Mock(m) => m.flush(),
        }
    }
}

/// A processor stage: a real WASM module, or a test double.
#[derive(Clone)]
enum ProcImpl {
    Wasm(WasmProcessor),
    /// Test double: scripted output per call.
    #[cfg(test)]
    Mock(Arc<tests::MockProc>),
}

impl ProcImpl {
    fn process(&self, batch: &Batch) -> Result<Vec<Batch>> {
        match self {
            ProcImpl::Wasm(p) => p.process(batch),
            #[cfg(test)]
            ProcImpl::Mock(m) => m.process(batch),
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
        tasks.push(tokio::spawn(processor_task(name, ProcImpl::Wasm(wp), rx, outs)));
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

/// Send `batch` to every live consumer, dropping any whose receiver is gone.
/// Returns false once no consumers remain.
///
/// A closed receiver means that branch's sink task exited — it paused after
/// exhausting its retries. That branch is done, but the others are not: a
/// producer that gave up here would silently truncate every sibling branch and
/// still let the pipeline report a clean EOF (ARCHITECTURE.md §13: "other sinks
/// keep flowing (independent cursors)").
async fn fan_out(node: &str, live: &mut Vec<Sender<Batch>>, batch: &Batch) -> bool {
    let mut i = 0;
    while i < live.len() {
        if live[i].send(batch.clone()).await.is_err() {
            tracing::warn!(
                node = %node,
                "a downstream branch has stopped consuming; dropping it from the fan-out — \
                 the remaining {} branch(es) continue",
                live.len() - 1
            );
            live.remove(i);
        } else {
            i += 1;
        }
    }
    !live.is_empty()
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
    let runner_name = name.clone();
    let runner = tokio::spawn(async move {
        if let Err(e) = source.run(start, tx, src_stop, Some(kv)).await {
            tracing::error!(source = %runner_name, "source error: {e}");
        }
    });
    let mut live = outs;
    while let Some(batch) = rx.recv().await {
        stat.records.fetch_add(batch.len() as u64, Ordering::Relaxed);
        stat.batches.fetch_add(1, Ordering::Relaxed);
        if !fan_out(&name, &mut live, &batch).await {
            // Only now is there nothing left to feed: stopping the source is
            // what unblocks its loop and lets the process wind down.
            tracing::error!(source = %name, "every downstream branch has stopped; stopping the source");
            stop.store(true, Ordering::Relaxed);
            break;
        }
    }
    let _ = runner.await;
}

async fn processor_task(
    name: String,
    wp: ProcImpl,
    mut rx: Receiver<Batch>,
    outs: Vec<Sender<Batch>>,
) {
    let mut live = outs;
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
            if !fan_out(&name, &mut live, &ob).await {
                return; // nothing downstream is listening any more
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
                    if attempt >= SINK_MAX_ATTEMPTS {
                        tracing::error!(sink = %name, "write failed after {attempt} attempts: {e}; pausing branch");
                        return;
                    }
                    tracing::warn!(sink = %name, "write error (attempt {attempt}): {e}; retrying");
                    tokio::time::sleep(SINK_RETRY_BACKOFF * attempt).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use hp_encoding::{BatchKind, BlockRange};
    use hp_engine::config::EnvSecrets;
    use serde_json::json;

    /// Env vars are process-global: every test that reads or writes one takes
    /// this lock so they can't interleave.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    // ---- test doubles ------------------------------------------------------

    /// A sink that records every batch and fails the first `fail_first` writes.
    #[derive(Default)]
    pub(super) struct MockSink {
        written: Mutex<Vec<Batch>>,
        flushed: AtomicU64,
        fail_first: AtomicU64,
        fail_always: AtomicBool,
        fail_flush: AtomicBool,
    }

    impl MockSink {
        fn new() -> Arc<MockSink> {
            Arc::new(MockSink::default())
        }
        fn failing(n: u64) -> Arc<MockSink> {
            let m = MockSink::new();
            m.fail_first.store(n, Ordering::SeqCst);
            m
        }
        fn always_failing() -> Arc<MockSink> {
            let m = MockSink::new();
            m.fail_always.store(true, Ordering::SeqCst);
            m
        }
        pub(super) fn write(&self, batch: &Batch) -> Result<()> {
            if self.fail_always.load(Ordering::SeqCst) {
                return Err(anyhow!("mock sink is down"));
            }
            let left = self.fail_first.load(Ordering::SeqCst);
            if left > 0 {
                self.fail_first.store(left - 1, Ordering::SeqCst);
                return Err(anyhow!("mock sink transient error"));
            }
            self.written.lock().unwrap().push(batch.clone());
            Ok(())
        }
        pub(super) fn flush(&self) -> Result<()> {
            if self.fail_flush.load(Ordering::SeqCst) {
                return Err(anyhow!("mock flush failed"));
            }
            self.flushed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn write_count(&self) -> usize {
            self.written.lock().unwrap().len()
        }
    }

    /// A processor whose output is scripted, and which can error on demand.
    pub(super) struct MockProc {
        /// What to return for a data batch: None = error, Some(v) = these batches.
        outputs: Mutex<VecDeque<Option<Vec<Batch>>>>,
        seen: Mutex<Vec<String>>,
    }

    impl MockProc {
        fn new(script: Vec<Option<Vec<Batch>>>) -> Arc<MockProc> {
            Arc::new(MockProc {
                outputs: Mutex::new(script.into()),
                seen: Mutex::new(Vec::new()),
            })
        }
        pub(super) fn process(&self, batch: &Batch) -> Result<Vec<Batch>> {
            self.seen.lock().unwrap().push(batch.batch_id.clone());
            match self.outputs.lock().unwrap().pop_front() {
                Some(Some(out)) => Ok(out),
                Some(None) => Err(anyhow!("mock process error")),
                None => Ok(vec![batch.clone()]), // default: pass through
            }
        }
        fn seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }

    // ---- shared fixtures ---------------------------------------------------

    fn data_batch(source: &str, from: u64, to: u64, records: usize) -> Batch {
        Batch::new(
            source,
            1,
            BlockRange(from, to),
            0,
            BatchKind::Log,
            (0..records).map(|i| json!({ "i": i, "block_number": from })).collect(),
        )
    }

    async fn store(name: &str) -> Arc<CheckpointStore> {
        let dir = std::env::temp_dir().join("hp-task-tests");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join(format!("{name}-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        Arc::new(
            CheckpointStore::open(&path.to_string_lossy(), tokio::runtime::Handle::current())
                .await
                .unwrap(),
        )
    }

    fn cursor(store: &CheckpointStore, source: &str, sink: &str) -> Option<u64> {
        store.restore(source, &[sink.to_string()])
    }

    fn cfg(yaml: &str) -> Config {
        Config::load_str(yaml, &EnvSecrets).expect("test config should be valid")
    }

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("hp-cli-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// source -> decode -> out
    const LINEAR: &str = r#"
name: linear
sources:
  - name: src
    chain: ethereum
    query: {}
processors:
  - name: decode
    module: builtin/evm-abi-decoder@1
    inputs: [src]
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [decode]
"#;

    // ---- build_consumer_map ------------------------------------------------

    #[test]
    fn consumer_map_of_a_linear_chain() {
        let m = build_consumer_map(&cfg(LINEAR));
        assert_eq!(m.get("src"), Some(&vec!["decode".to_string()]));
        assert_eq!(m.get("decode"), Some(&vec!["out".to_string()]));
        assert_eq!(m.get("out"), None, "a sink consumes but is consumed by nothing");
    }

    #[test]
    fn consumer_map_of_a_fan_out() {
        // one producer -> two consumers
        let yaml = r#"
name: fanout
sources:
  - name: src
    chain: ethereum
    query: {}
sinks:
  - name: a
    module: builtin/stdout@1
    inputs: [src]
  - name: b
    module: builtin/blackhole@1
    inputs: [src]
"#;
        let m = build_consumer_map(&cfg(yaml));
        assert_eq!(m.get("src"), Some(&vec!["a".to_string(), "b".to_string()]));
    }

    #[test]
    fn consumer_map_of_a_fan_in() {
        // two producers -> one consumer
        let yaml = r#"
name: fanin
sources:
  - name: eth
    chain: ethereum
    query: {}
  - name: base
    chain: base
    query: {}
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [eth, base]
"#;
        let m = build_consumer_map(&cfg(yaml));
        assert_eq!(m.get("eth"), Some(&vec!["out".to_string()]));
        assert_eq!(m.get("base"), Some(&vec!["out".to_string()]));
    }

    // ---- reachable_sinks_per_source ----------------------------------------

    #[test]
    fn reachable_sinks_over_a_diamond_lists_the_shared_sink_once() {
        // src -> {left, right} -> out. `out` is reachable by two paths; listing
        // it twice would make restore() compare the same cursor to itself and
        // hide a genuinely lagging sink.
        let yaml = r#"
name: diamond
sources:
  - name: src
    chain: ethereum
    query: {}
processors:
  - name: left
    module: builtin/filter@1
    inputs: [src]
  - name: right
    module: builtin/filter@1
    inputs: [src]
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [left, right]
"#;
        let r = reachable_sinks_per_source(&cfg(yaml));
        assert_eq!(r.get("src"), Some(&vec!["out".to_string()]));
    }

    #[test]
    fn reachable_sinks_follows_a_multi_hop_chain() {
        let r = reachable_sinks_per_source(&cfg(LINEAR));
        assert_eq!(r.get("src"), Some(&vec!["out".to_string()]));
    }

    #[test]
    fn reachable_sinks_are_per_source_when_branches_are_disjoint() {
        let yaml = r#"
name: disjoint
sources:
  - name: eth
    chain: ethereum
    query: {}
  - name: base
    chain: base
    query: {}
sinks:
  - name: eth_out
    module: builtin/stdout@1
    inputs: [eth]
  - name: base_out
    module: builtin/blackhole@1
    inputs: [base]
"#;
        let r = reachable_sinks_per_source(&cfg(yaml));
        // Each source's restart cursor depends only on its own sink.
        assert_eq!(r.get("eth"), Some(&vec!["eth_out".to_string()]));
        assert_eq!(r.get("base"), Some(&vec!["base_out".to_string()]));
    }

    #[test]
    fn a_source_reaching_both_sinks_lists_both() {
        let yaml = r#"
name: shared
sources:
  - name: eth
    chain: ethereum
    query: {}
sinks:
  - name: pg
    module: builtin/stdout@1
    inputs: [eth]
  - name: lake
    module: builtin/blackhole@1
    inputs: [eth]
"#;
        let r = reachable_sinks_per_source(&cfg(yaml));
        let mut sinks = r.get("eth").cloned().unwrap();
        sinks.sort();
        assert_eq!(sinks, vec!["lake".to_string(), "pg".to_string()]);
    }

    // ---- module resolution -------------------------------------------------

    #[test]
    fn every_builtin_name_maps_to_a_wasm_file() {
        let all = [
            ("evm-abi-decoder", "evm_abi_decoder.wasm"),
            ("filter", "filter.wasm"),
            ("stdout", "stdout_sink.wasm"),
            ("blackhole", "blackhole_sink.wasm"),
            ("postgres", "postgres_sink.wasm"),
            ("webhook", "webhook_sink.wasm"),
            ("s3", "s3_sink.wasm"),
        ];
        for (name, file) in all {
            assert_eq!(builtin_wasm_file(name), Some(file));
        }
        assert_eq!(builtin_wasm_file("kafka"), None);
        assert_eq!(builtin_wasm_file(""), None);
    }

    #[test]
    fn module_dir_prefers_the_env_override() {
        let _g = ENV_LOCK.lock().unwrap();
        let saved = std::env::var_os("HYPERPIPE_MODULE_DIR");

        std::env::set_var("HYPERPIPE_MODULE_DIR", "/opt/modules");
        assert_eq!(module_dir(), PathBuf::from("/opt/modules"));

        std::env::remove_var("HYPERPIPE_MODULE_DIR");
        assert_eq!(module_dir(), PathBuf::from("modules/target/wasm32-wasip2/debug"));

        if let Some(v) = saved {
            std::env::set_var("HYPERPIPE_MODULE_DIR", v);
        }
    }

    #[test]
    fn resolve_builtin_reads_from_the_module_dir() {
        let dir = tmpdir("resolve-builtin");
        std::fs::write(dir.join("filter.wasm"), b"\0asm-not-really").unwrap();
        let m = ModuleRef::Builtin("builtin/filter@1".into());
        let bytes = resolve_module(&m, Path::new("."), &dir).unwrap();
        assert_eq!(bytes, b"\0asm-not-really");
    }

    #[test]
    fn resolve_unknown_builtin_names_the_module() {
        let dir = tmpdir("resolve-unknown");
        let m = ModuleRef::Builtin("builtin/kafka@1".into());
        let e = resolve_module(&m, Path::new("."), &dir).unwrap_err();
        assert!(format!("{e:#}").contains("unknown builtin module `kafka`"), "got {e:#}");
    }

    #[test]
    fn resolve_malformed_builtin_ref() {
        let dir = tmpdir("resolve-malformed");
        // Validation normally catches this; resolve_module must not panic on it.
        let m = ModuleRef::Builtin("builtin/filter".into());
        let e = resolve_module(&m, Path::new("."), &dir).unwrap_err();
        assert!(format!("{e:#}").contains("bad builtin ref"), "got {e:#}");
    }

    #[test]
    fn missing_builtin_points_at_the_build_step() {
        // The most common first-run failure: the error must say how to fix it.
        let dir = tmpdir("resolve-missing");
        let m = ModuleRef::Builtin("builtin/filter@1".into());
        let e = resolve_module(&m, Path::new("."), &dir).unwrap_err();
        let msg = format!("{e:#}");
        assert!(msg.contains("HYPERPIPE_MODULE_DIR"), "got {msg}");
        assert!(msg.contains("cargo build --target wasm32-wasip2"), "got {msg}");
        assert!(msg.contains("filter.wasm"), "the resolved path should be shown: {msg}");
    }

    #[test]
    fn file_refs_resolve_relative_to_the_config_dir() {
        // `{file: ./enrich.wasm}` is relative to the YAML, not to $PWD.
        let base = tmpdir("resolve-file");
        std::fs::create_dir_all(base.join("modules")).unwrap();
        std::fs::write(base.join("modules/enrich.wasm"), b"custom").unwrap();
        let m = ModuleRef::File { file: PathBuf::from("./modules/enrich.wasm") };
        assert_eq!(resolve_module(&m, &base, Path::new("/nonexistent")).unwrap(), b"custom");

        let missing = ModuleRef::File { file: PathBuf::from("./nope.wasm") };
        let e = resolve_module(&missing, &base, Path::new("/nonexistent")).unwrap_err();
        assert!(format!("{e:#}").contains("read module file"), "got {e:#}");
    }

    // ---- preprocess_config -------------------------------------------------

    fn abi_json() -> serde_json::Value {
        json!([{ "type": "event", "name": "Transfer", "anonymous": false, "inputs": [] }])
    }

    #[test]
    fn decoder_abi_files_are_inlined() {
        // Modules have no filesystem (§6.3), so the engine must read the ABI and
        // hand the module its contents.
        let base = tmpdir("preprocess-inline");
        std::fs::write(base.join("erc20.json"), abi_json().to_string()).unwrap();

        let m = ModuleRef::Builtin("builtin/evm-abi-decoder@1".into());
        let config = json!({ "abis": [{ "file": "erc20.json", "events": ["Transfer"] }] });
        let out = preprocess_config(&m, &config, &base).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();

        assert_eq!(parsed["abis"][0]["abi"], abi_json());
        assert_eq!(parsed["abis"][0]["file"], "erc20.json", "the file key is left in place");
        assert_eq!(parsed["abis"][0]["events"], json!(["Transfer"]));
    }

    #[test]
    fn non_decoder_configs_pass_through_untouched() {
        let base = tmpdir("preprocess-passthrough");
        let config = json!({ "abis": [{ "file": "does-not-exist.json" }], "other": 1 });
        // A filter module happens to have an `abis` key: no file read, no error.
        let m = ModuleRef::Builtin("builtin/filter@1".into());
        let out = preprocess_config(&m, &config, &base).unwrap();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&out).unwrap(), config);

        // Same for a custom file-ref module.
        let m = ModuleRef::File { file: PathBuf::from("enrich.wasm") };
        let out = preprocess_config(&m, &config, &base).unwrap();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&out).unwrap(), config);
    }

    #[test]
    fn a_missing_abi_file_names_the_path() {
        let base = tmpdir("preprocess-missing");
        let m = ModuleRef::Builtin("builtin/evm-abi-decoder@1".into());
        let config = json!({ "abis": [{ "file": "gone.json" }] });
        let e = preprocess_config(&m, &config, &base).unwrap_err();
        let msg = format!("{e:#}");
        assert!(msg.contains("read abi file"), "got {msg}");
        assert!(msg.contains("gone.json"), "got {msg}");
    }

    #[test]
    fn a_malformed_abi_file_is_rejected_at_load_time() {
        // Better here than as an opaque module init error at runtime.
        let base = tmpdir("preprocess-malformed");
        std::fs::write(base.join("bad.json"), "{ not json").unwrap();
        let m = ModuleRef::Builtin("builtin/evm-abi-decoder@1".into());
        let config = json!({ "abis": [{ "file": "bad.json" }] });
        let e = preprocess_config(&m, &config, &base).unwrap_err();
        assert!(format!("{e:#}").contains("parse abi bad.json"), "got {e:#}");
    }

    #[test]
    fn decoder_entries_that_already_carry_an_abi_are_untouched() {
        let base = tmpdir("preprocess-inline-abi");
        let m = ModuleRef::Builtin("builtin/evm-abi-decoder@1".into());
        let config = json!({ "abis": [{ "abi": abi_json() }], "on_undecodable": "drop" });
        let out = preprocess_config(&m, &config, &base).unwrap();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&out).unwrap(), config);
    }

    #[test]
    fn decoder_config_without_abis_is_left_alone() {
        let base = tmpdir("preprocess-no-abis");
        let m = ModuleRef::Builtin("builtin/evm-abi-decoder@1".into());
        for config in [json!({}), json!({ "abis": "not-an-array" }), serde_json::Value::Null] {
            let out = preprocess_config(&m, &config, &base).unwrap();
            assert_eq!(serde_json::from_slice::<serde_json::Value>(&out).unwrap(), config);
        }
    }

    // ---- misc --------------------------------------------------------------

    #[test]
    fn cache_dir_falls_back_when_home_is_unset() {
        let _g = ENV_LOCK.lock().unwrap();
        let saved = std::env::var_os("HOME");

        std::env::set_var("HOME", "/home/someone");
        assert_eq!(default_cache_dir(), PathBuf::from("/home/someone/.cache/hyperpipe"));

        std::env::remove_var("HOME");
        assert_eq!(default_cache_dir(), PathBuf::from(".cache/hyperpipe"));

        if let Some(v) = saved {
            std::env::set_var("HOME", v);
        }
    }

    // ---- sink_task ---------------------------------------------------------

    #[tokio::test]
    async fn sink_task_acks_the_batchs_ack_block_not_its_range_end() {
        // §8.2 rule 1: a non-final chunk pins the ack to the range start, so a
        // crash between chunk acks replays the whole range instead of losing it.
        let st = store("sink-ack").await;
        let mock = MockSink::new();
        let (tx, rx) = channel(8);

        let mut chunk0 = data_batch("eth", 100, 200, 2);
        chunk0.ack_block = Some(100);
        tx.send(chunk0).await.unwrap();
        let chunk1 = data_batch("eth", 100, 200, 1); // final chunk: no pin
        tx.send(chunk1).await.unwrap();
        drop(tx);

        sink_task("out".into(), SinkImpl::Mock(mock.clone()), rx, st.clone()).await;

        assert_eq!(mock.write_count(), 2);
        assert_eq!(cursor(&st, "eth", "out"), Some(200), "the final chunk advanced it");
    }

    #[tokio::test]
    async fn sink_task_stops_the_cursor_at_a_pinned_chunk() {
        let st = store("sink-ack-pinned").await;
        let mock = MockSink::new();
        let (tx, rx) = channel(8);
        let mut chunk0 = data_batch("eth", 100, 200, 2);
        chunk0.ack_block = Some(100);
        tx.send(chunk0).await.unwrap();
        drop(tx); // "crash" before the final chunk

        sink_task("out".into(), SinkImpl::Mock(mock.clone()), rx, st.clone()).await;
        assert_eq!(cursor(&st, "eth", "out"), Some(100), "must replay from the range start");
    }

    #[tokio::test]
    async fn sink_task_rewinds_the_cursor_only_after_the_sink_took_the_rollback() {
        // The sink must have deleted the invalidated rows before the watermark
        // moves back — otherwise a crash in between leaves forked rows behind
        // a cursor that never revisits them.
        let st = store("sink-rollback").await;
        st.ack("eth", "out", 500);
        st.persist_snapshot(st.snapshot()).await.unwrap();

        let mock = MockSink::new();
        let (tx, rx) = channel(8);
        tx.send(Batch::control(
            "eth",
            1,
            ControlRecord::Rollback { chain_id: 1, invalidate_after_block: 99 },
        ))
        .await
        .unwrap();
        drop(tx);

        sink_task("out".into(), SinkImpl::Mock(mock.clone()), rx, st.clone()).await;

        assert_eq!(mock.write_count(), 1, "the control batch reached the sink");
        assert!(mock.written.lock().unwrap()[0].is_control());
        assert_eq!(cursor(&st, "eth", "out"), Some(100), "rewound to fork + 1");
    }

    #[tokio::test]
    async fn sink_task_does_not_rewind_when_the_sink_rejects_the_rollback() {
        let st = store("sink-rollback-fail").await;
        st.ack("eth", "out", 500);
        let mock = MockSink::always_failing();
        let (tx, rx) = channel(8);
        tx.send(Batch::control(
            "eth",
            1,
            ControlRecord::Rollback { chain_id: 1, invalidate_after_block: 99 },
        ))
        .await
        .unwrap();
        drop(tx);

        tokio::time::pause();
        sink_task("out".into(), SinkImpl::Mock(mock), rx, st.clone()).await;
        assert_eq!(
            cursor(&st, "eth", "out"),
            Some(500),
            "a sink that could not delete the forked rows must not move the watermark"
        );
    }

    #[tokio::test]
    async fn sink_task_retries_a_failing_write_then_acks() {
        let st = store("sink-retry").await;
        let mock = MockSink::failing(2); // fails twice, then succeeds
        let (tx, rx) = channel(8);
        tx.send(data_batch("eth", 100, 200, 1)).await.unwrap();
        drop(tx);

        // Skip the real backoff. (Paused from here, not from the top: the
        // checkpoint store's pool would auto-advance straight into its own
        // connect timeout.)
        tokio::time::pause();
        sink_task("out".into(), SinkImpl::Mock(mock.clone()), rx, st.clone()).await;

        assert_eq!(mock.write_count(), 1, "the third attempt landed");
        assert_eq!(cursor(&st, "eth", "out"), Some(200));
    }

    #[tokio::test]
    async fn sink_task_pauses_the_branch_after_the_attempt_limit() {
        // A permanently broken sink must NOT ack — the cursor stays put so a
        // restart replays, and the rest of the pipeline keeps running.
        let st = store("sink-give-up").await;
        let mock = MockSink::always_failing();
        let (tx, rx) = channel(8);
        tx.send(data_batch("eth", 100, 200, 1)).await.unwrap();
        tx.send(data_batch("eth", 200, 300, 1)).await.unwrap();
        drop(tx);

        tokio::time::pause();
        sink_task("out".into(), SinkImpl::Mock(mock.clone()), rx, st.clone()).await;

        assert_eq!(mock.write_count(), 0);
        assert_eq!(cursor(&st, "eth", "out"), None, "nothing was ever acked");
    }

    #[tokio::test]
    async fn sink_task_tracks_a_cursor_per_source() {
        // Fan-in: one sink fed by two chains keeps independent watermarks.
        let st = store("sink-multisource").await;
        let mock = MockSink::new();
        let (tx, rx) = channel(8);
        tx.send(data_batch("eth", 100, 200, 1)).await.unwrap();
        tx.send(data_batch("base", 500, 600, 1)).await.unwrap();
        drop(tx);

        sink_task("out".into(), SinkImpl::Mock(mock), rx, st.clone()).await;
        assert_eq!(cursor(&st, "eth", "out"), Some(200));
        assert_eq!(cursor(&st, "base", "out"), Some(600));
    }

    #[tokio::test]
    async fn stdout_sink_prints_records_and_skips_control_batches() {
        let sink = SinkImpl::Stdout;
        sink.write(&data_batch("eth", 100, 200, 2)).unwrap();
        sink.write(&Batch::control("eth", 1, ControlRecord::Eof { source: "eth".into(), at_block: 200 }))
            .unwrap();
        sink.flush().unwrap();
    }

    // ---- processor_task ----------------------------------------------------

    #[tokio::test]
    async fn processor_task_propagates_an_empty_batch_when_everything_is_filtered() {
        // The quiet-branch rule: a whale filter with no whales must still let
        // its sink ack, or the cursor freezes and restarts replay everything.
        let proc = MockProc::new(vec![Some(vec![])]);
        let (in_tx, in_rx) = channel(8);
        let (out_tx, mut out_rx) = channel(8);

        in_tx.send(data_batch("eth", 100, 200, 3)).await.unwrap();
        drop(in_tx);
        processor_task("filter".into(), ProcImpl::Mock(proc), in_rx, vec![out_tx]).await;

        let out = out_rx.recv().await.expect("an empty batch must still be forwarded");
        assert!(out.is_empty(), "no records survived");
        assert_eq!(out.block_range, BlockRange(100, 200), "but the range is intact");
        assert_eq!(out.ack_block(), 200, "so the sink can ack it");
        assert_eq!(out.batch_id, "eth:100:200:0", "identity preserved");
    }

    #[tokio::test]
    async fn processor_task_preserves_the_ack_pin_of_an_emptied_chunk() {
        let proc = MockProc::new(vec![Some(vec![])]);
        let (in_tx, in_rx) = channel(8);
        let (out_tx, mut out_rx) = channel(8);

        let mut chunk = data_batch("eth", 100, 200, 3);
        chunk.ack_block = Some(100); // non-final chunk
        in_tx.send(chunk).await.unwrap();
        drop(in_tx);
        processor_task("filter".into(), ProcImpl::Mock(proc), in_rx, vec![out_tx]).await;

        let out = out_rx.recv().await.unwrap();
        assert_eq!(out.ack_block, Some(100), "an emptied chunk must not un-pin the ack");
    }

    #[tokio::test]
    async fn processor_task_synthesizes_nothing_for_an_empty_control_output() {
        // A module that swallows a control batch must not have an empty control
        // batch invented on its behalf.
        let proc = MockProc::new(vec![Some(vec![])]);
        let (in_tx, in_rx) = channel(8);
        let (out_tx, mut out_rx) = channel(8);

        in_tx
            .send(Batch::control("eth", 1, ControlRecord::Eof { source: "eth".into(), at_block: 200 }))
            .await
            .unwrap();
        drop(in_tx);
        processor_task("p".into(), ProcImpl::Mock(proc), in_rx, vec![out_tx]).await;

        assert!(out_rx.recv().await.is_none(), "nothing forwarded");
    }

    #[tokio::test]
    async fn processor_task_drops_a_failed_batch_and_keeps_going() {
        // One bad batch must not take the stage down.
        let proc = MockProc::new(vec![None, Some(vec![data_batch("eth", 200, 300, 1)])]);
        let (in_tx, in_rx) = channel(8);
        let (out_tx, mut out_rx) = channel(8);

        in_tx.send(data_batch("eth", 100, 200, 1)).await.unwrap();
        in_tx.send(data_batch("eth", 200, 300, 1)).await.unwrap();
        drop(in_tx);
        processor_task("p".into(), ProcImpl::Mock(proc.clone()), in_rx, vec![out_tx]).await;

        let out = out_rx.recv().await.expect("the second batch still flows");
        assert_eq!(out.block_range, BlockRange(200, 300));
        assert!(out_rx.recv().await.is_none(), "the failed batch produced nothing");
        assert_eq!(proc.seen().len(), 2, "both batches were attempted");
    }

    #[tokio::test]
    async fn processor_task_fans_out_every_output_to_every_consumer() {
        let split = vec![data_batch("eth", 100, 150, 1), data_batch("eth", 150, 200, 1)];
        let proc = MockProc::new(vec![Some(split)]);
        let (in_tx, in_rx) = channel(8);
        let (a_tx, mut a_rx) = channel(8);
        let (b_tx, mut b_rx) = channel(8);

        in_tx.send(data_batch("eth", 100, 200, 2)).await.unwrap();
        drop(in_tx);
        processor_task("p".into(), ProcImpl::Mock(proc), in_rx, vec![a_tx, b_tx]).await;

        for rx in [&mut a_rx, &mut b_rx] {
            assert_eq!(rx.recv().await.unwrap().block_range, BlockRange(100, 150));
            assert_eq!(rx.recv().await.unwrap().block_range, BlockRange(150, 200));
        }
    }

    #[tokio::test]
    async fn processor_task_returns_when_its_only_consumer_goes_away() {
        let proc = MockProc::new(vec![]);
        let (in_tx, in_rx) = channel(8);
        let (out_tx, out_rx) = channel(1);
        drop(out_rx);

        in_tx.send(data_batch("eth", 100, 200, 1)).await.unwrap();
        // The task must return rather than block forever on a dead channel.
        tokio::time::timeout(
            Duration::from_secs(5),
            processor_task("p".into(), ProcImpl::Mock(proc), in_rx, vec![out_tx]),
        )
        .await
        .expect("must not hang");
    }

    // ---- fan_out: one paused branch must not take the others down -----------

    #[tokio::test]
    async fn fan_out_delivers_to_every_live_consumer() {
        let (a_tx, mut a_rx) = channel(4);
        let (b_tx, mut b_rx) = channel(4);
        let mut live = vec![a_tx, b_tx];

        assert!(fan_out("p", &mut live, &data_batch("eth", 100, 200, 1)).await);
        assert_eq!(live.len(), 2);
        assert_eq!(a_rx.recv().await.unwrap().block_range, BlockRange(100, 200));
        assert_eq!(b_rx.recv().await.unwrap().block_range, BlockRange(100, 200));
    }

    #[tokio::test]
    async fn fan_out_drops_a_dead_consumer_and_keeps_feeding_the_rest() {
        // This is the regression: a sink that paused (its task returned, closing
        // its receiver) used to make the producer give up, silently truncating
        // every sibling branch while the pipeline still reported a clean EOF.
        let (dead_tx, dead_rx) = channel(4);
        let (live_tx, mut live_rx) = channel(4);
        drop(dead_rx); // the webhook branch paused after its retries
        let mut live = vec![dead_tx, live_tx];

        assert!(
            fan_out("decode", &mut live, &data_batch("eth", 100, 200, 1)).await,
            "one dead consumer must not end the fan-out"
        );
        assert_eq!(live.len(), 1, "the dead consumer is dropped, not retried forever");
        assert_eq!(live_rx.recv().await.unwrap().block_range, BlockRange(100, 200));

        // ...and the survivor keeps receiving on later batches too.
        assert!(fan_out("decode", &mut live, &data_batch("eth", 200, 300, 1)).await);
        assert_eq!(live_rx.recv().await.unwrap().block_range, BlockRange(200, 300));
    }

    #[tokio::test]
    async fn fan_out_reports_when_the_last_consumer_is_gone() {
        let (a_tx, a_rx) = channel(4);
        let (b_tx, b_rx) = channel(4);
        drop(a_rx);
        drop(b_rx);
        let mut live = vec![a_tx, b_tx];

        assert!(
            !fan_out("decode", &mut live, &data_batch("eth", 100, 200, 1)).await,
            "with nothing left downstream the producer should stop"
        );
        assert!(live.is_empty());
    }

    #[tokio::test]
    async fn processor_task_keeps_serving_the_healthy_branch_when_one_pauses() {
        // The same regression, at the task level: pg keeps getting batches while
        // the webhook branch is gone.
        let proc = MockProc::new(vec![]); // default: pass the batch through
        let (in_tx, in_rx) = channel(8);
        let (paused_tx, paused_rx) = channel(1);
        let (healthy_tx, mut healthy_rx) = channel(8);
        drop(paused_rx);

        in_tx.send(data_batch("eth", 100, 200, 1)).await.unwrap();
        in_tx.send(data_batch("eth", 200, 300, 1)).await.unwrap();
        drop(in_tx);

        tokio::time::timeout(
            Duration::from_secs(5),
            processor_task("decode".into(), ProcImpl::Mock(proc), in_rx, vec![paused_tx, healthy_tx]),
        )
        .await
        .expect("must not hang");

        assert_eq!(healthy_rx.recv().await.unwrap().block_range, BlockRange(100, 200));
        assert_eq!(
            healthy_rx.recv().await.unwrap().block_range,
            BlockRange(200, 300),
            "the healthy branch must receive the whole range, not stop at the pause"
        );
    }

    // ---- source_task -------------------------------------------------------

    #[tokio::test]
    async fn checkpoint_once_persists_only_after_every_sink_flushed() {
        let st = store("ckpt-ok").await;
        st.ack("eth", "out", 200);
        let mock = MockSink::new();
        checkpoint_once(&st, &[SinkImpl::Mock(mock.clone())]).await;
        assert_eq!(mock.flushed.load(Ordering::SeqCst), 1);

        // Reopen: the cursor is durable.
        drop(st);
        let dir = std::env::temp_dir().join("hp-task-tests");
        let path = dir.join(format!("ckpt-ok-{}.db", std::process::id()));
        let st2 = CheckpointStore::open(&path.to_string_lossy(), tokio::runtime::Handle::current())
            .await
            .unwrap();
        assert_eq!(st2.restore("eth", &["out".to_string()]), Some(200));
    }

    #[tokio::test]
    async fn checkpoint_once_does_not_advance_cursors_when_a_flush_fails() {
        // §8.2 rule 5: a cursor may only advance past data that is durable.
        let st = store("ckpt-flush-fail").await;
        st.ack("eth", "out", 200);
        let mock = MockSink::new();
        mock.fail_flush.store(true, Ordering::SeqCst);

        checkpoint_once(&st, &[SinkImpl::Mock(mock)]).await;

        drop(st);
        let dir = std::env::temp_dir().join("hp-task-tests");
        let path = dir.join(format!("ckpt-flush-fail-{}.db", std::process::id()));
        let st2 = CheckpointStore::open(&path.to_string_lossy(), tokio::runtime::Handle::current())
            .await
            .unwrap();
        assert_eq!(
            st2.restore("eth", &["out".to_string()]),
            None,
            "the un-flushed watermark must not have been written"
        );
    }

    #[tokio::test]
    async fn checkpoint_once_with_no_sinks_is_harmless() {
        let st = store("ckpt-nosinks").await;
        checkpoint_once(&st, &[]).await;
    }
}
