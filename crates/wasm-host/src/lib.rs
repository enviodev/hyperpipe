//! wasmtime embedding for HyperPipe modules (§6).
//!
//! [`Runtime`] owns the wasmtime `Engine`, the shared linker, host services,
//! and the epoch ticker. It loads components into [`WasmProcessor`] /
//! [`WasmSink`] handles that the engine drives with decoded [`hp_encoding::Batch`]
//! values. Each handle keeps a small pool of pre-initialized instances.

mod host_impl;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context, Result};
use hp_encoding::{Batch, Encoding};
use wasmtime::component::{Component, Linker};
use wasmtime::{Engine, Store, StoreLimitsBuilder};

pub use host_impl::{BlobConn, HostServices, KvStore, MemKv, Metrics};

/// Config for an object-store connection (resolved into a [`BlobConn`]).
#[derive(Clone, Default)]
pub struct BlobConnCfg {
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub prefix: Option<String>,
    pub local_path: Option<String>,
}

fn build_blob(cfg: &BlobConnCfg) -> Result<BlobConn> {
    let prefix = cfg.prefix.clone().unwrap_or_default();
    if let Some(dir) = &cfg.local_path {
        std::fs::create_dir_all(dir).ok();
        let store = object_store::local::LocalFileSystem::new_with_prefix(dir)
            .context("local object store")?;
        Ok(BlobConn {
            store: std::sync::Arc::new(store),
            prefix,
        })
    } else {
        let bucket = cfg
            .bucket
            .clone()
            .ok_or_else(|| anyhow!("s3 connection needs `bucket` or `local_path`"))?;
        let mut b = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
        if let Some(r) = &cfg.region {
            b = b.with_region(r);
        }
        if let Some(e) = &cfg.endpoint {
            b = b.with_endpoint(e).with_allow_http(true);
        }
        let store = b.build().context("s3 object store")?;
        Ok(BlobConn {
            store: std::sync::Arc::new(store),
            prefix,
        })
    }
}
use host_impl::{proc_bindings, sink_bindings, types_iface, HostState};

/// Capability + identity context for one pipeline node.
#[derive(Clone)]
pub struct NodeCtx {
    pub module_name: String,
    pub pipeline_name: String,
    /// The YAML `config:` block as JSON bytes.
    pub config_json: Vec<u8>,
    pub http_allow: Vec<String>,
    pub granted_conns: Vec<String>,
}

/// Runtime-wide knobs derived from `resource_size` (§11.1).
#[derive(Clone)]
pub struct RuntimeConfig {
    pub cache_dir: PathBuf,
    pub wasm_mem_bytes: usize,
    pub epoch_deadline_secs: u64,
    pub instances_per_stage: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            cache_dir: default_cache_dir(),
            wasm_mem_bytes: 256 << 20,
            epoch_deadline_secs: 5,
            instances_per_stage: 2,
        }
    }
}

fn default_cache_dir() -> PathBuf {
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .unwrap_or_else(|| PathBuf::from(".cache"));
    base.join("hyperpipe")
}

/// Shared wasmtime engine + host services + epoch ticker.
pub struct Runtime {
    engine: Engine,
    linker: Arc<Linker<HostState>>,
    services: Arc<HostServices>,
    cfg: RuntimeConfig,
    epoch_stop: Arc<AtomicBool>,
    epoch_thread: Option<std::thread::JoinHandle<()>>,
}

impl Runtime {
    pub fn new(cfg: RuntimeConfig, services: Arc<HostServices>) -> Result<Self> {
        let mut wc = wasmtime::Config::new();
        wc.wasm_component_model(true);
        wc.epoch_interruption(true);
        let engine = Engine::new(&wc).context("create wasmtime engine")?;

        let mut linker: Linker<HostState> = Linker::new(&engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker).context("link wasi")?;
        proc_bindings::envio::hyperpipe::host::add_to_linker(&mut linker, |s: &mut HostState| s)
            .context("link host imports")?;

        std::fs::create_dir_all(&cfg.cache_dir).ok();

        // Epoch ticker: one increment per second drives per-call deadlines.
        let epoch_stop = Arc::new(AtomicBool::new(false));
        let engine_for_tick = engine.clone();
        let stop_for_tick = epoch_stop.clone();
        let epoch_thread = std::thread::Builder::new()
            .name("hp-epoch".into())
            .spawn(move || {
                while !stop_for_tick.load(Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    engine_for_tick.increment_epoch();
                }
            })
            .ok();

        Ok(Runtime {
            engine,
            linker: Arc::new(linker),
            services,
            cfg,
            epoch_stop,
            epoch_thread,
        })
    }

    pub fn services(&self) -> &Arc<HostServices> {
        &self.services
    }

    fn compile(&self, wasm: &[u8]) -> Result<Component> {
        let key = sha256_hex(wasm);
        let path = self.cfg.cache_dir.join(format!("{key}.cwasm"));
        if path.exists() {
            // SAFETY: cache dir is written only by us with matching engine config.
            if let Ok(c) = unsafe { Component::deserialize_file(&self.engine, &path) } {
                return Ok(c);
            }
        }
        let serialized = self
            .engine
            .precompile_component(wasm)
            .context("precompile component")?;
        std::fs::write(&path, &serialized).ok();
        // SAFETY: freshly produced by this engine.
        let component = unsafe { Component::deserialize(&self.engine, &serialized)? };
        Ok(component)
    }

    fn new_store(&self, node: &NodeCtx) -> Store<HostState> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.cfg.wasm_mem_bytes)
            .build();
        let state = HostState::new(
            node.module_name.clone(),
            node.http_allow.clone(),
            node.granted_conns.clone(),
            self.services.clone(),
            limits,
        );
        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| &mut s.limits);
        store.set_epoch_deadline(self.cfg.epoch_deadline_secs);
        store
    }

    /// Load + init a processor module. Pre-creates `instances_per_stage` ready
    /// instances; init errors surface here (used by the `validate` dry-run).
    pub fn load_processor(&self, wasm: &[u8], node: NodeCtx) -> Result<WasmProcessor> {
        let component = self.compile(wasm)?;
        let node = Arc::new(node);
        let mut pool = Vec::with_capacity(self.cfg.instances_per_stage);
        for _ in 0..self.cfg.instances_per_stage.max(1) {
            pool.push(self.instantiate_processor(&component, &node)?);
        }
        Ok(WasmProcessor {
            component,
            node,
            engine_deadline: self.cfg.epoch_deadline_secs,
            inner: Arc::new(ProcInner {
                linker: self.linker.clone(),
                engine: self.engine.clone(),
                services: self.services.clone(),
                mem_bytes: self.cfg.wasm_mem_bytes,
                pool: Mutex::new(pool),
            }),
        })
    }

    fn instantiate_processor(
        &self,
        component: &Component,
        node: &NodeCtx,
    ) -> Result<ProcSlot> {
        let mut store = self.new_store(node);
        let bindings = proc_bindings::Processor::instantiate(&mut store, component, &self.linker)
            .context("instantiate processor")?;
        let ctx = init_ctx(node);
        store.set_epoch_deadline(self.cfg.epoch_deadline_secs);
        bindings
            .call_init(&mut store, &ctx)
            .context("call init")?
            .map_err(|e| anyhow::anyhow!("module init: {e}"))?;
        Ok(ProcSlot { store, bindings })
    }

    /// Load + init a sink module.
    pub fn load_sink(&self, wasm: &[u8], node: NodeCtx) -> Result<WasmSink> {
        let component = self.compile(wasm)?;
        let node = Arc::new(node);
        let mut pool = Vec::with_capacity(self.cfg.instances_per_stage);
        for _ in 0..self.cfg.instances_per_stage.max(1) {
            pool.push(self.instantiate_sink(&component, &node)?);
        }
        Ok(WasmSink {
            component,
            node,
            deadline: self.cfg.epoch_deadline_secs,
            inner: Arc::new(SinkInner {
                linker: self.linker.clone(),
                engine: self.engine.clone(),
                mem_bytes: self.cfg.wasm_mem_bytes,
                services: self.services.clone(),
                pool: Mutex::new(pool),
            }),
        })
    }

    fn instantiate_sink(&self, component: &Component, node: &NodeCtx) -> Result<SinkSlot> {
        let mut store = self.new_store(node);
        let bindings = sink_bindings::Sink::instantiate(&mut store, component, &self.linker)
            .context("instantiate sink")?;
        let ctx = init_ctx(node);
        store.set_epoch_deadline(self.cfg.epoch_deadline_secs);
        bindings
            .call_init(&mut store, &ctx)
            .context("call init")?
            .map_err(|e| anyhow::anyhow!("module init: {e}"))?;
        Ok(SinkSlot { store, bindings })
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.epoch_stop.store(true, Ordering::Relaxed);
        // Nudge the ticker so it observes the stop flag promptly.
        self.engine.increment_epoch();
        if let Some(h) = self.epoch_thread.take() {
            let _ = h.join();
        }
    }
}

fn init_ctx(node: &NodeCtx) -> types_iface::InitCtx {
    types_iface::InitCtx {
        module_name: node.module_name.clone(),
        config: node.config_json.clone(),
        pipeline_name: node.pipeline_name.clone(),
    }
}

fn to_wire(batch: &Batch) -> Result<types_iface::Batch, String> {
    Ok(types_iface::Batch {
        encoding: types_iface::Encoding::Json,
        data: hp_encoding::encode(Encoding::Json, batch).map_err(|e| e.to_string())?,
    })
}

fn from_wire(b: &types_iface::Batch) -> Result<Batch, String> {
    let enc = match b.encoding {
        types_iface::Encoding::Json => Encoding::Json,
        types_iface::Encoding::Cbor => Encoding::Cbor,
        types_iface::Encoding::ArrowIpc => Encoding::ArrowIpc,
    };
    hp_encoding::decode(enc, &b.data).map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Processor handle
// ---------------------------------------------------------------------------

struct ProcSlot {
    store: Store<HostState>,
    bindings: proc_bindings::Processor,
}

struct ProcInner {
    linker: Arc<Linker<HostState>>,
    engine: Engine,
    services: Arc<HostServices>,
    mem_bytes: usize,
    pool: Mutex<Vec<ProcSlot>>,
}

/// A loaded processor module. Cheap to clone (shared instance pool).
#[derive(Clone)]
pub struct WasmProcessor {
    component: Component,
    node: Arc<NodeCtx>,
    engine_deadline: u64,
    inner: Arc<ProcInner>,
}

impl WasmProcessor {
    /// Transform one batch. Returns the module's output batches (0..n).
    pub fn process(&self, input: &Batch) -> Result<Vec<Batch>> {
        let wire = to_wire(input).map_err(|e| anyhow::anyhow!("encode input: {e}"))?;
        let mut slot = self.checkout()?;
        slot.store.set_epoch_deadline(self.engine_deadline);
        let call = slot
            .bindings
            .call_process(&mut slot.store, &wire)
            .context("call process")?;
        let result = match call {
            Ok(types_iface::Output::Batches(bs)) => {
                let mut out = Vec::with_capacity(bs.len());
                for b in &bs {
                    out.push(from_wire(b).map_err(|e| anyhow::anyhow!("decode output: {e}"))?);
                }
                Ok(out)
            }
            Err(msg) => Err(anyhow::anyhow!("module process error: {msg}")),
        };
        // Return the (still-good) instance to the pool only on success; on a
        // trap the store is poisoned, so drop it and let checkout make a fresh one.
        if result.is_ok() {
            self.inner.pool.lock().unwrap().push(slot);
        }
        result
    }

    fn checkout(&self) -> Result<ProcSlot> {
        if let Some(slot) = self.inner.pool.lock().unwrap().pop() {
            return Ok(slot);
        }
        // Pool empty (all in use or a prior trap) — build a fresh instance.
        let mut store = new_store_for(
            &self.inner.engine,
            &self.node,
            &self.inner.services,
            self.inner.mem_bytes,
            self.engine_deadline,
        );
        let bindings =
            proc_bindings::Processor::instantiate(&mut store, &self.component, &self.inner.linker)
                .context("instantiate processor")?;
        let ctx = init_ctx(&self.node);
        bindings
            .call_init(&mut store, &ctx)
            .context("call init")?
            .map_err(|e| anyhow::anyhow!("module init: {e}"))?;
        Ok(ProcSlot { store, bindings })
    }
}

// ---------------------------------------------------------------------------
// Sink handle
// ---------------------------------------------------------------------------

struct SinkSlot {
    store: Store<HostState>,
    bindings: sink_bindings::Sink,
}

struct SinkInner {
    linker: Arc<Linker<HostState>>,
    engine: Engine,
    mem_bytes: usize,
    services: Arc<HostServices>,
    pool: Mutex<Vec<SinkSlot>>,
}

/// A loaded sink module. Cheap to clone (shared instance pool).
#[derive(Clone)]
pub struct WasmSink {
    component: Component,
    node: Arc<NodeCtx>,
    deadline: u64,
    inner: Arc<SinkInner>,
}

impl WasmSink {
    /// Deliver one batch. Returning `Ok` means the batch is durable enough to ack.
    pub fn write(&self, input: &Batch) -> Result<()> {
        let wire = to_wire(input).map_err(|e| anyhow::anyhow!("encode input: {e}"))?;
        let mut slot = self.checkout()?;
        slot.store.set_epoch_deadline(self.deadline);
        let call = slot
            .bindings
            .call_write(&mut slot.store, &wire)
            .context("call write")?;
        let result = call.map_err(|msg| anyhow::anyhow!("module write error: {msg}"));
        if result.is_ok() {
            self.inner.pool.lock().unwrap().push(slot);
        }
        result
    }

    /// Flush all pooled instances (graceful shutdown / checkpoint barrier).
    pub fn flush(&self) -> Result<()> {
        let mut pool = self.inner.pool.lock().unwrap();
        for slot in pool.iter_mut() {
            slot.store.set_epoch_deadline(self.deadline);
            slot.bindings
                .call_flush(&mut slot.store)
                .context("call flush")?
                .map_err(|e| anyhow::anyhow!("module flush error: {e}"))?;
        }
        Ok(())
    }

    fn checkout(&self) -> Result<SinkSlot> {
        if let Some(slot) = self.inner.pool.lock().unwrap().pop() {
            return Ok(slot);
        }
        let mut store = new_store_for(
            &self.inner.engine,
            &self.node,
            &self.inner.services,
            self.inner.mem_bytes,
            self.deadline,
        );
        let bindings =
            sink_bindings::Sink::instantiate(&mut store, &self.component, &self.inner.linker)
                .context("instantiate sink")?;
        let ctx = init_ctx(&self.node);
        bindings
            .call_init(&mut store, &ctx)
            .context("call init")?
            .map_err(|e| anyhow::anyhow!("module init: {e}"))?;
        Ok(SinkSlot { store, bindings })
    }
}

fn new_store_for(
    engine: &Engine,
    node: &NodeCtx,
    services: &Arc<HostServices>,
    mem_bytes: usize,
    deadline: u64,
) -> Store<HostState> {
    let limits = StoreLimitsBuilder::new().memory_size(mem_bytes).build();
    let state = HostState::new(
        node.module_name.clone(),
        node.http_allow.clone(),
        node.granted_conns.clone(),
        services.clone(),
        limits,
    );
    let mut store = Store::new(engine, state);
    store.limiter(|s| &mut s.limits);
    store.set_epoch_deadline(deadline);
    store
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

// ---------------------------------------------------------------------------
// Service construction
// ---------------------------------------------------------------------------

/// Open postgres pools for the given connections and assemble [`HostServices`].
pub fn build_services(
    handle: tokio::runtime::Handle,
    pg_conns: HashMap<String, (String, u32)>,
    kv: Arc<dyn KvStore>,
) -> Result<Arc<HostServices>> {
    let http = reqwest::Client::builder()
        .user_agent("hyperpipe/0.1")
        .build()
        .context("build http client")?;

    let mut sql = HashMap::new();
    for (name, (dsn, max)) in pg_conns {
        let pool = handle
            .block_on(async {
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(max)
                    .connect(&dsn)
                    .await
            })
            .with_context(|| format!("connect postgres `{name}`"))?;
        sql.insert(name, pool);
    }

    Ok(Arc::new(HostServices {
        http,
        sql,
        blob: HashMap::new(),
        handle,
        kv,
        metrics: Arc::new(Metrics::default()),
    }))
}

/// Async variant of [`build_services`] for use from within a tokio runtime
/// (the CLI's async context, where `block_on` would panic).
pub async fn build_services_async(
    handle: tokio::runtime::Handle,
    pg_conns: HashMap<String, (String, u32)>,
    blob_conns: HashMap<String, BlobConnCfg>,
    kv: Arc<dyn KvStore>,
) -> Result<Arc<HostServices>> {
    let http = reqwest::Client::builder()
        .user_agent("hyperpipe/0.1")
        .build()
        .context("build http client")?;
    let mut sql = HashMap::new();
    for (name, (dsn, max)) in pg_conns {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(max)
            .connect(&dsn)
            .await
            .with_context(|| format!("connect postgres `{name}`"))?;
        sql.insert(name, pool);
    }
    let mut blob = HashMap::new();
    for (name, cfg) in blob_conns {
        blob.insert(
            name.clone(),
            build_blob(&cfg).with_context(|| format!("open object store `{name}`"))?,
        );
    }
    Ok(Arc::new(HostServices {
        http,
        sql,
        blob,
        handle,
        kv,
        metrics: Arc::new(Metrics::default()),
    }))
}

/// Services with no external connections (decode/stdout pipelines, tests).
pub fn build_services_bare(handle: tokio::runtime::Handle, kv: Arc<dyn KvStore>) -> Arc<HostServices> {
    HostServices {
        http: reqwest::Client::builder()
            .user_agent("hyperpipe/0.1")
            .build()
            .expect("http client"),
        sql: HashMap::new(),
        blob: HashMap::new(),
        handle,
        kv,
        metrics: Arc::new(Metrics::default()),
    }
    .into_arc()
}

impl HostServices {
    fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}
