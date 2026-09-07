//! wasmtime embedding for HyperPipe modules (§6).
//!
//! [`Runtime`] owns the wasmtime `Engine`, the shared linker, host services,
//! and the epoch ticker. It loads components into [`WasmProcessor`] /
//! [`WasmSink`] handles that the engine drives with decoded [`hp_encoding::Batch`]
//! values. Each handle keeps a small pool of pre-initialized instances.

mod host_impl;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
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

/// Shared reqwest client for guest `host.http` calls. Timeouts are mandatory:
/// epoch interruption cannot preempt a guest parked inside a host import, so
/// without them one hung endpoint wedges a worker thread forever.
///
/// Redirects are never followed. The `permissions.http` allowlist is checked
/// against the URL the guest asked for; an allowlisted host that answered
/// with a 3xx to somewhere else would otherwise pull the request onto a host
/// the module was never granted. The guest sees the 3xx and can decide.
pub(crate) fn build_http_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent("hyperpipe/0.1")
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

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
    /// False when the cache directory could not be made private to this
    /// user; then every load precompiles in memory and nothing is read from
    /// or written to disk.
    cache_usable: bool,
    epoch_stop: Arc<AtomicBool>,
    epoch_thread: Option<std::thread::JoinHandle<()>>,
}

// ---------------------------------------------------------------------------
// Precompiled-component cache.
//
// `Component::deserialize_file` is `unsafe` because a `.cwasm` is native code:
// whatever is in that file runs in this process with the host's privileges,
// outside the sandbox. The cache key is the hash of the *input* wasm, so it
// says nothing about the bytes on disk. The only thing that makes the cache
// trustworthy is that nobody else can write to it, so that is what is
// enforced: the directory and every entry must be owned by this user and not
// writable by anyone else. Entries are written to a temp file with mode 0600
// and renamed into place, so a reader never sees a half-written artifact.
// ---------------------------------------------------------------------------

/// Create the cache directory if needed and decide whether it is safe to use.
fn prepare_cache_dir(dir: &Path) -> bool {
    if !dir.exists() {
        if let Err(e) = std::fs::create_dir_all(dir) {
            tracing::warn!(dir = %dir.display(), "cannot create component cache: {e}; caching disabled");
            return false;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
        }
    }
    match std::fs::metadata(dir) {
        Ok(md) if md.is_dir() => {
            if private_to_this_user(&md) {
                true
            } else {
                tracing::warn!(
                    dir = %dir.display(),
                    "component cache is not private to this user (wrong owner or group/other-writable); \
                     caching disabled — chmod 700 it or set XDG_CACHE_HOME to a private directory"
                );
                false
            }
        }
        _ => {
            tracing::warn!(dir = %dir.display(), "component cache path is not a directory; caching disabled");
            false
        }
    }
}

/// A cache entry may be handed to `deserialize_file` only if it is a regular
/// file that this user owns and nobody else can modify.
fn cache_entry_trusted(path: &Path) -> bool {
    match std::fs::symlink_metadata(path) {
        Ok(md) => md.is_file() && private_to_this_user(&md),
        Err(_) => false,
    }
}

#[cfg(unix)]
fn private_to_this_user(md: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    // SAFETY: geteuid has no preconditions and cannot fail.
    let me = unsafe { libc::geteuid() };
    md.uid() == me && md.mode() & 0o022 == 0
}

#[cfg(not(unix))]
fn private_to_this_user(_md: &std::fs::Metadata) -> bool {
    // No portable ownership/ACL check here; rely on the per-user cache location.
    true
}

/// Atomically place `bytes` at `path` with mode 0600.
fn write_cache_entry(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        use std::io::Write;
        let mut f = opts.open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
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

        let cache_usable = prepare_cache_dir(&cfg.cache_dir);

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
            cache_usable,
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
        if self.cache_usable && cache_entry_trusted(&path) {
            // SAFETY: the directory and this entry are owned by us and not
            // writable by anyone else (checked above), and we only ever write
            // artifacts produced by an engine with this configuration.
            // wasmtime additionally rejects artifacts from a different
            // engine/version.
            if let Ok(c) = unsafe { Component::deserialize_file(&self.engine, &path) } {
                return Ok(c);
            }
            tracing::warn!(path = %path.display(), "cached component rejected by wasmtime; recompiling");
        }
        let serialized = self
            .engine
            .precompile_component(wasm)
            .context("precompile component")?;
        if self.cache_usable {
            if let Err(e) = write_cache_entry(&path, &serialized) {
                tracing::warn!(path = %path.display(), "could not write component cache entry: {e}");
            }
        }
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
        // A trap poisons the store: drop the slot (checkout builds a fresh one
        // later). A graceful module Err leaves a perfectly reusable instance —
        // return it to the pool, or its internal state would be lost.
        let call = match slot.bindings.call_process(&mut slot.store, &wire) {
            Ok(c) => c,
            Err(trap) => return Err(trap).context("call process"),
        };
        self.inner.pool.lock().unwrap().push(slot);
        match call {
            Ok(types_iface::Output::Batches(bs)) => {
                let mut out = Vec::with_capacity(bs.len());
                for b in &bs {
                    out.push(from_wire(b).map_err(|e| anyhow::anyhow!("decode output: {e}"))?);
                }
                Ok(out)
            }
            Err(msg) => Err(anyhow::anyhow!("module process error: {msg}")),
        }
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
        // Trap -> drop the poisoned slot. Graceful Err -> KEEP the instance:
        // a buffering sink (e.g. s3) holds not-yet-flushed rows in its state,
        // and discarding it on a retryable write error would silently lose them.
        let call = match slot.bindings.call_write(&mut slot.store, &wire) {
            Ok(c) => c,
            Err(trap) => return Err(trap).context("call write"),
        };
        self.inner.pool.lock().unwrap().push(slot);
        call.map_err(|msg| anyhow::anyhow!("module write error: {msg}"))
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
    let http = build_http_client().context("build http client")?;

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
    let http = build_http_client().context("build http client")?;
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
        http: build_http_client().expect("http client"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use hp_encoding::{BatchKind, BlockRange};
    use serde_json::json;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("hp-blob-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn a_local_path_connection_becomes_a_filesystem_store() {
        // The dev/test escape hatch: no MinIO, no credentials, just a directory.
        let dir = tmpdir("local");
        let conn = build_blob(&BlobConnCfg {
            local_path: Some(dir.to_string_lossy().into_owned()),
            prefix: Some("lake/".into()),
            ..Default::default()
        })
        .expect("local store");
        assert_eq!(conn.prefix, "lake/");
        assert!(dir.exists(), "build_blob creates the directory");
    }

    #[test]
    fn a_connection_with_neither_bucket_nor_local_path_is_rejected() {
        let e = build_blob(&BlobConnCfg::default()).err().unwrap();
        assert!(
            format!("{e:#}").contains("needs `bucket` or `local_path`"),
            "got {e:#}"
        );
    }

    #[test]
    fn an_s3_connection_builds_from_bucket_region_and_endpoint() {
        // A custom endpoint (MinIO/localstack) implies allow_http.
        let conn = build_blob(&BlobConnCfg {
            bucket: Some("my-bucket".into()),
            region: Some("us-east-1".into()),
            endpoint: Some("http://127.0.0.1:9000".into()),
            prefix: None,
            local_path: None,
        });
        assert!(conn.is_ok(), "building must not require a live endpoint: {:?}", conn.err());
        assert_eq!(conn.unwrap().prefix, "", "an absent prefix is empty, not None");
    }

    #[test]
    fn local_path_wins_over_a_bucket() {
        let dir = tmpdir("local-wins");
        let conn = build_blob(&BlobConnCfg {
            bucket: Some("ignored".into()),
            local_path: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        });
        assert!(conn.is_ok(), "local_path short-circuits the S3 builder");
    }

    // ---- wire codec --------------------------------------------------------

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
    fn wire_roundtrip_preserves_the_envelope() {
        let b = batch();
        let wire = to_wire(&b).unwrap();
        assert!(matches!(wire.encoding, types_iface::Encoding::Json));
        let back = from_wire(&wire).unwrap();
        assert_eq!(back.batch_id, b.batch_id);
        assert_eq!(back.block_range, b.block_range);
        assert_eq!(back.records[0]["value"], "5000000000000");
    }

    #[test]
    fn from_wire_reports_unwired_encodings() {
        // A guest that tags its output cbor gets a clear error, not a panic.
        let wire = types_iface::Batch {
            encoding: types_iface::Encoding::Cbor,
            data: to_wire(&batch()).unwrap().data,
        };
        let e = from_wire(&wire).err().expect("cbor is not wired");
        assert!(e.contains("unsupported encoding"), "got {e}");

        let wire = types_iface::Batch {
            encoding: types_iface::Encoding::ArrowIpc,
            data: vec![],
        };
        assert!(from_wire(&wire).is_err());
    }

    #[test]
    fn from_wire_reports_malformed_payloads() {
        let wire = types_iface::Batch {
            encoding: types_iface::Encoding::Json,
            data: b"{ not json".to_vec(),
        };
        assert!(from_wire(&wire).err().unwrap().contains("json"));
    }

    // ---- misc --------------------------------------------------------------

    #[test]
    fn sha256_hex_is_the_component_cache_key() {
        // Same bytes -> same key (cache hit); one bit different -> new entry.
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_ne!(sha256_hex(b"abc"), sha256_hex(b"abd"));
        assert_eq!(sha256_hex(b"").len(), 64);
    }

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("hp-cache-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn prepare_cache_dir_creates_a_private_directory() {
        let d = scratch("create");
        assert!(prepare_cache_dir(&d));
        assert!(d.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&d).unwrap().permissions().mode() & 0o777, 0o700);
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    #[cfg(unix)]
    #[test]
    fn a_shared_writable_cache_dir_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let d = scratch("shared");
        std::fs::create_dir_all(&d).unwrap();
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(!prepare_cache_dir(&d), "group/other-writable dir must disable the cache");
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(prepare_cache_dir(&d));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[cfg(unix)]
    #[test]
    fn cache_entries_must_be_private_regular_files() {
        use std::os::unix::fs::PermissionsExt;
        let d = scratch("entries");
        std::fs::create_dir_all(&d).unwrap();

        let ok = d.join("ok.cwasm");
        write_cache_entry(&ok, b"native code").unwrap();
        assert_eq!(std::fs::metadata(&ok).unwrap().permissions().mode() & 0o777, 0o600);
        assert!(cache_entry_trusted(&ok));
        assert!(!d.join(format!("ok.tmp-{}", std::process::id())).exists(), "temp file renamed away");

        // Someone else could have modified this one.
        let loose = d.join("loose.cwasm");
        std::fs::write(&loose, b"x").unwrap();
        std::fs::set_permissions(&loose, std::fs::Permissions::from_mode(0o666)).unwrap();
        assert!(!cache_entry_trusted(&loose));

        // A symlink pointing somewhere we do not control is not a cache entry.
        let link = d.join("link.cwasm");
        std::os::unix::fs::symlink(&ok, &link).unwrap();
        assert!(!cache_entry_trusted(&link));

        assert!(!cache_entry_trusted(&d.join("missing.cwasm")));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn cache_dir_prefers_xdg_then_home() {
        let _g = ENV_LOCK.lock().unwrap();
        let (xdg, home) = (std::env::var_os("XDG_CACHE_HOME"), std::env::var_os("HOME"));

        std::env::set_var("XDG_CACHE_HOME", "/xdg");
        std::env::set_var("HOME", "/home/u");
        assert_eq!(default_cache_dir(), PathBuf::from("/xdg/hyperpipe"));

        std::env::remove_var("XDG_CACHE_HOME");
        assert_eq!(default_cache_dir(), PathBuf::from("/home/u/.cache/hyperpipe"));

        std::env::remove_var("HOME");
        assert_eq!(default_cache_dir(), PathBuf::from(".cache/hyperpipe"));

        if let Some(v) = xdg {
            std::env::set_var("XDG_CACHE_HOME", v);
        }
        if let Some(v) = home {
            std::env::set_var("HOME", v);
        }
    }

    #[test]
    fn default_runtime_config_matches_the_medium_profile() {
        let c = RuntimeConfig::default();
        assert_eq!(c.wasm_mem_bytes, 256 << 20);
        assert_eq!(c.epoch_deadline_secs, 5);
        assert_eq!(c.instances_per_stage, 2);
    }

    #[test]
    fn bare_services_have_no_connections() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let s = build_services_bare(rt.handle().clone(), Arc::new(MemKv::default()));
        assert!(s.sql.is_empty());
        assert!(s.blob.is_empty());
        assert!(s.metrics.snapshot().is_empty());
    }

    #[tokio::test]
    async fn an_unusable_postgres_dsn_names_the_connection() {
        // Fail at startup with the name the user wrote in their YAML, not with a
        // bare sqlx error. (A malformed DSN rather than an unreachable host: the
        // pool retries a refused connection for its full 30s acquire timeout,
        // which is a slow test for the same `connect postgres` context line.)
        let pg = HashMap::from([("main_db".to_string(), ("not-a-dsn".to_string(), 1u32))]);
        let e = build_services_async(
            tokio::runtime::Handle::current(),
            pg,
            HashMap::new(),
            Arc::new(MemKv::default()),
        )
        .await
        .err()
        .expect("must fail");
        assert!(format!("{e:#}").contains("connect postgres `main_db`"), "got {e:#}");
    }

    #[tokio::test]
    async fn a_bad_blob_connection_names_the_connection() {
        let blob = HashMap::from([("lake".to_string(), BlobConnCfg::default())]);
        let e = build_services_async(
            tokio::runtime::Handle::current(),
            HashMap::new(),
            blob,
            Arc::new(MemKv::default()),
        )
        .await
        .err()
        .expect("must fail");
        assert!(format!("{e:#}").contains("open object store `lake`"), "got {e:#}");
    }

    #[tokio::test]
    async fn services_with_no_connections_build() {
        let s = build_services_async(
            tokio::runtime::Handle::current(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(MemKv::default()),
        )
        .await
        .unwrap();
        assert!(s.sql.is_empty() && s.blob.is_empty());
    }

    #[test]
    fn a_runtime_starts_and_stops_its_epoch_ticker() {
        // Drop must join the ticker thread; leaking one per pipeline reload
        // would pile up threads.
        let rt = tokio::runtime::Runtime::new().unwrap();
        for _ in 0..3 {
            let services = build_services_bare(rt.handle().clone(), Arc::new(MemKv::default()));
            let mut cfg = RuntimeConfig::default();
            cfg.cache_dir = std::env::temp_dir().join("hyperpipe-test-cache");
            let r = Runtime::new(cfg, services).expect("runtime");
            assert!(r.services().sql.is_empty());
            drop(r);
        }
    }
}
