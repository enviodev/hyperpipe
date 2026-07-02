//! wasmtime component bindings, the sandboxed host state, and the host-import
//! implementations (the ONLY way a module touches the outside world — §6.3).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use wasmtime::component::ResourceTable;
use wasmtime::StoreLimits;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};

// Bindgen for the `processor` world. This also generates the shared `host` and
// `types` interface modules that the `sink` world reuses via `with`.
pub mod proc_bindings {
    wasmtime::component::bindgen!({
        world: "processor",
        path: "../../wit",
        trappable_imports: true,
    });
}

// Bindgen for the `sink` world, reusing the host + types modules above so there
// is exactly one `Host` trait and one set of record types to implement.
pub mod sink_bindings {
    wasmtime::component::bindgen!({
        world: "sink",
        path: "../../wit",
        trappable_imports: true,
        with: {
            "envio:hyperpipe/host": crate::host_impl::proc_bindings::envio::hyperpipe::host,
            "envio:hyperpipe/types": crate::host_impl::proc_bindings::envio::hyperpipe::types,
        },
    });
}

pub use proc_bindings::envio::hyperpipe::host as host_iface;
pub use proc_bindings::envio::hyperpipe::types as types_iface;

pub use hp_encoding::KvStore;

/// In-memory KV (M4 / tests). The SQLite-backed [`CheckpointStore`] is the
/// durable implementation used by the runtime.
#[derive(Default)]
pub struct MemKv {
    inner: Mutex<HashMap<(String, String), Vec<u8>>>,
}

impl KvStore for MemKv {
    fn get(&self, module: &str, key: &str) -> Option<Vec<u8>> {
        self.inner
            .lock()
            .unwrap()
            .get(&(module.to_string(), key.to_string()))
            .cloned()
    }
    fn set(&self, module: &str, key: &str, value: Vec<u8>) {
        self.inner
            .lock()
            .unwrap()
            .insert((module.to_string(), key.to_string()), value);
    }
}

/// Simple additive metrics registry (name -> counter). Prometheus export is
/// phase 1; this keeps the values so the engine can log them.
#[derive(Default)]
pub struct Metrics {
    counters: Mutex<HashMap<String, u64>>,
}

impl Metrics {
    pub fn add(&self, name: &str, value: u64) {
        *self.counters.lock().unwrap().entry(name.to_string()).or_default() += value;
    }
    pub fn snapshot(&self) -> HashMap<String, u64> {
        self.counters.lock().unwrap().clone()
    }
}

/// Process-wide host services shared by every module instance. Owns the real
/// IO resources; modules only ever reference them through capability checks.
/// An object-store connection: the store plus a key prefix to prepend.
#[derive(Clone)]
pub struct BlobConn {
    pub store: Arc<dyn object_store::ObjectStore>,
    pub prefix: String,
}

pub struct HostServices {
    pub http: reqwest::Client,
    pub sql: HashMap<String, sqlx::Pool<sqlx::Postgres>>,
    pub blob: HashMap<String, BlobConn>,
    pub handle: tokio::runtime::Handle,
    pub kv: Arc<dyn KvStore>,
    pub metrics: Arc<Metrics>,
}

/// Per-instance store state: WASI (locked down), resource table, memory limits,
/// and this module's capability grants.
pub struct HostState {
    wasi: WasiCtx,
    table: ResourceTable,
    pub limits: StoreLimits,
    pub module_name: String,
    pub http_allow: Vec<String>,
    pub granted_conns: Vec<String>,
    pub services: Arc<HostServices>,
}

impl HostState {
    pub fn new(
        module_name: String,
        http_allow: Vec<String>,
        granted_conns: Vec<String>,
        services: Arc<HostServices>,
        limits: StoreLimits,
    ) -> Self {
        // Locked-down WASI: inherit stdout/stderr (so stdout-sink works), but no
        // args, no env, no preopened dirs, no sockets (§6.3).
        let wasi = WasiCtxBuilder::new().inherit_stdout().inherit_stderr().build();
        HostState {
            wasi,
            table: ResourceTable::new(),
            limits,
            module_name,
            http_allow,
            granted_conns,
            services,
        }
    }

    fn host_allowed(&self, url: &str) -> bool {
        host_allowed_by(&self.http_allow, url)
    }
}

/// Capability check for the `http` import: the URL's hostname must EXACTLY
/// match an allowlist entry (§6.3). No prefix/suffix matching — `evil-slack.com`
/// must not pass an allowlist containing `slack.com`. Unparseable URLs are
/// denied (fail closed).
fn host_allowed_by(allow: &[String], url: &str) -> bool {
    match url_host(url) {
        Some(host) => allow.iter().any(|h| *h == host),
        None => false,
    }
}

impl WasiView for HostState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

fn url_host(url: &str) -> Option<String> {
    // avoid a url crate dependency: scheme://host[:port]/...
    let after = url.split("://").nth(1)?;
    let authority = after.split('/').next()?;
    let host = authority.split('@').last()?; // strip userinfo
    let host = host.split(':').next()?; // strip port
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

use host_iface::{Host, HttpRequest, HttpResponse, LogLevel};

impl Host for HostState {
    fn log(&mut self, level: LogLevel, message: String) -> wasmtime::Result<()> {
        let module = &self.module_name;
        match level {
            LogLevel::Trace => tracing::trace!(module, "{message}"),
            LogLevel::Debug => tracing::debug!(module, "{message}"),
            LogLevel::Info => tracing::info!(module, "{message}"),
            LogLevel::Warn => tracing::warn!(module, "{message}"),
            LogLevel::Error => tracing::error!(module, "{message}"),
        }
        Ok(())
    }

    fn metric_add(&mut self, name: String, value: u64) -> wasmtime::Result<()> {
        self.services
            .metrics
            .add(&format!("{}.{}", self.module_name, name), value);
        Ok(())
    }

    fn kv_get(&mut self, key: String) -> wasmtime::Result<Option<Vec<u8>>> {
        Ok(self.services.kv.get(&self.module_name, &key))
    }

    fn kv_set(&mut self, key: String, value: Vec<u8>) -> wasmtime::Result<()> {
        self.services.kv.set(&self.module_name, &key, value);
        Ok(())
    }

    fn http(&mut self, req: HttpRequest) -> wasmtime::Result<Result<HttpResponse, String>> {
        if !self.host_allowed(&req.url) {
            return Ok(Err(format!(
                "http denied: host of `{}` not in permissions.http allowlist {:?}",
                req.url, self.http_allow
            )));
        }
        let client = self.services.http.clone();
        let result = self.services.handle.block_on(async move {
            let method = reqwest::Method::from_bytes(req.method.as_bytes())
                .map_err(|e| format!("bad method: {e}"))?;
            let mut rb = client.request(method, &req.url);
            for (k, v) in &req.headers {
                rb = rb.header(k, v);
            }
            if let Some(body) = req.body {
                rb = rb.body(body);
            }
            let resp = rb.send().await.map_err(|e| format!("request: {e}"))?;
            let status = resp.status().as_u16();
            let headers = resp
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();
            let body = resp.bytes().await.map_err(|e| format!("body: {e}"))?.to_vec();
            Ok::<HttpResponse, String>(HttpResponse { status, headers, body })
        });
        Ok(result)
    }

    fn sql_exec(
        &mut self,
        conn: String,
        statement: String,
        params_json: Vec<u8>,
    ) -> wasmtime::Result<Result<u64, String>> {
        Ok(self.run_sql_batch(&conn, vec![(statement, params_json)]))
    }

    fn sql_batch(
        &mut self,
        conn: String,
        statements: Vec<(String, Vec<u8>)>,
    ) -> wasmtime::Result<Result<u64, String>> {
        Ok(self.run_sql_batch(&conn, statements))
    }

    fn kafka_produce(
        &mut self,
        _conn: String,
        _topic: String,
        _key: Option<Vec<u8>>,
        _payload: Vec<u8>,
    ) -> wasmtime::Result<Result<(), String>> {
        Ok(Err("kafka sink not wired (phase 1)".into()))
    }

    fn blob_put(
        &mut self,
        conn: String,
        key: String,
        data: Vec<u8>,
    ) -> wasmtime::Result<Result<(), String>> {
        if !self.granted_conns.iter().any(|c| *c == conn) {
            return Ok(Err(format!(
                "connection `{conn}` not granted to module `{}`",
                self.module_name
            )));
        }
        let blob = match self.services.blob.get(&conn) {
            Some(b) => b.clone(),
            None => return Ok(Err(format!("connection `{conn}` has no open object store"))),
        };
        let path = object_store::path::Path::from(format!("{}{}", blob.prefix, key));
        let result = self.services.handle.block_on(async move {
            blob.store
                .put(&path, bytes::Bytes::from(data).into())
                .await
                .map(|_| ())
                .map_err(|e| format!("blob put: {e}"))
        });
        Ok(result)
    }
}

impl HostState {
    /// Execute a set of parameterized statements in one transaction against a
    /// granted postgres connection. Params are a JSON array bound positionally.
    fn run_sql_batch(&self, conn: &str, statements: Vec<(String, Vec<u8>)>) -> Result<u64, String> {
        if !self.granted_conns.iter().any(|c| c == conn) {
            return Err(format!(
                "connection `{conn}` not granted to module `{}`",
                self.module_name
            ));
        }
        let pool = self
            .services
            .sql
            .get(conn)
            .ok_or_else(|| format!("connection `{conn}` has no open pool"))?
            .clone();

        self.services.handle.block_on(async move {
            let mut tx = pool.begin().await.map_err(|e| format!("begin: {e}"))?;
            let mut affected = 0u64;
            for (stmt, params_json) in statements {
                let params: Vec<serde_json::Value> = if params_json.is_empty() {
                    Vec::new()
                } else {
                    serde_json::from_slice(&params_json).map_err(|e| format!("params json: {e}"))?
                };
                let mut q = sqlx::query(&stmt);
                for p in &params {
                    q = bind_json(q, p);
                }
                let r = q.execute(&mut *tx).await.map_err(|e| format!("exec: {e}"))?;
                affected += r.rows_affected();
            }
            tx.commit().await.map_err(|e| format!("commit: {e}"))?;
            Ok(affected)
        })
    }
}

/// Bind a JSON scalar as a postgres parameter. Numbers >= 2^53 arrive as
/// decimal strings (§3.1); we pass them as text and let postgres coerce to the
/// column type (numeric/text).
fn bind_json<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    v: &'q serde_json::Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match v {
        serde_json::Value::Null => q.bind(Option::<String>::None),
        serde_json::Value::Bool(b) => q.bind(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                q.bind(i)
            } else if let Some(f) = n.as_f64() {
                q.bind(f)
            } else {
                q.bind(n.to_string())
            }
        }
        serde_json::Value::String(s) => q.bind(s.as_str()),
        other => q.bind(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(hosts: &[&str]) -> Vec<String> {
        hosts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn url_host_parses_common_shapes() {
        assert_eq!(url_host("https://hooks.slack.com/services/x"), Some("hooks.slack.com".into()));
        assert_eq!(url_host("http://127.0.0.1:8716/hook"), Some("127.0.0.1".into()));
        assert_eq!(url_host("https://user:pw@api.example.com:443/v1"), Some("api.example.com".into()));
        assert_eq!(url_host("https://api.example.com"), Some("api.example.com".into()));
    }

    #[test]
    fn url_host_rejects_malformed() {
        assert_eq!(url_host("not-a-url"), None);
        assert_eq!(url_host("https://"), None);
        assert_eq!(url_host(""), None);
    }

    #[test]
    fn allowlist_is_exact_match_only() {
        let a = allow(&["slack.com"]);
        assert!(host_allowed_by(&a, "https://slack.com/x"));
        // No suffix/prefix tricks.
        assert!(!host_allowed_by(&a, "https://evil-slack.com/x"));
        assert!(!host_allowed_by(&a, "https://slack.com.evil.io/x"));
        assert!(!host_allowed_by(&a, "https://api.slack.com/x")); // subdomain != listed host
    }

    #[test]
    fn deny_by_default() {
        assert!(!host_allowed_by(&[], "https://anywhere.com/x"));
        assert!(!host_allowed_by(&allow(&["a.com"]), "garbage"));
    }
}
