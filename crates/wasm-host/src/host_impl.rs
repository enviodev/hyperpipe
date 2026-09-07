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
    // Hostnames are case-insensitive (reqwest lowercases them); compare
    // likewise or a mixed-case allowlist entry/URL would be wrongly denied.
    match url_host(url) {
        Some(host) => allow.iter().any(|h| h.eq_ignore_ascii_case(&host)),
        None => false,
    }
}

/// Cap on guest-visible HTTP response bodies. A misbehaving (even allowlisted)
/// endpoint must not be able to OOM the host by streaming an unbounded body.
const HTTP_BODY_LIMIT: usize = 32 << 20; // 32 MiB

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
            // Only the host goes into the message: it is returned to the guest
            // and logged by the engine, and webhook-style URLs carry their
            // credential in the path.
            return Ok(Err(format!(
                "http denied: host `{}` not in permissions.http allowlist {:?}",
                url_host(&req.url).unwrap_or_else(|| "<invalid url>".into()),
                self.http_allow
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
            let mut resp = rb.send().await.map_err(|e| format!("request: {e}"))?;
            let status = resp.status().as_u16();
            let headers = resp
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();
            if let Some(len) = resp.content_length() {
                if len > HTTP_BODY_LIMIT as u64 {
                    return Err(format!("response body {len} bytes exceeds {HTTP_BODY_LIMIT} limit"));
                }
            }
            let mut body: Vec<u8> = Vec::new();
            while let Some(chunk) = resp.chunk().await.map_err(|e| format!("body: {e}"))? {
                if body.len() + chunk.len() > HTTP_BODY_LIMIT {
                    return Err(format!("response body exceeds {HTTP_BODY_LIMIT} byte limit"));
                }
                body.extend_from_slice(&chunk);
            }
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

    #[test]
    fn allowlist_matches_case_insensitively() {
        assert!(host_allowed_by(&allow(&["api.example.com"]), "https://API.Example.COM/v1"));
        assert!(host_allowed_by(&allow(&["API.EXAMPLE.COM"]), "https://api.example.com/v1"));
        // still no suffix tricks with different case
        assert!(!host_allowed_by(&allow(&["slack.com"]), "https://EVIL-SLACK.COM/x"));
    }

    // ---- host imports ------------------------------------------------------
    //
    // These drive `HostState` directly — no wasm needed. The import bodies use
    // `Handle::block_on`, so every test owns a tokio runtime and calls from the
    // test thread (outside it), exactly as `spawn_blocking` does in the engine.

    use hp_testutil::{MockServer, Reply};
    use serde_json::json;
    use sqlx::Row;

    struct Harness {
        _rt: tokio::runtime::Runtime,
        services: Arc<HostServices>,
        kv: Arc<MemKv>,
    }

    fn harness() -> Harness {
        harness_with(HashMap::new(), HashMap::new())
    }

    fn harness_with(
        sql: HashMap<String, sqlx::Pool<sqlx::Postgres>>,
        blob: HashMap<String, BlobConn>,
    ) -> Harness {
        let rt = tokio::runtime::Runtime::new().expect("tokio");
        let kv = Arc::new(MemKv::default());
        let services = Arc::new(HostServices {
            http: reqwest::Client::builder().build().unwrap(),
            sql,
            blob,
            handle: rt.handle().clone(),
            kv: kv.clone(),
            metrics: Arc::new(Metrics::default()),
        });
        Harness { _rt: rt, services, kv }
    }

    impl Harness {
        fn state(&self, module: &str, http_allow: &[&str], conns: &[&str]) -> HostState {
            HostState::new(
                module.to_string(),
                http_allow.iter().map(|s| s.to_string()).collect(),
                conns.iter().map(|s| s.to_string()).collect(),
                self.services.clone(),
                wasmtime::StoreLimitsBuilder::new().build(),
            )
        }
    }

    fn req(method: &str, url: &str) -> HttpRequest {
        HttpRequest {
            method: method.to_string(),
            url: url.to_string(),
            headers: vec![],
            body: None,
        }
    }

    #[test]
    fn http_to_a_denied_host_never_touches_the_network() {
        let h = harness();
        let srv = h._rt.block_on(MockServer::start(|_p, _b, _i| Reply::json(json!({}))));
        // The module is granted a *different* host.
        let mut st = h.state("webhook", &["api.example.com"], &[]);

        let out = st.http(req("POST", &format!("{}/hook", srv.url))).unwrap();
        let e = out.err().expect("must be denied");
        assert!(e.contains("http denied"), "got {e}");
        assert!(e.contains("api.example.com"), "the error should show the allowlist: {e}");
        assert!(!e.contains("/hook"), "the URL path must not be echoed: {e}");
        assert_eq!(srv.call_count(), 0, "a denied call must not reach the server");
    }

    #[test]
    fn http_with_an_empty_allowlist_denies_everything() {
        let h = harness();
        let mut st = h.state("webhook", &[], &[]);
        let out = st.http(req("GET", "https://example.com/")).unwrap();
        assert!(out.err().unwrap().contains("http denied"));
    }

    #[test]
    fn http_to_an_allowed_host_round_trips() {
        let h = harness();
        let srv = h._rt.block_on(MockServer::start(|path, body, _i| {
            assert_eq!(path, "/hook");
            assert_eq!(body["hello"], "world");
            Reply::json(json!({ "ok": true })).with_header("x-test", "yes")
        }));
        let mut st = h.state("webhook", &["127.0.0.1"], &[]);

        let mut r = req("POST", &format!("{}/hook", srv.url));
        r.headers = vec![("content-type".into(), "application/json".into())];
        r.body = Some(br#"{"hello":"world"}"#.to_vec());

        let resp = st.http(r).unwrap().expect("allowed");
        assert_eq!(resp.status, 200);
        assert_eq!(String::from_utf8(resp.body).unwrap(), r#"{"ok":true}"#);
        assert!(resp.headers.iter().any(|(k, v)| k == "x-test" && v == "yes"));
        assert_eq!(srv.requests()[0].path, "/hook");
    }

    #[test]
    fn http_rejects_a_declared_body_over_the_limit() {
        // Guard the host's memory before reading a byte: an allowlisted but
        // misbehaving endpoint must not be able to OOM us.
        let h = harness();
        let srv = h._rt.block_on(MockServer::start(|_p, _b, _i| {
            Reply::declaring_length(HTTP_BODY_LIMIT as u64 + 1)
        }));
        let mut st = h.state("m", &["127.0.0.1"], &[]);
        let e = st.http(req("GET", &srv.url)).unwrap().err().expect("too big");
        assert!(e.contains("exceeds"), "got {e}");
        assert!(e.contains(&HTTP_BODY_LIMIT.to_string()), "got {e}");
    }

    #[test]
    fn http_rejects_an_undeclared_body_over_the_limit() {
        // No content-length: the cap has to hold while streaming chunks.
        let h = harness();
        let srv = h._rt.block_on(MockServer::start(|_p, _b, _i| {
            Reply::until_eof("x".repeat(HTTP_BODY_LIMIT + 1024))
        }));
        let mut st = h.state("m", &["127.0.0.1"], &[]);
        let e = st.http(req("GET", &srv.url)).unwrap().err().expect("too big");
        assert!(e.contains("byte limit"), "got {e}");
    }

    #[test]
    fn http_rejects_a_bad_method() {
        let h = harness();
        let srv = h._rt.block_on(MockServer::start(|_p, _b, _i| Reply::json(json!({}))));
        let mut st = h.state("m", &["127.0.0.1"], &[]);
        let e = st.http(req("GET SPACE", &srv.url)).unwrap().err().expect("bad method");
        assert!(e.contains("bad method"), "got {e}");
        assert_eq!(srv.call_count(), 0);
    }

    #[test]
    fn http_surfaces_transport_errors() {
        let h = harness();
        // Nothing is listening on this port.
        let mut st = h.state("m", &["127.0.0.1"], &[]);
        let e = st
            .http(req("GET", "http://127.0.0.1:1/x"))
            .unwrap()
            .err()
            .expect("connection refused");
        assert!(e.contains("request:"), "got {e}");
    }

    // ---- sql ---------------------------------------------------------------

    #[test]
    fn sql_to_an_ungranted_connection_is_denied() {
        let h = harness();
        let mut st = h.state("pg_sink", &[], &["other_db"]);
        let e = st
            .sql_exec("main_db".into(), "SELECT 1".into(), vec![])
            .unwrap()
            .err()
            .expect("denied");
        assert!(e.contains("connection `main_db` not granted to module `pg_sink`"), "got {e}");
    }

    #[test]
    fn sql_to_a_granted_connection_with_no_pool_says_so() {
        // Granted in config, but the engine opened no pool for it.
        let h = harness();
        let mut st = h.state("pg_sink", &[], &["main_db"]);
        let e = st
            .sql_batch("main_db".into(), vec![("SELECT 1".into(), vec![])])
            .unwrap()
            .err()
            .expect("no pool");
        assert!(e.contains("has no open pool"), "got {e}");
    }

    #[test]
    fn kafka_is_not_wired_yet() {
        let h = harness();
        let mut st = h.state("m", &[], &["k"]);
        let e = st
            .kafka_produce("k".into(), "topic".into(), None, b"x".to_vec())
            .unwrap()
            .err()
            .unwrap();
        assert!(e.contains("kafka sink not wired"), "got {e}");
    }

    // ---- blob --------------------------------------------------------------

    fn local_blob(dir: &std::path::Path, prefix: &str) -> BlobConn {
        std::fs::create_dir_all(dir).unwrap();
        BlobConn {
            store: Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir).unwrap()),
            prefix: prefix.to_string(),
        }
    }

    fn tmpdir(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("hp-host-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn blob_put_to_an_ungranted_connection_is_denied() {
        let dir = tmpdir("blob-denied");
        let blob = HashMap::from([("lake".to_string(), local_blob(&dir, ""))]);
        let h = harness_with(HashMap::new(), blob);
        let mut st = h.state("s3_sink", &[], &[]); // no grants at all
        let e = st
            .blob_put("lake".into(), "k.parquet".into(), b"x".to_vec())
            .unwrap()
            .err()
            .expect("denied");
        assert!(e.contains("connection `lake` not granted to module `s3_sink`"), "got {e}");
        assert!(!dir.join("k.parquet").exists(), "nothing may be written");
    }

    #[test]
    fn blob_put_to_a_granted_connection_with_no_store_says_so() {
        let h = harness();
        let mut st = h.state("s3_sink", &[], &["lake"]);
        let e = st
            .blob_put("lake".into(), "k".into(), b"x".to_vec())
            .unwrap()
            .err()
            .expect("no store");
        assert!(e.contains("has no open object store"), "got {e}");
    }

    #[test]
    fn blob_put_writes_under_the_connection_prefix() {
        let dir = tmpdir("blob-prefix");
        let blob = HashMap::from([("lake".to_string(), local_blob(&dir, "hyperpipe/v1/"))]);
        let h = harness_with(HashMap::new(), blob);
        let mut st = h.state("s3_sink", &[], &["lake"]);

        st.blob_put("lake".into(), "1/100-200-5.parquet".into(), b"PAR1".to_vec())
            .unwrap()
            .expect("granted");

        // The module's key is relative; the connection prefix is the host's.
        let path = dir.join("hyperpipe/v1/1/100-200-5.parquet");
        assert!(path.exists(), "expected {path:?}");
        assert_eq!(std::fs::read(&path).unwrap(), b"PAR1");
    }

    #[test]
    fn blob_put_with_no_prefix_uses_the_key_verbatim() {
        let dir = tmpdir("blob-noprefix");
        let blob = HashMap::from([("lake".to_string(), local_blob(&dir, ""))]);
        let h = harness_with(HashMap::new(), blob);
        let mut st = h.state("s3_sink", &[], &["lake"]);
        st.blob_put("lake".into(), "8453/1-2-1.ndjson".into(), b"{}".to_vec())
            .unwrap()
            .unwrap();
        assert!(dir.join("8453/1-2-1.ndjson").exists());
    }

    // ---- kv / metrics / logging -------------------------------------------

    #[test]
    fn kv_is_namespaced_per_module() {
        // Two modules using the same key must not see each other's value.
        let h = harness();
        let mut a = h.state("dedupe", &[], &[]);
        let mut b = h.state("enrich", &[], &[]);

        a.kv_set("window".into(), b"from-a".to_vec()).unwrap();
        b.kv_set("window".into(), b"from-b".to_vec()).unwrap();

        assert_eq!(a.kv_get("window".into()).unwrap(), Some(b"from-a".to_vec()));
        assert_eq!(b.kv_get("window".into()).unwrap(), Some(b"from-b".to_vec()));
        assert_eq!(a.kv_get("never-set".into()).unwrap(), None);
        // and the namespacing is visible in the backing store
        assert_eq!(h.kv.get("dedupe", "window"), Some(b"from-a".to_vec()));
    }

    #[test]
    fn mem_kv_roundtrips() {
        let kv = MemKv::default();
        assert_eq!(kv.get("m", "k"), None);
        kv.set("m", "k", b"v".to_vec());
        assert_eq!(kv.get("m", "k"), Some(b"v".to_vec()));
        kv.set("m", "k", b"v2".to_vec());
        assert_eq!(kv.get("m", "k"), Some(b"v2".to_vec()));
        assert_eq!(kv.get("other", "k"), None);
    }

    #[test]
    fn metrics_are_prefixed_with_the_module_name() {
        let h = harness();
        let mut a = h.state("whale_filter", &[], &[]);
        let mut b = h.state("decode", &[], &[]);
        a.metric_add("dropped".into(), 3).unwrap();
        a.metric_add("dropped".into(), 2).unwrap();
        b.metric_add("dropped".into(), 7).unwrap();

        let snap = h.services.metrics.snapshot();
        assert_eq!(snap.get("whale_filter.dropped"), Some(&5), "same-named counters must not merge");
        assert_eq!(snap.get("decode.dropped"), Some(&7));
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn metrics_snapshot_of_a_fresh_registry_is_empty() {
        assert!(Metrics::default().snapshot().is_empty());
    }

    #[test]
    fn log_accepts_every_level() {
        let h = harness();
        let mut st = h.state("m", &[], &[]);
        for lvl in [LogLevel::Trace, LogLevel::Debug, LogLevel::Info, LogLevel::Warn, LogLevel::Error] {
            st.log(lvl, "message".into()).unwrap();
        }
    }

    // ---- postgres-backed (soft-skips without HP_TEST_PG_DSN) ---------------

    /// The DSN of a throwaway postgres, e.g.
    /// `HP_TEST_PG_DSN=postgres://postgres:hp@127.0.0.1:5435/hp`.
    fn pg_dsn() -> Option<String> {
        std::env::var("HP_TEST_PG_DSN").ok()
    }

    fn pg_harness(table: &str) -> Option<Harness> {
        let dsn = match pg_dsn() {
            Some(d) => d,
            None => {
                eprintln!("SKIP: set HP_TEST_PG_DSN to run the postgres host-import tests");
                return None;
            }
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let pool = rt
            .block_on(sqlx::postgres::PgPoolOptions::new().max_connections(2).connect(&dsn))
            .expect("HP_TEST_PG_DSN is set but unreachable");
        rt.block_on(async {
            sqlx::query(&format!("DROP TABLE IF EXISTS {table}")).execute(&pool).await.unwrap();
            sqlx::query(&format!("CREATE TABLE {table} (id bigint primary key, v text)"))
                .execute(&pool)
                .await
                .unwrap();
        });
        let kv = Arc::new(MemKv::default());
        let services = Arc::new(HostServices {
            http: reqwest::Client::builder().build().unwrap(),
            sql: HashMap::from([("main".to_string(), pool)]),
            blob: HashMap::new(),
            handle: rt.handle().clone(),
            kv: kv.clone(),
            metrics: Arc::new(Metrics::default()),
        });
        Some(Harness { _rt: rt, services, kv })
    }

    fn pg_rows(h: &Harness, table: &str) -> Vec<(i64, Option<String>)> {
        let pool = h.services.sql.get("main").unwrap().clone();
        h.services.handle.block_on(async move {
            sqlx::query(&format!("SELECT id, v FROM {table} ORDER BY id"))
                .fetch_all(&pool)
                .await
                .unwrap()
                .into_iter()
                .map(|r| (r.get::<i64, _>("id"), r.get::<Option<String>, _>("v")))
                .collect()
        })
    }

    #[test]
    fn sql_batch_commits_every_statement_in_one_transaction() {
        let Some(h) = pg_harness("hp_test_batch") else { return };
        let mut st = h.state("pg_sink", &[], &["main"]);
        let stmts = vec![
            (
                "INSERT INTO hp_test_batch (id, v) VALUES ($1, $2)".to_string(),
                serde_json::to_vec(&json!([1, "a"])).unwrap(),
            ),
            (
                "INSERT INTO hp_test_batch (id, v) VALUES ($1, $2)".to_string(),
                serde_json::to_vec(&json!([2, "b"])).unwrap(),
            ),
        ];
        let affected = st.sql_batch("main".into(), stmts).unwrap().expect("committed");
        assert_eq!(affected, 2, "affected rows are summed across statements");
        assert_eq!(pg_rows(&h, "hp_test_batch"), vec![(1, Some("a".into())), (2, Some("b".into()))]);
    }

    #[test]
    fn a_failing_statement_rolls_the_whole_batch_back() {
        // Atomicity is what lets the engine retry a whole batch safely.
        let Some(h) = pg_harness("hp_test_rollback") else { return };
        let mut st = h.state("pg_sink", &[], &["main"]);
        let stmts = vec![
            (
                "INSERT INTO hp_test_rollback (id, v) VALUES ($1, $2)".to_string(),
                serde_json::to_vec(&json!([1, "a"])).unwrap(),
            ),
            ("INSERT INTO hp_test_rollback (id, v) VALUES (nope)".to_string(), vec![]),
        ];
        let e = st.sql_batch("main".into(), stmts).unwrap().err().expect("must fail");
        assert!(e.contains("exec:"), "got {e}");
        assert!(
            pg_rows(&h, "hp_test_rollback").is_empty(),
            "the first insert must not survive its batch"
        );
    }

    #[test]
    fn malformed_params_json_is_rejected() {
        let Some(h) = pg_harness("hp_test_params") else { return };
        let mut st = h.state("pg_sink", &[], &["main"]);
        let e = st
            .sql_exec(
                "main".into(),
                "INSERT INTO hp_test_params (id) VALUES ($1)".into(),
                b"{not json".to_vec(),
            )
            .unwrap()
            .err()
            .expect("bad params");
        assert!(e.contains("params json"), "got {e}");
    }

    #[test]
    fn bind_json_maps_every_scalar_shape_onto_postgres() {
        let Some(h) = pg_harness("hp_test_bind") else { return };
        let pool = h.services.sql.get("main").unwrap().clone();
        h.services.handle.block_on(async {
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS hp_test_bind_all (
                    nul text, b boolean, i bigint, f double precision,
                    big numeric, s text, arr text)",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("DELETE FROM hp_test_bind_all").execute(&pool).await.unwrap();
        });

        let mut st = h.state("pg_sink", &[], &["main"]);
        // A uint256 arrives as a decimal string (§3.1) and must land in `numeric`
        // intact; an array becomes JSON text.
        let params = json!([
            null, true, -42, 1.5,
            "123456789012345678901234567890",
            "hello",
            ["a", "b"]
        ]);
        st.sql_exec(
            "main".into(),
            "INSERT INTO hp_test_bind_all (nul, b, i, f, big, s, arr)
             VALUES ($1, $2, $3, $4, $5::numeric, $6, $7)"
                .into(),
            serde_json::to_vec(&params).unwrap(),
        )
        .unwrap()
        .expect("bound");

        let row = h.services.handle.block_on(async {
            sqlx::query("SELECT nul, b, i, f, big::text as big, s, arr FROM hp_test_bind_all")
                .fetch_one(&pool)
                .await
                .unwrap()
        });
        assert_eq!(row.get::<Option<String>, _>("nul"), None);
        assert_eq!(row.get::<bool, _>("b"), true);
        assert_eq!(row.get::<i64, _>("i"), -42);
        assert_eq!(row.get::<f64, _>("f"), 1.5);
        assert_eq!(row.get::<String, _>("big"), "123456789012345678901234567890");
        assert_eq!(row.get::<String, _>("s"), "hello");
        assert_eq!(row.get::<String, _>("arr"), r#"["a","b"]"#);
    }

    #[test]
    fn empty_params_bind_nothing() {
        let Some(h) = pg_harness("hp_test_noparams") else { return };
        let mut st = h.state("pg_sink", &[], &["main"]);
        let n = st
            .sql_exec(
                "main".into(),
                "INSERT INTO hp_test_noparams (id, v) VALUES (1, 'x')".into(),
                vec![],
            )
            .unwrap()
            .expect("ok");
        assert_eq!(n, 1);
    }
}
