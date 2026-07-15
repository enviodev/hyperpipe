//! A scripted HTTP server for tests (test-only; no extra dependencies).
//!
//! Shared by every crate whose tests need a real HTTP peer. Dev-dependency
//! only — never linked into a shipped binary.
//!
//! The source layer's whole job is turning HTTP responses into batches, so the
//! tests that matter need a real socket on the other end — not a stubbed client.
//! This is the smallest thing that does that: bind port 0, speak enough HTTP/1.1
//! to satisfy `reqwest`, and answer every request from a caller-supplied closure
//! that sees the path and the parsed JSON body.
//!
//! Every response carries `connection: close`, so each request is one
//! connection and the handler never has to deal with keep-alive framing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// How the response frames its body.
enum Framing {
    /// `content-length: <body.len()>` — the normal case.
    Length,
    /// `content-length: <n>` regardless of what is actually sent. Lets a test
    /// drive a client's "declared body is too big" check without shipping the
    /// bytes.
    DeclaredLength(u64),
    /// No `content-length` at all: the body ends when the socket closes. This
    /// is how a client is forced to discover an oversized body by reading it.
    UntilEof,
}

/// What a handler returns: an HTTP status and a raw body.
pub struct Reply {
    pub status: u16,
    pub body: String,
    framing: Framing,
    extra_headers: Vec<(String, String)>,
}

impl Reply {
    pub fn json(v: Value) -> Reply {
        Reply::raw(&v.to_string())
    }
    pub fn status(status: u16, v: Value) -> Reply {
        Reply { status, ..Reply::raw(&v.to_string()) }
    }
    /// A 200 whose body is an arbitrary string (not necessarily JSON).
    pub fn raw(body: &str) -> Reply {
        Reply {
            status: 200,
            body: body.to_string(),
            framing: Framing::Length,
            extra_headers: Vec::new(),
        }
    }
    /// Claim a `content-length` of `n` without sending that many bytes.
    pub fn declaring_length(n: u64) -> Reply {
        Reply { framing: Framing::DeclaredLength(n), ..Reply::raw("") }
    }
    /// Send `body` with no `content-length`, terminated by the socket close.
    pub fn until_eof(body: String) -> Reply {
        Reply { framing: Framing::UntilEof, ..Reply::raw(&body) }
    }
    pub fn with_header(mut self, k: &str, v: &str) -> Reply {
        self.extra_headers.push((k.to_string(), v.to_string()));
        self
    }
}

/// One recorded request.
#[derive(Clone, Debug)]
pub struct Recorded {
    pub path: String,
    pub body: Value,
    pub auth: Option<String>,
}

pub struct MockServer {
    pub url: String,
    requests: Arc<Mutex<Vec<Recorded>>>,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MockServer {
    /// Start a server whose handler sees `(path, body, call_index)` — the index
    /// is what lets a test script "fail twice, then succeed".
    pub async fn start<F>(handler: F) -> MockServer
    where
        F: Fn(&str, &Value, usize) -> Reply + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock server");
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));

        let (reqs, n, h) = (requests.clone(), calls.clone(), Arc::new(handler));
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { continue };
                let (reqs, n, h) = (reqs.clone(), n.clone(), h.clone());
                tokio::spawn(async move {
                    let Some(req) = read_request(&mut sock).await else { return };
                    let idx = n.fetch_add(1, Ordering::SeqCst);
                    reqs.lock().unwrap().push(req.clone());
                    let reply = h(&req.path, &req.body, idx);
                    let reason = if reply.status < 300 { "OK" } else { "Error" };
                    let mut head = format!(
                        "HTTP/1.1 {} {reason}\r\ncontent-type: application/json\r\nconnection: close\r\n",
                        reply.status
                    );
                    match reply.framing {
                        Framing::Length => {
                            head.push_str(&format!("content-length: {}\r\n", reply.body.len()))
                        }
                        Framing::DeclaredLength(n) => {
                            head.push_str(&format!("content-length: {n}\r\n"))
                        }
                        Framing::UntilEof => {}
                    }
                    for (k, v) in &reply.extra_headers {
                        head.push_str(&format!("{k}: {v}\r\n"));
                    }
                    head.push_str("\r\n");
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.write_all(reply.body.as_bytes()).await;
                    let _ = sock.flush().await;
                    // Closing is what terminates an UntilEof body.
                    let _ = sock.shutdown().await;
                });
            }
        });

        MockServer { url: format!("http://{addr}"), requests, calls, task }
    }

    /// Every request received so far, in order.
    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// The bodies of `/query` requests only (the ones tests usually assert on).
    pub fn query_bodies(&self) -> Vec<Value> {
        self.requests()
            .into_iter()
            .filter(|r| r.path == "/query")
            .map(|r| r.body)
            .collect()
    }
}

/// Read one HTTP request: request line, headers, then `content-length` bytes.
async fn read_request(sock: &mut tokio::net::TcpStream) -> Option<Recorded> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    // Headers first.
    let head_end = loop {
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let path = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .to_string();
    let header = |name: &str| -> Option<String> {
        head.lines()
            .find(|l| l.to_ascii_lowercase().starts_with(&format!("{name}:")))
            .map(|l| l[name.len() + 1..].trim().to_string())
    };
    let len: usize = header("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);

    // Then the body.
    let mut body = buf[head_end..].to_vec();
    while body.len() < len {
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);

    Some(Recorded { path, body, auth: header("authorization") })
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}
