//! Minimal HTTP health server for Kubernetes probes. No extra deps — raw TCP.
//!
//! Enabled by setting `HYPERPIPE_HEALTH_PORT`. Endpoints:
//!   GET /healthz  -> 200 while alive, 503 once shutting down   (livenessProbe)
//!   GET /readyz   -> 200 once running,  503 while starting      (readinessProbe)

use std::sync::atomic::{AtomicU8, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub const STARTING: u8 = 0;
pub const RUNNING: u8 = 1;
pub const STOPPING: u8 = 2;

static STATE: AtomicU8 = AtomicU8::new(STARTING);

pub fn set_state(s: u8) {
    STATE.store(s, Ordering::Relaxed);
}

/// Spawn the health server if `HYPERPIPE_HEALTH_PORT` is set. Returns the join
/// handle (aborted on shutdown) or None.
pub fn maybe_spawn() -> Option<tokio::task::JoinHandle<()>> {
    let raw = std::env::var("HYPERPIPE_HEALTH_PORT").ok()?;
    let port: u16 = match raw.parse() {
        Ok(p) => p,
        Err(_) => {
            tracing::warn!(
                "HYPERPIPE_HEALTH_PORT=`{raw}` is not a valid port; health server disabled"
            );
            return None;
        }
    };
    Some(tokio::spawn(async move {
        if let Err(e) = serve(port).await {
            tracing::warn!("health server stopped: {e}");
        }
    }))
}

async fn serve(port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "health server listening (/healthz, /readyz)");
    loop {
        // Transient accept errors (e.g. EMFILE under fd pressure) must not
        // kill the probe endpoints for the rest of the pod's life.
        let (mut sock, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!("health server accept error: {e}; retrying");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = sock.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req.split_whitespace().nth(1).unwrap_or("/");
            let state = STATE.load(Ordering::Relaxed);
            let (code, body) = if path.starts_with("/readyz") {
                if state == RUNNING {
                    (200, "ready")
                } else {
                    (503, "starting")
                }
            } else {
                // /healthz or anything else: alive unless shutting down
                if state == STOPPING {
                    (503, "shutting down")
                } else {
                    (200, "ok")
                }
            };
            let reason = if code == 200 { "OK" } else { "Service Unavailable" };
            let resp = format!(
                "HTTP/1.1 {code} {reason}\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
    }
}
