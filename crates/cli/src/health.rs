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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// `STATE` and `HYPERPIPE_HEALTH_PORT` are process-global.
    static LOCK: Mutex<()> = Mutex::new(());

    /// Bind an ephemeral port and serve on it; returns the port.
    async fn spawn_server() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // free it for serve() to take (racy in theory, fine here)
        tokio::spawn(async move {
            let _ = serve(port).await;
        });
        // Wait for the listener to come up rather than sleeping blind.
        for _ in 0..100 {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return port;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("health server never came up on port {port}");
    }

    /// GET `path` and return (status code, body).
    async fn get(port: u16, path: &str) -> (u16, String) {
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.expect("connect");
        sock.write_all(format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut resp = String::new();
        sock.read_to_string(&mut resp).await.unwrap();
        let code: u16 = resp
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        let body = resp.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (code, body)
    }

    #[tokio::test]
    async fn no_env_var_means_no_health_server() {
        let _g = LOCK.lock().unwrap();
        let saved = std::env::var_os("HYPERPIPE_HEALTH_PORT");
        std::env::remove_var("HYPERPIPE_HEALTH_PORT");
        assert!(maybe_spawn().is_none(), "the probe server is opt-in");
        if let Some(v) = saved {
            std::env::set_var("HYPERPIPE_HEALTH_PORT", v);
        }
    }

    #[tokio::test]
    async fn a_junk_port_disables_the_server_without_killing_the_pipeline() {
        // A typo in the env var must not take the whole pipeline down.
        let _g = LOCK.lock().unwrap();
        let saved = std::env::var_os("HYPERPIPE_HEALTH_PORT");
        std::env::set_var("HYPERPIPE_HEALTH_PORT", "not-a-port");
        assert!(maybe_spawn().is_none());
        std::env::set_var("HYPERPIPE_HEALTH_PORT", "99999"); // out of u16 range
        assert!(maybe_spawn().is_none());
        match saved {
            Some(v) => std::env::set_var("HYPERPIPE_HEALTH_PORT", v),
            None => std::env::remove_var("HYPERPIPE_HEALTH_PORT"),
        }
    }

    #[tokio::test]
    async fn a_valid_port_spawns_the_server() {
        let _g = LOCK.lock().unwrap();
        let saved = std::env::var_os("HYPERPIPE_HEALTH_PORT");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        std::env::set_var("HYPERPIPE_HEALTH_PORT", port.to_string());
        let handle = maybe_spawn().expect("spawned");
        handle.abort();
        match saved {
            Some(v) => std::env::set_var("HYPERPIPE_HEALTH_PORT", v),
            None => std::env::remove_var("HYPERPIPE_HEALTH_PORT"),
        }
    }

    /// The full K8s contract, in lifecycle order, against one live server.
    #[tokio::test]
    async fn probes_track_the_pipeline_lifecycle() {
        let _g = LOCK.lock().unwrap();
        let port = spawn_server().await;

        // STARTING: alive, but not ready for traffic.
        set_state(STARTING);
        assert_eq!(get(port, "/healthz").await, (200, "ok".into()));
        assert_eq!(get(port, "/readyz").await, (503, "starting".into()));

        // RUNNING: both green.
        set_state(RUNNING);
        assert_eq!(get(port, "/healthz").await, (200, "ok".into()));
        assert_eq!(get(port, "/readyz").await, (200, "ready".into()));

        // STOPPING (draining after SIGTERM): liveness goes red so the pod is
        // pulled out of rotation rather than restarted mid-drain.
        set_state(STOPPING);
        assert_eq!(get(port, "/healthz").await.0, 503);
        assert_eq!(get(port, "/readyz").await.0, 503);

        set_state(RUNNING);
    }

    #[tokio::test]
    async fn unknown_paths_answer_like_healthz() {
        let _g = LOCK.lock().unwrap();
        let port = spawn_server().await;
        set_state(RUNNING);
        assert_eq!(get(port, "/").await, (200, "ok".into()));
        assert_eq!(get(port, "/anything").await, (200, "ok".into()));
        // ...including while draining
        set_state(STOPPING);
        assert_eq!(get(port, "/anything").await.0, 503);
        set_state(RUNNING);
    }

    #[tokio::test]
    async fn readyz_matches_on_the_path_prefix() {
        // kubelet appends nothing, but a query string must still route.
        let _g = LOCK.lock().unwrap();
        let port = spawn_server().await;
        set_state(RUNNING);
        assert_eq!(get(port, "/readyz?probe=1").await, (200, "ready".into()));
    }
}
