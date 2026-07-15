//! HyperSync source layer (§5): a per-source cursor loop that turns a chain +
//! query into a stream of batch envelopes.
//!
//! Native (not WASM) on purpose — ingestion is the performance edge and must
//! not sit behind a serialization boundary. Talks to the HyperSync HTTP `/query`
//! endpoint; set `HYPERSYNC_BEARER_TOKEN` (create one at app.envio.dev/api-tokens).

mod client;
mod reorg;

pub use client::{HyperSyncClient, QueryResponse, RollbackGuard};
pub use reorg::ReorgTracker;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use hp_encoding::{Batch, BatchKind, BlockRange, ControlRecord, KvStore};
use hp_engine::config::{Source, SourceMode};
use tokio::sync::mpsc::Sender;

/// A configured, ready-to-run HyperSync source.
pub struct HyperSyncSource {
    pub name: String,
    chain_id: u64,
    client: HyperSyncClient,
    mode: SourceMode,
    from_block: u64,
    to_block: Option<u64>,
    confirmations: u64,
    reorg_enabled: bool,
    reorg_window: u64,
    max_records: usize,
    poll_interval: Duration,
}

impl HyperSyncSource {
    /// Build from a validated config source. `default_max_records` comes from
    /// the resource profile when the source omits `batch.max_records`.
    pub fn from_config(src: &Source, default_max_records: usize) -> Self {
        let ep = src.endpoint();
        let token = std::env::var("HYPERSYNC_BEARER_TOKEN").ok();
        let client = HyperSyncClient::new(ep.url.clone(), token, &src.query, src.reorg.enabled);
        HyperSyncSource {
            name: src.name.clone(),
            chain_id: ep.chain_id,
            client,
            mode: src.mode,
            from_block: src.from_block.unwrap_or(0),
            to_block: src.to_block,
            confirmations: src.confirmations,
            reorg_enabled: src.reorg.enabled,
            reorg_window: src.reorg.window.max(1),
            max_records: src.batch.max_records.unwrap_or(default_max_records).max(1),
            poll_interval: Duration::from_millis(500),
        }
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Starting cursor for a fresh run (no checkpoint). `backfill` starts at 0
    /// unless `from_block` set; `live`/`both` start at `from_block` (or head,
    /// resolved lazily on first poll when 0).
    pub fn default_start(&self) -> u64 {
        self.from_block
    }

    /// Run the cursor loop until EOF (backfill) or `stop` is set. Batches are
    /// sent on `tx`; a full channel blocks the send, which is the backpressure
    /// signal back to ingestion. `kv` (when given) persists the reorg hash
    /// window across restarts, keyed by source name.
    pub async fn run(
        &self,
        start_cursor: u64,
        tx: Sender<Batch>,
        stop: Arc<AtomicBool>,
        kv: Option<Arc<dyn KvStore>>,
    ) -> Result<()> {
        let mut cursor = start_cursor.max(self.from_block);
        let mut backoff = Duration::from_millis(250);
        // KvStore::get may block on IO (the checkpoint store runs a SQLite
        // read through block_on) — load off the async runtime.
        let mut tracker = if self.reorg_enabled {
            let (window, name, kv2) = (self.reorg_window, self.name.clone(), kv.clone());
            Some(
                tokio::task::spawn_blocking(move || ReorgTracker::load(window, &name, kv2.as_deref()))
                    .await
                    .unwrap_or_else(|_| ReorgTracker::new(window)),
            )
        } else {
            None
        };

        // `live` with from_block unset (0) means "start at head".
        if cursor == 0 && matches!(self.mode, SourceMode::Live) {
            if let Ok(h) = self.client.archive_height().await {
                cursor = h.saturating_sub(self.confirmations);
            }
        }

        loop {
            if stop.load(Ordering::Relaxed) {
                return Ok(());
            }

            let mut resp = match self.client.query(cursor, self.to_block).await {
                Ok(r) => {
                    backoff = Duration::from_millis(250);
                    r
                }
                Err(e) => {
                    tracing::warn!(source = %self.name, cursor, "hypersync query failed: {e}; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    continue;
                }
            };

            // Reorg check BEFORE anything from this response is emitted: if a
            // stored hash conflicts with what the chain now says, locate the
            // fork, tell downstream to invalidate, and rewind the cursor.
            if let Some(t) = tracker.as_mut() {
                if t.conflict(&resp).is_some() {
                    let fork = self.locate_fork(t, cursor).await;
                    tracing::warn!(
                        source = %self.name,
                        fork_block = fork,
                        cursor,
                        "reorg detected; rolling back blocks > {fork}"
                    );
                    t.purge_after(fork);
                    t.save(&self.name, kv.as_deref());
                    let rb = Batch::control(
                        self.name.clone(),
                        self.chain_id,
                        ControlRecord::Rollback {
                            chain_id: self.chain_id,
                            invalidate_after_block: fork,
                        },
                    );
                    if tx.send(rb).await.is_err() {
                        return Ok(()); // pipeline shutting down
                    }
                    cursor = fork.saturating_add(1).max(self.from_block);
                    continue;
                }
            }

            match plan_step(
                self.mode,
                cursor,
                self.to_block,
                self.confirmations,
                resp.next_block,
                resp.archive_height,
            ) {
                Step::Wait => {
                    tokio::time::sleep(self.poll_interval).await;
                }
                Step::Eof => {
                    self.emit_eof(&tx, cursor).await?;
                    return Ok(());
                }
                Step::Emit { effective_next } => {
                    // Truncate to the confirmed window (§5.1): records beyond
                    // `effective_next` were fetched but may still reorg — they
                    // are dropped here and re-fetched once confirmed.
                    let mut records = std::mem::take(&mut resp.records);
                    if effective_next < resp.next_block {
                        records.retain(|r| {
                            block_number_of(r).map_or(true, |b| b < effective_next)
                        });
                    }

                    let chunks: Vec<&[serde_json::Value]> =
                        records.chunks(self.max_records).collect();
                    let n = chunks.len();
                    for (seq, chunk) in chunks.into_iter().enumerate() {
                        let mut batch = Batch::new(
                            self.name.clone(),
                            self.chain_id,
                            BlockRange(cursor, effective_next),
                            seq as u64,
                            BatchKind::Log,
                            chunk.to_vec(),
                        );
                        // §8.2 rule 1: only the final chunk of a range may
                        // advance the cursor; earlier chunks pin it to the
                        // range start so a crash mid-range replays everything.
                        if seq + 1 < n {
                            batch.ack_block = Some(cursor);
                        }
                        if tx.send(batch).await.is_err() {
                            // Receiver dropped -> pipeline shutting down.
                            return Ok(());
                        }
                    }

                    cursor = effective_next;

                    // Remember hashes for the blocks just emitted; next poll's
                    // rollback guard is checked against these.
                    if let Some(t) = tracker.as_mut() {
                        t.record(&resp, effective_next);
                        t.save(&self.name, kv.as_deref());
                    }

                    if matches!(self.mode, SourceMode::Backfill)
                        && matches!(self.to_block, Some(end) if cursor >= end)
                    {
                        self.emit_eof(&tx, cursor).await?;
                        return Ok(());
                    }
                    // `both`: once past to_block/head it degrades to Wait via plan_step.
                }
            }
        }
    }

    /// Last canonical block we can still trust after a detected reorg. Fetches
    /// the live hash chain over the tracked window and walks stored hashes
    /// newest-first until one matches. Falls back to the window start (replay
    /// everything tracked) when the chain can't be re-fetched — coarse but safe.
    async fn locate_fork(&self, tracker: &ReorgTracker, cursor: u64) -> u64 {
        let coarse = || {
            tracker
                .min_block()
                .map(|m| m.saturating_sub(1))
                .unwrap_or_else(|| cursor.saturating_sub(self.reorg_window + 1))
        };
        let from = match tracker.min_block() {
            Some(m) => m,
            None => return coarse(),
        };
        match self.client.block_hashes(from, cursor).await {
            Ok(canonical) => {
                let canon: std::collections::HashMap<u64, String> = canonical.into_iter().collect();
                tracker.last_matching(&canon).unwrap_or_else(coarse)
            }
            Err(e) => {
                tracing::warn!(
                    source = %self.name,
                    "could not fetch canonical hashes to locate fork ({e}); \
                     rewinding the full tracked window"
                );
                coarse()
            }
        }
    }

    async fn emit_eof(&self, tx: &Sender<Batch>, at: u64) -> Result<()> {
        let eof = Batch::control(
            self.name.clone(),
            self.chain_id,
            ControlRecord::Eof {
                source: self.name.clone(),
                at_block: at,
            },
        );
        let _ = tx.send(eof).await;
        tracing::info!(source = %self.name, at_block = at, "backfill complete (EOF)");
        Ok(())
    }
}

/// What the cursor loop should do after one query response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Nothing safe to emit yet — sleep and re-poll.
    Wait,
    /// Emit records with `block_number < effective_next`, advance the cursor there.
    Emit { effective_next: u64 },
    /// Backfill finished its configured range.
    Eof,
}

/// Pure planning for one iteration of the source loop (§5.1). Decides how far
/// the cursor may advance given the response, the confirmation lag, and the
/// configured range — and when a backfill is actually done.
///
/// Invariants encoded here:
/// - never emit past `archive_height - confirmations` (reorg window),
/// - never emit past `to_block`,
/// - backfill EOFs ONLY at `to_block` — a stalled server (`next_block <=
///   cursor`) means wait-and-retry, not a silent early EOF.
pub fn plan_step(
    mode: SourceMode,
    cursor: u64,
    to_block: Option<u64>,
    confirmations: u64,
    resp_next_block: u64,
    archive_height: Option<u64>,
) -> Step {
    // Exclusive upper bound imposed by the confirmation lag: the last safe
    // block is `height - confirmations`, so the bound is one past it.
    let confirmed_bound = archive_height.map(|h| {
        if h >= confirmations {
            h - confirmations + 1
        } else {
            0
        }
    });

    let mut effective_next = resp_next_block;
    if let Some(b) = confirmed_bound {
        effective_next = effective_next.min(b);
    }
    if let Some(t) = to_block {
        effective_next = effective_next.min(t);
    }

    if effective_next > cursor {
        return Step::Emit { effective_next };
    }

    // No safe forward progress this round.
    match mode {
        SourceMode::Backfill if matches!(to_block, Some(end) if cursor >= end) => Step::Eof,
        _ => Step::Wait,
    }
}

/// Extract a record's block number (JSON number, decimal string, or 0x-hex).
fn block_number_of(rec: &serde_json::Value) -> Option<u64> {
    match rec.get("block_number")? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => {
            if let Some(h) = s.strip_prefix("0x") {
                u64::from_str_radix(h, 16).ok()
            } else {
                s.parse().ok()
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    const LIVE: SourceMode = SourceMode::Live;
    const BACKFILL: SourceMode = SourceMode::Backfill;

    #[test]
    fn emits_up_to_response_when_within_window() {
        // head 1000, conf 10 -> bound 991; response only reaches 500.
        assert_eq!(
            plan_step(LIVE, 100, None, 10, 500, Some(1000)),
            Step::Emit { effective_next: 500 }
        );
    }

    #[test]
    fn confirmations_clamp_the_emit_window() {
        // head 1000, conf 10 -> last safe block 990, bound 991. Response
        // claims blocks up to 1000 — we must NOT emit past the window.
        assert_eq!(
            plan_step(LIVE, 985, None, 10, 1000, Some(1000)),
            Step::Emit { effective_next: 991 }
        );
    }

    #[test]
    fn waits_at_confirmed_head_instead_of_emitting_unconfirmed() {
        // cursor already at the bound: nothing safe to emit.
        assert_eq!(plan_step(LIVE, 991, None, 10, 1000, Some(1000)), Step::Wait);
    }

    #[test]
    fn to_block_clamps_the_emit_window() {
        assert_eq!(
            plan_step(BACKFILL, 100, Some(150), 0, 500, Some(1000)),
            Step::Emit { effective_next: 150 }
        );
    }

    #[test]
    fn backfill_eofs_only_at_to_block() {
        assert_eq!(plan_step(BACKFILL, 150, Some(150), 0, 150, Some(1000)), Step::Eof);
        assert_eq!(plan_step(BACKFILL, 200, Some(150), 0, 200, Some(1000)), Step::Eof);
    }

    #[test]
    fn backfill_stall_waits_instead_of_premature_eof() {
        // Server made no progress (next == cursor) but the range is NOT done:
        // must wait-and-retry, never EOF early (that would silently drop data).
        assert_eq!(plan_step(BACKFILL, 100, Some(150), 0, 100, Some(1000)), Step::Wait);
    }

    #[test]
    fn backfill_waits_for_chain_to_reach_to_block() {
        // to_block is beyond the confirmed head: wait for the chain, no EOF.
        assert_eq!(
            plan_step(BACKFILL, 995, Some(2000), 10, 1000, Some(1000)),
            Step::Wait
        );
    }

    #[test]
    fn missing_height_trusts_the_response() {
        assert_eq!(
            plan_step(LIVE, 100, None, 10, 200, None),
            Step::Emit { effective_next: 200 }
        );
    }

    #[test]
    fn tiny_chain_below_confirmations_emits_nothing() {
        // height 5, conf 10 -> bound 0 -> nothing is confirmed yet.
        assert_eq!(plan_step(LIVE, 0, None, 10, 5, Some(5)), Step::Wait);
    }

    #[test]
    fn block_number_parsing_variants() {
        use serde_json::json;
        assert_eq!(block_number_of(&json!({"block_number": 42})), Some(42));
        assert_eq!(block_number_of(&json!({"block_number": "42"})), Some(42));
        assert_eq!(block_number_of(&json!({"block_number": "0x2a"})), Some(42));
        assert_eq!(block_number_of(&json!({"other": 1})), None);
        assert_eq!(block_number_of(&json!({"block_number": "junk"})), None);
        assert_eq!(block_number_of(&json!({"block_number": true})), None);
    }
}

/// The cursor loop, driven against a scripted HTTP server.
#[cfg(test)]
mod run_tests {
    use super::*;
    use hp_testutil::{MockServer, Reply};
    use hp_engine::config::{BatchCfg, Query, ReorgCfg, ResolvedEndpoint, Source};
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::sync::mpsc::{channel, Receiver};

    #[derive(Default)]
    struct MemKv(Mutex<HashMap<(String, String), Vec<u8>>>);
    impl KvStore for MemKv {
        fn get(&self, m: &str, k: &str) -> Option<Vec<u8>> {
            self.0.lock().unwrap().get(&(m.into(), k.into())).cloned()
        }
        fn set(&self, m: &str, k: &str, v: Vec<u8>) {
            self.0.lock().unwrap().insert((m.into(), k.into()), v);
        }
    }

    /// A validated-looking source config pointed at the mock server. Mirrors
    /// what `Config::validate` would have produced (`resolved` filled in).
    fn source_cfg(url: &str, mode: SourceMode) -> Source {
        Source {
            name: "eth".into(),
            r#type: "hypersync".into(),
            chain: None,
            chain_id: Some(1),
            url: Some(url.to_string()),
            mode,
            from_block: Some(100),
            to_block: None,
            confirmations: 0,
            reorg: ReorgCfg { enabled: false, window: 64 },
            batch: BatchCfg { max_records: Some(1_000), max_interval_ms: 500 },
            query: Query::default(),
            resolved: Some(ResolvedEndpoint { chain_id: 1, url: url.to_string() }),
        }
    }

    fn logs(blocks: &[u64]) -> Value {
        Value::Array(
            blocks
                .iter()
                .map(|b| json!({ "block_number": b, "log_index": 0 }))
                .collect(),
        )
    }

    /// Drain a receiver into a vec (the loop has already finished or stopped).
    fn drain(rx: &mut Receiver<Batch>) -> Vec<Batch> {
        let mut out = Vec::new();
        while let Ok(b) = rx.try_recv() {
            out.push(b);
        }
        out
    }

    #[tokio::test]
    async fn backfill_pages_chunks_and_eofs() {
        // 100..106 in two pages of three blocks, chunked at 2 records.
        let srv = MockServer::start(|_p, body, _i| {
            match body["from_block"].as_u64().unwrap() {
                100 => Reply::json(json!({ "archive_height": 19_000_000, "next_block": 103,
                                           "data": { "logs": logs(&[100, 101, 102]) } })),
                103 => Reply::json(json!({ "archive_height": 19_000_000, "next_block": 106,
                                           "data": { "logs": logs(&[103, 104, 105]) } })),
                other => panic!("unexpected from_block {other}"),
            }
        })
        .await;

        let mut cfg = source_cfg(&srv.url, SourceMode::Backfill);
        cfg.to_block = Some(106);
        cfg.batch.max_records = Some(2);
        let source = HyperSyncSource::from_config(&cfg, 1_000);
        assert_eq!(source.chain_id(), 1);
        assert_eq!(source.default_start(), 100);

        let (tx, mut rx) = channel(16);
        let stop = Arc::new(AtomicBool::new(false));
        source.run(100, tx, stop, None).await.expect("run to EOF");

        let batches = drain(&mut rx);
        // 2 pages x 2 chunks + EOF
        assert_eq!(batches.len(), 5, "got {:?}", batches.iter().map(|b| &b.batch_id).collect::<Vec<_>>());

        // Page 1: chunk 0 is non-final -> its ack pins to the range START, so a
        // crash between chunk acks replays the whole page (§8.2 rule 1).
        assert_eq!(batches[0].block_range, BlockRange(100, 103));
        assert_eq!(batches[0].len(), 2);
        assert_eq!(batches[0].ack_block, Some(100));
        assert_eq!(batches[0].batch_id, "eth:100:103:0");
        // chunk 1 is final -> it may advance the cursor to the range end
        assert_eq!(batches[1].len(), 1);
        assert_eq!(batches[1].ack_block, None);
        assert_eq!(batches[1].ack_block(), 103);
        assert_eq!(batches[1].batch_id, "eth:100:103:1");

        // Page 2, same shape, starting where page 1 ended.
        assert_eq!(batches[2].block_range, BlockRange(103, 106));
        assert_eq!(batches[2].ack_block, Some(103));
        assert_eq!(batches[3].ack_block(), 106);

        // EOF closes the range so job-mode can terminate.
        assert!(batches[4].is_control());
        assert_eq!(
            batches[4].as_control(),
            Some(ControlRecord::Eof { source: "eth".into(), at_block: 106 })
        );
        for b in &batches[..4] {
            assert_eq!(b.kind, BatchKind::Log);
            assert_eq!(b.chain_id, 1);
        }
    }

    #[tokio::test]
    async fn a_single_chunk_page_is_not_pinned() {
        let srv = MockServer::start(|_p, _b, i| {
            if i == 0 {
                Reply::json(json!({ "next_block": 110, "data": { "logs": logs(&[100]) } }))
            } else {
                Reply::json(json!({ "next_block": 110, "data": { "logs": [] } }))
            }
        })
        .await;
        let mut cfg = source_cfg(&srv.url, SourceMode::Backfill);
        cfg.to_block = Some(110);
        let source = HyperSyncSource::from_config(&cfg, 1_000);
        let (tx, mut rx) = channel(16);
        source.run(100, tx, Arc::new(AtomicBool::new(false)), None).await.unwrap();

        let batches = drain(&mut rx);
        assert_eq!(batches[0].ack_block, None, "the only chunk of a range is the final one");
        assert_eq!(batches[0].ack_block(), 110);
    }

    #[tokio::test]
    async fn a_query_error_backs_off_and_retries_without_moving_the_cursor() {
        // Fail twice, then serve. A retry that advanced the cursor would skip
        // the blocks the failed queries were supposed to fetch.
        let srv = MockServer::start(|_p, _b, i| match i {
            0 | 1 => Reply::status(500, json!({ "error": "upstream boom" })),
            _ => Reply::json(json!({ "next_block": 110, "data": { "logs": logs(&[100]) } })),
        })
        .await;
        let mut cfg = source_cfg(&srv.url, SourceMode::Backfill);
        cfg.to_block = Some(110);
        let source = HyperSyncSource::from_config(&cfg, 1_000);

        let (tx, mut rx) = channel(16);
        source.run(100, tx, Arc::new(AtomicBool::new(false)), None).await.unwrap();

        assert_eq!(srv.call_count(), 3, "two failures then one success");
        for body in srv.query_bodies() {
            assert_eq!(body["from_block"], 100, "the cursor must not move across failures");
        }
        let batches = drain(&mut rx);
        assert_eq!(batches[0].len(), 1);
        assert!(batches[1].is_control());
    }

    #[tokio::test]
    async fn a_set_stop_flag_returns_before_querying() {
        let srv = MockServer::start(|_p, _b, _i| panic!("must not query after stop")).await;
        let cfg = source_cfg(&srv.url, SourceMode::Live);
        let source = HyperSyncSource::from_config(&cfg, 1_000);

        let (tx, mut rx) = channel(16);
        let stop = Arc::new(AtomicBool::new(true));
        source.run(100, tx, stop, None).await.unwrap();

        assert_eq!(srv.call_count(), 0);
        assert!(drain(&mut rx).is_empty(), "a stopped source emits no EOF — the range isn't done");
    }

    #[tokio::test]
    async fn stopping_mid_stream_ends_the_loop_without_an_eof() {
        // A live source that keeps producing: stop after the first batch.
        let srv = MockServer::start(|_p, body, _i| {
            let from = body["from_block"].as_u64().unwrap();
            Reply::json(json!({ "next_block": from + 1, "data": { "logs": logs(&[from]) } }))
        })
        .await;
        let cfg = source_cfg(&srv.url, SourceMode::Live);
        let source = HyperSyncSource::from_config(&cfg, 1_000);

        let (tx, mut rx) = channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let runner = tokio::spawn(async move { source.run(100, tx, stop2, None).await });

        let first = rx.recv().await.expect("a batch");
        assert_eq!(first.block_range, BlockRange(100, 101));
        stop.store(true, Ordering::Relaxed);
        while rx.recv().await.is_some() {} // drain so the loop isn't wedged on send

        let out = tokio::time::timeout(Duration::from_secs(5), runner)
            .await
            .expect("run must observe the stop flag promptly")
            .unwrap();
        assert!(out.is_ok());
    }

    #[tokio::test]
    async fn a_dropped_receiver_shuts_the_source_down_cleanly() {
        let srv = MockServer::start(|_p, _b, _i| {
            Reply::json(json!({ "next_block": 200, "data": { "logs": logs(&[100]) } }))
        })
        .await;
        let cfg = source_cfg(&srv.url, SourceMode::Live);
        let source = HyperSyncSource::from_config(&cfg, 1_000);

        let (tx, rx) = channel(1);
        drop(rx); // the pipeline went away mid-stream
        let out = tokio::time::timeout(
            Duration::from_secs(5),
            source.run(100, tx, Arc::new(AtomicBool::new(false)), None),
        )
        .await
        .expect("must not hang on a dead receiver");
        assert!(out.is_ok(), "a shutdown is not an error: {out:?}");
    }

    #[tokio::test]
    async fn live_from_zero_starts_at_the_confirmed_head() {
        // `from_block` unset in live mode means "start at the head", minus the
        // confirmation lag — not block 0 (which would replay all of history).
        let srv = MockServer::start(|path, body, _i| {
            if path == "/height" {
                return Reply::json(json!({ "height": 1_000 }));
            }
            assert_eq!(body["from_block"], 990, "head 1000 - 10 confirmations");
            Reply::json(json!({ "archive_height": 1_000, "next_block": 991,
                                "data": { "logs": logs(&[990]) } }))
        })
        .await;
        let mut cfg = source_cfg(&srv.url, SourceMode::Live);
        cfg.from_block = None;
        cfg.confirmations = 10;
        let source = HyperSyncSource::from_config(&cfg, 1_000);

        let (tx, mut rx) = channel(4);
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let runner = tokio::spawn(async move { source.run(0, tx, stop2, None).await });

        let b = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a batch")
            .unwrap();
        assert_eq!(b.block_range, BlockRange(990, 991));
        stop.store(true, Ordering::Relaxed);
        while rx.recv().await.is_some() {}
        let _ = runner.await;
    }

    #[tokio::test]
    async fn records_past_the_confirmation_window_are_held_back() {
        // The server happily returns blocks up to the head; anything above
        // head - confirmations may still reorg, so it must not be emitted yet.
        let srv = MockServer::start(|_p, _b, _i| {
            Reply::json(json!({
                "archive_height": 1_000, "next_block": 1_000,
                "data": { "logs": logs(&[985, 990, 991, 995]) }
            }))
        })
        .await;
        let mut cfg = source_cfg(&srv.url, SourceMode::Live);
        cfg.confirmations = 10; // last safe block 990 -> exclusive bound 991
        let source = HyperSyncSource::from_config(&cfg, 1_000);

        let (tx, mut rx) = channel(4);
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let runner = tokio::spawn(async move { source.run(100, tx, stop2, None).await });

        let b = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a batch")
            .unwrap();
        assert_eq!(b.block_range, BlockRange(100, 991), "cursor stops at the confirmed bound");
        let emitted: Vec<u64> = b
            .records
            .iter()
            .map(|r| r["block_number"].as_u64().unwrap())
            .collect();
        assert_eq!(emitted, vec![985, 990], "991 and 995 are unconfirmed — re-fetched later");

        stop.store(true, Ordering::Relaxed);
        while rx.recv().await.is_some() {}
        let _ = runner.await;
    }

    // ---- reorg handling ----------------------------------------------------

    /// Page 1 emits 100..102 with known hashes; page 2 comes back with block 101
    /// re-hashed (a reorg), and the canonical sweep says 100 survived. Once the
    /// fork has been reported the server serves the *corrected* chain — a real
    /// endpoint would, and a handler that kept replaying the forked hashes would
    /// make the loop re-detect the same reorg forever.
    fn reorg_handler(canonical_ok: bool) -> impl Fn(&str, &Value, usize) -> Reply + Send + Sync {
        let forked = Arc::new(AtomicBool::new(false));
        move |_p: &str, body: &Value, _i: usize| {
            let from = body["from_block"].as_u64().unwrap();
            // The fork-locating sweep is the headers-only query.
            if body["include_all_blocks"] == json!(true) {
                if !canonical_ok {
                    return Reply::status(500, json!({ "error": "no canonical data" }));
                }
                return Reply::json(json!({ "next_block": 103, "data": { "blocks": [
                    {"number": 100, "hash": "0xa"},
                    {"number": 101, "hash": "0xREORGED"},
                    {"number": 102, "hash": "0xALSO-REORGED"}
                ]}}));
            }
            // After the rewind: the post-fork chain, streamed to the end.
            if forked.load(Ordering::SeqCst) {
                return Reply::json(json!({ "archive_height": 19_000_000, "next_block": 110,
                    "data": { "blocks": [{"number": 101, "hash": "0xREORGED"},
                                         {"number": 105, "hash": "0xnew"}],
                              "logs": logs(&[101, 102, 105]) } }));
            }
            match from {
                100 => Reply::json(json!({ "archive_height": 19_000_000, "next_block": 103,
                    "data": { "blocks": [
                        {"number": 100, "hash": "0xa"},
                        {"number": 101, "hash": "0xb"},
                        {"number": 102, "hash": "0xc"}],
                        "logs": logs(&[100, 101, 102]) } })),
                // The reorg surfaces: block 101 now hashes differently.
                _ => {
                    forked.store(true, Ordering::SeqCst);
                    Reply::json(json!({ "archive_height": 19_000_000, "next_block": 106,
                        "data": { "blocks": [{"number": 101, "hash": "0xREORGED"}], "logs": [] } }))
                }
            }
        }
    }

    #[tokio::test]
    async fn a_reorg_emits_a_rollback_rewinds_and_replays() {
        let srv = MockServer::start(reorg_handler(true)).await;
        let mut cfg = source_cfg(&srv.url, SourceMode::Backfill);
        cfg.to_block = Some(110);
        cfg.reorg = ReorgCfg { enabled: true, window: 64 };
        let source = HyperSyncSource::from_config(&cfg, 1_000);

        let kv: Arc<dyn KvStore> = Arc::new(MemKv::default());
        let (tx, mut rx) = channel(16);
        tokio::time::timeout(
            Duration::from_secs(10),
            source.run(100, tx, Arc::new(AtomicBool::new(false)), Some(kv.clone())),
        )
        .await
        .expect("must finish")
        .unwrap();

        let batches = drain(&mut rx);
        let rollbacks: Vec<&Batch> = batches
            .iter()
            .filter(|b| matches!(b.as_control(), Some(ControlRecord::Rollback { .. })))
            .collect();
        assert_eq!(rollbacks.len(), 1, "exactly one rollback for one reorg");
        assert_eq!(
            rollbacks[0].as_control(),
            Some(ControlRecord::Rollback { chain_id: 1, invalidate_after_block: 100 }),
            "block 100 is the last one whose hash still matches the canonical chain"
        );

        // The rollback is emitted BEFORE any replayed data, so downstream sinks
        // delete the forked rows before the corrected ones arrive.
        let rb_pos = batches.iter().position(|b| matches!(b.as_control(), Some(ControlRecord::Rollback { .. }))).unwrap();
        let replay_pos = batches.iter().rposition(|b| !b.is_control()).unwrap();
        assert!(rb_pos < replay_pos, "rollback must precede the replayed batch");

        // The cursor rewound to fork+1: the next query re-fetches from 101.
        let froms: Vec<u64> = srv
            .requests()
            .iter()
            .filter(|r| r.body["include_all_blocks"] != json!(true))
            .map(|r| r.body["from_block"].as_u64().unwrap())
            .collect();
        assert_eq!(froms, vec![100, 103, 101], "queried, hit the fork, rewound to 101");

        // The purged window is persisted, so a restart cannot re-detect the
        // same reorg or trust the voided hashes.
        let saved = kv.get("__source/eth", "reorg_hashes").expect("window persisted");
        let pairs: Vec<(u64, String)> = serde_json::from_slice(&saved).unwrap();
        assert!(pairs.iter().all(|(n, _)| *n != 101 || pairs.iter().any(|(m, _)| *m == 105)),
                "the forked hash for 101 must not survive as `0xb`");
        assert_eq!(
            pairs.iter().find(|(n, _)| *n == 100).map(|(_, h)| h.as_str()),
            Some("0xa"),
            "the surviving block keeps its hash"
        );
        assert!(batches.last().unwrap().is_control(), "still terminates with EOF");
    }

    #[tokio::test]
    async fn a_reorg_with_no_canonical_data_rewinds_the_whole_window() {
        // Coarse fallback: if the fork can't be located exactly, replay
        // everything tracked rather than risk keeping forked rows.
        let srv = MockServer::start(reorg_handler(false)).await;
        let mut cfg = source_cfg(&srv.url, SourceMode::Backfill);
        cfg.to_block = Some(110);
        cfg.reorg = ReorgCfg { enabled: true, window: 64 };
        let source = HyperSyncSource::from_config(&cfg, 1_000);

        let (tx, mut rx) = channel(16);
        tokio::time::timeout(
            Duration::from_secs(10),
            source.run(100, tx, Arc::new(AtomicBool::new(false)), None),
        )
        .await
        .expect("must finish")
        .unwrap();

        let batches = drain(&mut rx);
        let rb = batches
            .iter()
            .find_map(|b| b.as_control())
            .filter(|c| matches!(c, ControlRecord::Rollback { .. }))
            .expect("a rollback");
        // Tracked window starts at 100 -> invalidate everything after 99.
        assert_eq!(rb, ControlRecord::Rollback { chain_id: 1, invalidate_after_block: 99 });
    }

    #[tokio::test]
    async fn reorg_tracking_off_means_no_rollback_ever() {
        let srv = MockServer::start(reorg_handler(true)).await;
        let mut cfg = source_cfg(&srv.url, SourceMode::Backfill);
        cfg.to_block = Some(110);
        cfg.reorg = ReorgCfg { enabled: false, window: 64 }; // confirmations-only stance
        let source = HyperSyncSource::from_config(&cfg, 1_000);

        let (tx, mut rx) = channel(16);
        tokio::time::timeout(
            Duration::from_secs(10),
            source.run(100, tx, Arc::new(AtomicBool::new(false)), None),
        )
        .await
        .expect("must finish")
        .unwrap();

        let batches = drain(&mut rx);
        assert!(
            !batches.iter().any(|b| matches!(b.as_control(), Some(ControlRecord::Rollback { .. }))),
            "tracking is off: the changed hash is never even compared"
        );
        // No headers-only sweep either — nothing to locate.
        assert!(srv.requests().iter().all(|r| r.body["include_all_blocks"] != json!(true)));
    }

    #[tokio::test]
    async fn a_persisted_window_is_restored_on_start() {
        // A reorg that happened while the pipeline was down must be caught on
        // the first poll after resume — which only works if the window survived.
        let reported = Arc::new(AtomicBool::new(false));
        let srv = MockServer::start(move |_p, body, _i| {
            if body["include_all_blocks"] == json!(true) {
                // Canonical: block 100 still hashes as the restored window says,
                // so the fork is at 100 and only later blocks are void.
                return Reply::json(json!({ "next_block": 103, "data": { "blocks": [
                    {"number": 100, "hash": "0xa"}]}}));
            }
            if reported.swap(true, Ordering::SeqCst) {
                // Post-rollback: the corrected chain, no longer touching block 100.
                return Reply::json(json!({ "archive_height": 19_000_000, "next_block": 110,
                    "data": { "blocks": [{"number": 105, "hash": "0xnew"}], "logs": [] } }));
            }
            // First poll after resume: block 100 disagrees with the restored window.
            Reply::json(json!({ "archive_height": 19_000_000, "next_block": 110,
                "data": { "blocks": [{"number": 100, "hash": "0xCHANGED"}], "logs": [] } }))
        })
        .await;

        let kv: Arc<dyn KvStore> = Arc::new(MemKv::default());
        // Seed the window as a previous run would have left it.
        kv.set(
            "__source/eth",
            "reorg_hashes",
            serde_json::to_vec(&vec![(100u64, "0xa".to_string())]).unwrap(),
        );

        let mut cfg = source_cfg(&srv.url, SourceMode::Backfill);
        cfg.to_block = Some(110);
        cfg.reorg = ReorgCfg { enabled: true, window: 64 };
        let source = HyperSyncSource::from_config(&cfg, 1_000);

        let (tx, mut rx) = channel(16);
        tokio::time::timeout(
            Duration::from_secs(10),
            source.run(101, tx, Arc::new(AtomicBool::new(false)), Some(kv)),
        )
        .await
        .expect("must finish")
        .unwrap();

        let batches = drain(&mut rx);
        assert!(
            batches
                .iter()
                .any(|b| matches!(b.as_control(), Some(ControlRecord::Rollback { .. }))),
            "the restored hash for block 100 disagrees with the chain -> rollback"
        );
    }

    #[tokio::test]
    async fn the_start_cursor_never_goes_below_from_block() {
        // A stale checkpoint from a wider run must not drag a source back before
        // its configured range.
        let srv = MockServer::start(|_p, body, _i| {
            assert_eq!(body["from_block"], 100, "clamped up to from_block");
            Reply::json(json!({ "next_block": 110, "data": { "logs": [] } }))
        })
        .await;
        let mut cfg = source_cfg(&srv.url, SourceMode::Backfill);
        cfg.to_block = Some(110);
        let source = HyperSyncSource::from_config(&cfg, 1_000);
        let (tx, _rx) = channel(16);
        source.run(5, tx, Arc::new(AtomicBool::new(false)), None).await.unwrap();
    }

    #[tokio::test]
    async fn a_stalled_backfill_waits_instead_of_eofing_early() {
        // next_block == cursor with the range unfinished: the loop must poll
        // again rather than declare the backfill complete and lose the tail.
        let srv = MockServer::start(|_p, _b, i| {
            if i < 2 {
                Reply::json(json!({ "archive_height": 19_000_000, "next_block": 100,
                                    "data": { "logs": [] } }))
            } else {
                Reply::json(json!({ "archive_height": 19_000_000, "next_block": 110,
                                    "data": { "logs": logs(&[100]) } }))
            }
        })
        .await;
        let mut cfg = source_cfg(&srv.url, SourceMode::Backfill);
        cfg.to_block = Some(110);
        let source = HyperSyncSource::from_config(&cfg, 1_000);

        let (tx, mut rx) = channel(16);
        tokio::time::timeout(
            Duration::from_secs(10),
            source.run(100, tx, Arc::new(AtomicBool::new(false)), None),
        )
        .await
        .expect("must finish")
        .unwrap();

        assert!(srv.call_count() >= 3, "the stall was re-polled, not EOF'd");
        let batches = drain(&mut rx);
        assert_eq!(batches.len(), 2, "one real batch + EOF");
        assert!(batches[1].is_control());
    }
}
