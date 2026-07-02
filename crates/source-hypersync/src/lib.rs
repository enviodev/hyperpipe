//! HyperSync source layer (§5): a per-source cursor loop that turns a chain +
//! query into a stream of batch envelopes.
//!
//! Native (not WASM) on purpose — ingestion is the performance edge and must
//! not sit behind a serialization boundary. Talks to the HyperSync HTTP `/query`
//! endpoint; set `HYPERSYNC_BEARER_TOKEN` (create one at app.envio.dev/api-tokens).

mod client;

pub use client::{HyperSyncClient, QueryResponse};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use hp_encoding::{Batch, BatchKind, BlockRange, ControlRecord};
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
    max_records: usize,
    poll_interval: Duration,
}

impl HyperSyncSource {
    /// Build from a validated config source. `default_max_records` comes from
    /// the resource profile when the source omits `batch.max_records`.
    pub fn from_config(src: &Source, default_max_records: usize) -> Self {
        let ep = src.endpoint();
        let token = std::env::var("HYPERSYNC_BEARER_TOKEN").ok();
        let client = HyperSyncClient::new(ep.url.clone(), token, &src.query);
        HyperSyncSource {
            name: src.name.clone(),
            chain_id: ep.chain_id,
            client,
            mode: src.mode,
            from_block: src.from_block.unwrap_or(0),
            to_block: src.to_block,
            confirmations: src.confirmations,
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
    /// signal back to ingestion.
    pub async fn run(&self, start_cursor: u64, tx: Sender<Batch>, stop: Arc<AtomicBool>) -> Result<()> {
        let mut cursor = start_cursor.max(self.from_block);
        let mut backoff = Duration::from_millis(250);

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

            let resp = match self.client.query(cursor, self.to_block).await {
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
                    let mut records = resp.records;
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
    }
}
