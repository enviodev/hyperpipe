//! Reorg tracking (§5.2 phase 2): a bounded window of (block_number ->
//! block_hash) for blocks this source has emitted, checked against HyperSync's
//! `rollback_guard` on every poll. A mismatch means the chain reorganized under
//! us and everything past the fork must be invalidated downstream.
//!
//! The window is persisted through the checkpoint KV store (same durability
//! tick as the cursors), so detection survives restarts — a reorg that happens
//! while the pipeline is down is caught on the first poll after resume.

use std::collections::{BTreeMap, HashMap};

use hp_encoding::KvStore;

use crate::client::QueryResponse;

/// KV namespace for per-source state (kept out of module KV namespaces).
fn kv_module(source: &str) -> String {
    format!("__source/{source}")
}
const KV_KEY: &str = "reorg_hashes";

pub struct ReorgTracker {
    window: u64,
    /// block_number -> block_hash for recently emitted blocks.
    hashes: BTreeMap<u64, String>,
}

impl ReorgTracker {
    pub fn new(window: u64) -> Self {
        ReorgTracker {
            window: window.max(1),
            hashes: BTreeMap::new(),
        }
    }

    /// Restore from the KV store (empty tracker when nothing was persisted or
    /// the payload doesn't parse — detection just warms back up).
    pub fn load(window: u64, source: &str, kv: Option<&dyn KvStore>) -> Self {
        let mut t = ReorgTracker::new(window);
        if let Some(kv) = kv {
            if let Some(bytes) = kv.get(&kv_module(source), KV_KEY) {
                match serde_json::from_slice::<Vec<(u64, String)>>(&bytes) {
                    Ok(pairs) => {
                        t.hashes = pairs.into_iter().collect();
                        t.trim();
                    }
                    Err(e) => {
                        tracing::warn!(source, "persisted reorg window unreadable ({e}); starting fresh");
                    }
                }
            }
        }
        t
    }

    /// Persist the window (durable on the next checkpoint tick).
    pub fn save(&self, source: &str, kv: Option<&dyn KvStore>) {
        if let Some(kv) = kv {
            let pairs: Vec<(&u64, &String)> = self.hashes.iter().collect();
            if let Ok(bytes) = serde_json::to_vec(&pairs) {
                kv.set(&kv_module(source), KV_KEY, bytes);
            }
        }
    }

    /// Record hashes from a response for blocks strictly below `emitted_next`
    /// (only blocks that actually went downstream), plus the guard's own
    /// hashes, then trim to the window.
    pub fn record(&mut self, resp: &QueryResponse, emitted_next: u64) {
        for (num, hash) in &resp.block_hashes {
            if *num < emitted_next {
                self.hashes.insert(*num, hash.clone());
            }
        }
        if let Some(g) = &resp.rollback_guard {
            if g.block_number < emitted_next {
                self.hashes.insert(g.block_number, g.hash.clone());
            }
            // The guard also pins the parent of the first scanned block — a
            // block we emitted on an earlier iteration (or pre-start context).
            if g.first_block_number > 0 {
                self.hashes
                    .insert(g.first_block_number - 1, g.first_parent_hash.clone());
            }
        }
        self.trim();
    }

    /// Compare a fresh response against the stored window. `Some(block)` =
    /// the lowest stored block whose hash no longer matches what the chain
    /// says — a reorg at or before that height.
    pub fn conflict(&self, resp: &QueryResponse) -> Option<u64> {
        let mut worst: Option<u64> = None;
        let mut note = |b: u64| worst = Some(worst.map_or(b, |w: u64| w.min(b)));

        if let Some(g) = &resp.rollback_guard {
            if g.first_block_number > 0 {
                let parent = g.first_block_number - 1;
                if let Some(stored) = self.hashes.get(&parent) {
                    if *stored != g.first_parent_hash {
                        note(parent);
                    }
                }
            }
        }
        // Overlap: blocks re-fetched in this response that we already emitted.
        for (num, hash) in &resp.block_hashes {
            if let Some(stored) = self.hashes.get(num) {
                if stored != hash {
                    note(*num);
                }
            }
        }
        worst
    }

    /// Highest stored block whose hash matches the canonical chain — the last
    /// block that survived the reorg. `None` when nothing matches.
    pub fn last_matching(&self, canonical: &HashMap<u64, String>) -> Option<u64> {
        self.hashes
            .iter()
            .rev()
            .find(|(num, hash)| canonical.get(num) == Some(hash))
            .map(|(num, _)| *num)
    }

    /// Drop everything above the fork (those hashes are void).
    pub fn purge_after(&mut self, fork: u64) {
        self.hashes.retain(|num, _| *num <= fork);
    }

    pub fn min_block(&self) -> Option<u64> {
        self.hashes.keys().next().copied()
    }

    fn trim(&mut self) {
        while self.hashes.len() as u64 > self.window {
            self.hashes.pop_first();
        }
    }

    #[cfg(test)]
    fn get(&self, num: u64) -> Option<&String> {
        self.hashes.get(&num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::RollbackGuard;

    fn resp(
        block_hashes: Vec<(u64, &str)>,
        guard: Option<(u64, &str, u64, &str)>,
    ) -> QueryResponse {
        QueryResponse {
            archive_height: None,
            next_block: 0,
            records: vec![],
            block_hashes: block_hashes
                .into_iter()
                .map(|(n, h)| (n, h.to_string()))
                .collect(),
            rollback_guard: guard.map(|(bn, h, fbn, fph)| RollbackGuard {
                block_number: bn,
                hash: h.to_string(),
                first_block_number: fbn,
                first_parent_hash: fph.to_string(),
                timestamp: None,
            }),
        }
    }

    #[test]
    fn records_only_emitted_blocks_and_guard_parent() {
        let mut t = ReorgTracker::new(10);
        // response scanned up to 105 but only 100..103 were emitted
        t.record(&resp(vec![(100, "0xa"), (102, "0xb"), (104, "0xd")], Some((104, "0xd", 100, "0x99"))), 103);
        assert_eq!(t.get(100), Some(&"0xa".to_string()));
        assert_eq!(t.get(102), Some(&"0xb".to_string()));
        assert_eq!(t.get(104), None, "unemitted block must not be tracked");
        // guard parent (block 99) is a valid reference point
        assert_eq!(t.get(99), Some(&"0x99".to_string()));
    }

    #[test]
    fn detects_reorg_via_guard_parent_mismatch() {
        let mut t = ReorgTracker::new(10);
        t.record(&resp(vec![(100, "0xa")], None), 101);
        // next poll: first scanned block is 101, whose parent (100) now hashes differently
        let r = resp(vec![], Some((105, "0xtip", 101, "0xDIFFERENT")));
        assert_eq!(t.conflict(&r), Some(100));
    }

    #[test]
    fn detects_reorg_via_overlapping_block_hash() {
        let mut t = ReorgTracker::new(10);
        t.record(&resp(vec![(100, "0xa"), (101, "0xb")], None), 102);
        // confirmation-lag truncation makes the source re-fetch 101; hash changed
        let r = resp(vec![(101, "0xREORGED")], None);
        assert_eq!(t.conflict(&r), Some(101));
    }

    #[test]
    fn no_false_positive_when_chain_intact() {
        let mut t = ReorgTracker::new(10);
        t.record(&resp(vec![(100, "0xa")], None), 101);
        let ok = resp(vec![(100, "0xa"), (101, "0xb")], Some((105, "0xtip", 101, "0xa")));
        assert_eq!(t.conflict(&ok), None);
        // guard for a range whose parent we never saw -> nothing to compare
        let unknown = resp(vec![], Some((300, "0xt", 200, "0xunseen")));
        assert_eq!(t.conflict(&unknown), None);
    }

    #[test]
    fn conflict_reports_lowest_mismatch() {
        let mut t = ReorgTracker::new(10);
        t.record(&resp(vec![(100, "0xa"), (101, "0xb"), (102, "0xc")], None), 103);
        let r = resp(vec![(100, "0xa"), (101, "0xX"), (102, "0xY")], None);
        assert_eq!(t.conflict(&r), Some(101));
    }

    #[test]
    fn window_trims_oldest() {
        let mut t = ReorgTracker::new(3);
        t.record(&resp(vec![(1, "a"), (2, "b"), (3, "c"), (4, "d")], None), 100);
        assert_eq!(t.min_block(), Some(2));
        assert_eq!(t.get(1), None);
    }

    #[test]
    fn purge_after_drops_forked_blocks() {
        let mut t = ReorgTracker::new(10);
        t.record(&resp(vec![(100, "a"), (101, "b"), (102, "c")], None), 103);
        t.purge_after(100);
        assert_eq!(t.get(101), None);
        assert_eq!(t.get(102), None);
        assert_eq!(t.get(100), Some(&"a".to_string()));
    }

    #[test]
    fn last_matching_walks_newest_first() {
        let mut t = ReorgTracker::new(10);
        t.record(&resp(vec![(100, "a"), (101, "b"), (102, "c")], None), 103);
        let canonical: HashMap<u64, String> = [
            (100u64, "a".to_string()),
            (101, "b".to_string()),
            (102, "REORGED".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(t.last_matching(&canonical), Some(101));
        // nothing matches -> None
        let none: HashMap<u64, String> = [(100u64, "X".to_string())].into_iter().collect();
        assert_eq!(t.last_matching(&none), None);
    }

    #[test]
    fn save_load_roundtrip_via_kv() {
        use hp_encoding::KvStore;
        use std::collections::HashMap as Map;
        use std::sync::Mutex;

        #[derive(Default)]
        struct MemKv(Mutex<Map<(String, String), Vec<u8>>>);
        impl KvStore for MemKv {
            fn get(&self, m: &str, k: &str) -> Option<Vec<u8>> {
                self.0.lock().unwrap().get(&(m.into(), k.into())).cloned()
            }
            fn set(&self, m: &str, k: &str, v: Vec<u8>) {
                self.0.lock().unwrap().insert((m.into(), k.into()), v);
            }
        }

        let kv = MemKv::default();
        let mut t = ReorgTracker::new(10);
        t.record(&resp(vec![(100, "a"), (101, "b")], None), 102);
        t.save("src", Some(&kv));

        let restored = ReorgTracker::load(10, "src", Some(&kv));
        assert_eq!(restored.get(100), Some(&"a".to_string()));
        assert_eq!(restored.get(101), Some(&"b".to_string()));

        // corrupt payload -> fresh tracker, no panic
        kv.set(&kv_module("src"), KV_KEY, b"not json".to_vec());
        let fresh = ReorgTracker::load(10, "src", Some(&kv));
        assert_eq!(fresh.min_block(), None);
    }
}
