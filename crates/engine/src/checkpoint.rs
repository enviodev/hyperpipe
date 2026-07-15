//! Checkpoint store (§8): per-(source,sink) cursors + module KV, in SQLite.
//!
//! At-least-once mechanism:
//! - `ack(source, sink, next_block)` records the highest block a sink has
//!   accepted (in-order, so this is a contiguous watermark).
//! - A checkpoint tick snapshots the watermarks, the runtime flushes all sinks,
//!   then `persist_snapshot` writes cursors + dirty KV in one transaction. The
//!   snapshot is taken *before* the flush, so only durable batches advance
//!   (§8.2 rule 5).
//! - `restore(source, reachable_sinks)` returns the safe restart block: the min
//!   cursor over the sinks the source feeds, or `None` (start from `from_block`)
//!   if any of them has no cursor yet.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::{Context, Result};
use hp_encoding::KvStore;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

pub struct CheckpointStore {
    pool: SqlitePool,
    handle: tokio::runtime::Handle,
    /// (source, sink) -> latest acked next_block (in memory; persisted on tick).
    cursors: Mutex<HashMap<(String, String), u64>>,
    /// (module, key) -> value, not yet persisted.
    kv_dirty: Mutex<HashMap<(String, String), Vec<u8>>>,
    /// read-through cache for kv_get.
    kv_cache: Mutex<HashMap<(String, String), Option<Vec<u8>>>>,
}

impl CheckpointStore {
    /// Open (creating if needed) the SQLite store and load existing cursors.
    pub async fn open(path: &str, handle: tokio::runtime::Handle) -> Result<Self> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .with_context(|| format!("open checkpoint db {path}"))?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS cursors (
                source TEXT NOT NULL, sink TEXT NOT NULL,
                next_block INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                PRIMARY KEY (source, sink))",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS module_kv (
                module TEXT NOT NULL, key TEXT NOT NULL, value BLOB,
                PRIMARY KEY (module, key))",
        )
        .execute(&pool)
        .await?;

        let mut cursors = HashMap::new();
        let rows = sqlx::query("SELECT source, sink, next_block FROM cursors")
            .fetch_all(&pool)
            .await?;
        for r in rows {
            let source: String = r.get("source");
            let sink: String = r.get("sink");
            let next: i64 = r.get("next_block");
            cursors.insert((source, sink), next as u64);
        }

        Ok(CheckpointStore {
            pool,
            handle,
            cursors: Mutex::new(cursors),
            kv_dirty: Mutex::new(HashMap::new()),
            kv_cache: Mutex::new(HashMap::new()),
        })
    }

    /// Record that `sink` has accepted everything up to (exclusive) `next_block`
    /// from `source`. Monotonic — never moves a watermark backwards.
    pub fn ack(&self, source: &str, sink: &str, next_block: u64) {
        let mut c = self.cursors.lock().unwrap();
        let e = c.entry((source.to_string(), sink.to_string())).or_insert(0);
        if next_block > *e {
            *e = next_block;
        }
    }

    /// Force the (source, sink) cursor DOWN to `next_block` after a reorg
    /// rollback (the only sanctioned non-monotonic move). Writes through to
    /// SQLite immediately so a crash right after the rollback cannot resume
    /// from the stale, higher cursor and skip the corrected blocks.
    pub async fn rewind(&self, source: &str, sink: &str, next_block: u64) -> Result<()> {
        {
            let mut c = self.cursors.lock().unwrap();
            let e = c
                .entry((source.to_string(), sink.to_string()))
                .or_insert(next_block);
            if next_block < *e {
                *e = next_block;
            }
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        sqlx::query(
            "UPDATE cursors SET next_block=?3, updated_at=?4
             WHERE source=?1 AND sink=?2 AND next_block > ?3",
        )
        .bind(source)
        .bind(sink)
        .bind(next_block as i64)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("persist cursor rewind")?;
        Ok(())
    }

    /// Safe restart block for `source`: the min cursor across the sinks it
    /// feeds. `None` means "no durable progress yet — start from from_block".
    pub fn restore(&self, source: &str, reachable_sinks: &[String]) -> Option<u64> {
        let c = self.cursors.lock().unwrap();
        let mut min = u64::MAX;
        for sink in reachable_sinks {
            match c.get(&(source.to_string(), sink.clone())) {
                Some(v) => min = min.min(*v),
                None => return None, // a sink has nothing durable -> replay fully
            }
        }
        if reachable_sinks.is_empty() || min == u64::MAX {
            None
        } else {
            Some(min)
        }
    }

    /// Snapshot the current watermarks (call before flushing sinks).
    pub fn snapshot(&self) -> Vec<(String, String, u64)> {
        self.cursors
            .lock()
            .unwrap()
            .iter()
            .map(|((s, k), v)| (s.clone(), k.clone(), *v))
            .collect()
    }

    /// Persist a watermark snapshot + all dirty KV in a single transaction.
    pub async fn persist_snapshot(&self, snapshot: Vec<(String, String, u64)>) -> Result<()> {
        let dirty: Vec<((String, String), Vec<u8>)> = {
            let mut d = self.kv_dirty.lock().unwrap();
            d.drain().collect()
        };
        if snapshot.is_empty() && dirty.is_empty() {
            return Ok(());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let mut tx = self.pool.begin().await.context("begin checkpoint txn")?;
        for (source, sink, next) in snapshot {
            // Floor against the live watermark: a rollback rewind that landed
            // after this snapshot was taken must not be re-raised by persisting
            // the stale, higher value.
            let next = {
                let c = self.cursors.lock().unwrap();
                c.get(&(source.clone(), sink.clone()))
                    .map(|cur| next.min(*cur))
                    .unwrap_or(next)
            };
            sqlx::query(
                "INSERT INTO cursors (source, sink, next_block, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(source, sink) DO UPDATE SET next_block=?3, updated_at=?4",
            )
            .bind(source)
            .bind(sink)
            .bind(next as i64)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        for ((module, key), value) in dirty {
            sqlx::query(
                "INSERT INTO module_kv (module, key, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT(module, key) DO UPDATE SET value=?3",
            )
            .bind(module)
            .bind(key)
            .bind(value)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await.context("commit checkpoint txn")?;
        Ok(())
    }
}

impl KvStore for CheckpointStore {
    fn get(&self, module: &str, key: &str) -> Option<Vec<u8>> {
        let k = (module.to_string(), key.to_string());
        if let Some(v) = self.kv_dirty.lock().unwrap().get(&k) {
            return Some(v.clone());
        }
        if let Some(v) = self.kv_cache.lock().unwrap().get(&k) {
            return v.clone();
        }
        let pool = self.pool.clone();
        let (m, key_s) = (module.to_string(), key.to_string());
        let loaded = self.handle.block_on(async move {
            sqlx::query("SELECT value FROM module_kv WHERE module=?1 AND key=?2")
                .bind(&m)
                .bind(&key_s)
                .fetch_optional(&pool)
                .await
                .ok()
                .flatten()
                .map(|r| r.get::<Vec<u8>, _>("value"))
        });
        self.kv_cache.lock().unwrap().insert(k, loaded.clone());
        loaded
    }

    fn set(&self, module: &str, key: &str, value: Vec<u8>) {
        let k = (module.to_string(), key.to_string());
        self.kv_cache.lock().unwrap().insert(k.clone(), Some(value.clone()));
        self.kv_dirty.lock().unwrap().insert(k, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_db(name: &str) -> String {
        let dir = std::env::temp_dir().join("hyperpipe-ckpt-tests");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join(format!("{name}-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path.to_string_lossy().into_owned()
    }

    async fn open(name: &str) -> (CheckpointStore, String) {
        let path = tmp_db(name);
        let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
            .await
            .unwrap();
        (store, path)
    }

    #[tokio::test]
    async fn ack_is_monotonic() {
        let (store, _p) = open("monotonic").await;
        store.ack("src", "sink", 100);
        store.ack("src", "sink", 50); // stale ack must not move the cursor back
        let snap = store.snapshot();
        assert_eq!(snap, vec![("src".into(), "sink".into(), 100)]);
    }

    #[tokio::test]
    async fn restore_is_min_over_reachable_sinks() {
        let (store, _p) = open("restore-min").await;
        store.ack("src", "fast_sink", 500);
        store.ack("src", "slow_sink", 120);
        let sinks = vec!["fast_sink".to_string(), "slow_sink".to_string()];
        // The slow sink bounds the safe restart point.
        assert_eq!(store.restore("src", &sinks), Some(120));
    }

    #[tokio::test]
    async fn restore_none_when_any_sink_has_no_cursor() {
        let (store, _p) = open("restore-none").await;
        store.ack("src", "a", 500);
        let sinks = vec!["a".to_string(), "b".to_string()]; // b never acked
        assert_eq!(store.restore("src", &sinks), None);
        assert_eq!(store.restore("src", &[]), None);
    }

    #[tokio::test]
    async fn rewind_moves_cursor_down_and_persists() {
        let path = tmp_db("rewind");
        {
            let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
                .await
                .unwrap();
            store.ack("src", "sink", 500);
            store.persist_snapshot(store.snapshot()).await.unwrap();
            // reorg: everything after block 99 is void -> next fetch is 100
            store.rewind("src", "sink", 100).await.unwrap();
            // stale higher ack after the rewind must not raise it back...
            store.ack("src", "sink", 90); // ...and a LOWER ack stays ignored too
            assert_eq!(store.restore("src", &["sink".to_string()]), Some(100));
        }
        // ...even across a crash/reopen (write-through)
        let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
            .await
            .unwrap();
        assert_eq!(store.restore("src", &["sink".to_string()]), Some(100));
    }

    #[tokio::test]
    async fn stale_snapshot_cannot_reraise_a_rewound_cursor() {
        // Race: checkpoint tick snapshots 500, a rollback rewinds to 100 during
        // the flush, then the tick persists. The persisted value must be 100.
        let path = tmp_db("rewind-race");
        {
            let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
                .await
                .unwrap();
            store.ack("src", "sink", 500);
            let snap = store.snapshot();
            store.rewind("src", "sink", 100).await.unwrap();
            store.persist_snapshot(snap).await.unwrap();
        }
        let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
            .await
            .unwrap();
        assert_eq!(store.restore("src", &["sink".to_string()]), Some(100));
    }

    #[tokio::test]
    async fn rewind_then_replay_acks_forward_again() {
        let (store, _p) = open("rewind-forward").await;
        store.ack("src", "sink", 500);
        store.rewind("src", "sink", 100).await.unwrap();
        store.ack("src", "sink", 150); // replayed corrected blocks
        assert_eq!(store.restore("src", &["sink".to_string()]), Some(150));
    }

    #[tokio::test]
    async fn cursors_survive_reopen() {
        let path = tmp_db("reopen");
        {
            let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
                .await
                .unwrap();
            store.ack("src", "sink", 777);
            let snap = store.snapshot();
            store.persist_snapshot(snap).await.unwrap();
        }
        let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
            .await
            .unwrap();
        assert_eq!(store.restore("src", &["sink".to_string()]), Some(777));
    }

    #[tokio::test]
    async fn snapshot_taken_before_later_acks_is_what_persists() {
        // §8.2 rule 5: the snapshot is taken BEFORE sinks flush; acks that land
        // during the flush must not be persisted by this tick.
        let path = tmp_db("pre-flush-snap");
        {
            let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
                .await
                .unwrap();
            store.ack("src", "sink", 100);
            let snap = store.snapshot();
            store.ack("src", "sink", 999); // arrives "during the flush"
            store.persist_snapshot(snap).await.unwrap();
        }
        let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
            .await
            .unwrap();
        assert_eq!(store.restore("src", &["sink".to_string()]), Some(100));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kv_dirty_reads_back_and_persists_with_snapshot() {
        let path = tmp_db("kv");
        {
            let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
                .await
                .unwrap();
            store.set("dedupe", "window", b"abc".to_vec());
            // read-your-writes before any persist
            assert_eq!(store.get("dedupe", "window"), Some(b"abc".to_vec()));
            store.persist_snapshot(store.snapshot()).await.unwrap();
        }
        let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
            .await
            .unwrap();
        // KvStore::get uses handle.block_on — call it off the runtime thread,
        // mirroring how WASM host imports invoke it (spawn_blocking).
        let store = std::sync::Arc::new(store);
        let s2 = store.clone();
        let got = tokio::task::spawn_blocking(move || s2.get("dedupe", "window"))
            .await
            .unwrap();
        assert_eq!(got, Some(b"abc".to_vec()));
        assert_eq!(
            tokio::task::spawn_blocking({
                let s = store.clone();
                move || s.get("dedupe", "missing")
            })
            .await
            .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn open_on_an_unwritable_path_errors() {
        // A path whose parent cannot be created (a file stands where a dir must go).
        let blocker = tmp_db("not-a-dir");
        std::fs::write(&blocker, b"i am a file").unwrap();
        let err = match CheckpointStore::open(
            &format!("{blocker}/sub/ck.db"),
            tokio::runtime::Handle::current(),
        )
        .await
        {
            Err(e) => e,
            Ok(_) => panic!("opening under a file path must fail"),
        };
        assert!(
            format!("{err:#}").contains("open checkpoint db"),
            "expected the open context, got: {err:#}"
        );
    }

    #[tokio::test]
    async fn persist_with_nothing_to_write_is_a_noop() {
        let (store, _p) = open("empty-persist").await;
        // No acks, no kv: must return Ok without opening a transaction.
        store.persist_snapshot(Vec::new()).await.unwrap();
        assert!(store.snapshot().is_empty());
        let rows = sqlx::query("SELECT COUNT(*) as n FROM cursors")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(rows.get::<i64, _>("n"), 0);
    }

    #[tokio::test]
    async fn rewind_above_the_current_cursor_changes_nothing() {
        let path = tmp_db("rewind-above");
        {
            let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
                .await
                .unwrap();
            store.ack("src", "sink", 100);
            store.persist_snapshot(store.snapshot()).await.unwrap();
            // A "rewind" to a HIGHER block is not a rollback — it must not raise
            // the watermark (that would skip blocks 100..500 on restart).
            store.rewind("src", "sink", 500).await.unwrap();
            assert_eq!(store.restore("src", &["sink".to_string()]), Some(100));
        }
        let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
            .await
            .unwrap();
        assert_eq!(
            store.restore("src", &["sink".to_string()]),
            Some(100),
            "the SQLite row must not have been raised either"
        );
    }

    #[tokio::test]
    async fn rewind_of_an_unknown_pair_seeds_the_cursor() {
        let (store, _p) = open("rewind-unknown").await;
        // No prior ack: the rewind seeds the entry rather than panicking.
        store.rewind("src", "never-acked", 42).await.unwrap();
        assert_eq!(store.restore("src", &["never-acked".to_string()]), Some(42));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kv_get_caches_reads_from_a_previous_instance() {
        let path = tmp_db("kv-readthrough");
        {
            let store = CheckpointStore::open(&path, tokio::runtime::Handle::current())
                .await
                .unwrap();
            store.set("mod", "k", b"v1".to_vec());
            store.persist_snapshot(store.snapshot()).await.unwrap();
        }
        let store = std::sync::Arc::new(
            CheckpointStore::open(&path, tokio::runtime::Handle::current())
                .await
                .unwrap(),
        );
        // First get: nothing dirty, nothing cached -> reads through to SQLite.
        let s2 = store.clone();
        let first = tokio::task::spawn_blocking(move || s2.get("mod", "k")).await.unwrap();
        assert_eq!(first, Some(b"v1".to_vec()));

        // Delete the row out from under the store: a cached get must not notice.
        sqlx::query("DELETE FROM module_kv WHERE module='mod' AND key='k'")
            .execute(&store.pool)
            .await
            .unwrap();
        let s3 = store.clone();
        let second = tokio::task::spawn_blocking(move || s3.get("mod", "k")).await.unwrap();
        assert_eq!(second, Some(b"v1".to_vec()), "second get must come from kv_cache");

        // A miss is cached as a miss too (no repeated SQLite hit per lookup).
        let s4 = store.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || s4.get("mod", "absent")).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn kv_namespaces_do_not_bleed_between_modules() {
        let (store, _p) = open("kv-namespace").await;
        store.set("a", "same-key", b"from-a".to_vec());
        store.set("b", "same-key", b"from-b".to_vec());
        assert_eq!(store.get("a", "same-key"), Some(b"from-a".to_vec()));
        assert_eq!(store.get("b", "same-key"), Some(b"from-b".to_vec()));
    }

    #[tokio::test]
    async fn restore_tolerates_a_duplicated_sink_in_the_list() {
        let (store, _p) = open("restore-dupes").await;
        store.ack("src", "a", 500);
        store.ack("src", "b", 120);
        let sinks = vec!["a".to_string(), "b".to_string(), "a".to_string()];
        assert_eq!(store.restore("src", &sinks), Some(120));
    }
}
