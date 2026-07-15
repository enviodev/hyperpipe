//! test-chaos — a module that misbehaves on command (TEST_PLAN §4).
//!
//! Every other module in this workspace is well-behaved, which means nothing
//! exercises the host's defences: instance recycling after a trap, the epoch
//! deadline that stops a runaway loop, the memory cap, and the difference
//! between a graceful `Err` (instance kept, state intact) and a trap (instance
//! discarded). This module supplies the misbehaviour, driven entirely by config
//! so one `.wasm` covers every case.
//!
//! Test fixture only — it is not a builtin and `resolve_module` cannot name it;
//! pipelines reference it by path (`module: { file: ... }`).
//!
//! ```yaml
//! config:
//!   trap_on_batch: 3        # panic on the 3rd process() call -> trap
//!   error_on_batch: 2       # return Err on the 2nd call (graceful)
//!   error_always: true      # return Err every call
//!   loop_on_batch: 1        # spin forever -> epoch deadline must interrupt
//!   alloc_on_batch: 1       # allocate alloc_bytes -> memory cap must trap it
//!   alloc_bytes: 536870912
//!   init_error: true        # fail init() -> load must surface it
//! ```
//!
//! Passing batches are tagged with `chaos_calls`: this instance's call counter.
//! That is how a test tells a fresh instance (counter restarts at 1) from a
//! reused one (counter continues) without any host-side bookkeeping.

#[cfg(target_arch = "wasm32")]
mod wasm_glue {
    use hyperpipe_sdk::serde_json::{json, Value};
    use hyperpipe_sdk::{export_processor, Batch, InitInfo, Processor};

    struct Cfg {
        trap_on_batch: Option<u64>,
        error_on_batch: Option<u64>,
        error_always: bool,
        loop_on_batch: Option<u64>,
        alloc_on_batch: Option<u64>,
        alloc_bytes: usize,
    }

    struct Chaos {
        cfg: Cfg,
        /// Calls seen by THIS instance. Resets to 0 when the host builds a new
        /// one, which is exactly what the recycling tests assert on.
        calls: u64,
    }

    fn opt_u64(config: &Value, key: &str) -> Option<u64> {
        config.get(key).and_then(|v| v.as_u64())
    }

    fn flag(config: &Value, key: &str) -> bool {
        config.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
    }

    impl Processor for Chaos {
        fn init(config: Value, _ctx: &InitInfo) -> Result<Self, String> {
            if flag(&config, "init_error") {
                return Err("chaos: init failed on purpose".into());
            }
            Ok(Chaos {
                cfg: Cfg {
                    trap_on_batch: opt_u64(&config, "trap_on_batch"),
                    error_on_batch: opt_u64(&config, "error_on_batch"),
                    error_always: flag(&config, "error_always"),
                    loop_on_batch: opt_u64(&config, "loop_on_batch"),
                    alloc_on_batch: opt_u64(&config, "alloc_on_batch"),
                    alloc_bytes: opt_u64(&config, "alloc_bytes").unwrap_or(1 << 30) as usize,
                },
                calls: 0,
            })
        }

        fn process(&mut self, batch: Batch) -> Result<Vec<Batch>, String> {
            self.calls += 1;
            let n = self.calls;

            // Graceful error: the SDK returns Err across the boundary and the
            // instance stays perfectly usable — the host must keep it.
            if self.cfg.error_always || self.cfg.error_on_batch == Some(n) {
                return Err(format!("chaos: refusing batch {n} on purpose"));
            }

            // Trap: a guest panic aborts the instance and poisons its store.
            if self.cfg.trap_on_batch == Some(n) {
                hp_host::log_warn(&format!("chaos: trapping on call {n}"));
                panic!("chaos: trap on call {n}");
            }

            // Runaway: only the epoch deadline can stop this.
            if self.cfg.loop_on_batch == Some(n) {
                hp_host::log_warn(&format!("chaos: spinning forever on call {n}"));
                loop {
                    core::hint::spin_loop();
                }
            }

            // Memory hog: grow past the store's limit and let the limiter deny
            // it, which aborts the guest rather than the host.
            if self.cfg.alloc_on_batch == Some(n) {
                hp_host::log_warn(&format!("chaos: allocating {} bytes", self.cfg.alloc_bytes));
                let chunk = 1 << 20; // 1 MiB at a time
                let mut held: Vec<Vec<u8>> = Vec::new();
                while held.len() * chunk < self.cfg.alloc_bytes {
                    let mut block = vec![0u8; chunk];
                    // Touch it so the pages are real, not just reserved.
                    block[0] = held.len() as u8;
                    held.push(block);
                }
                // Keep the allocation observable so it cannot be optimized away.
                hp_host::metric_add("chaos.alloc_mib", held.len() as u64);
            }

            let records: Vec<Value> = batch
                .records
                .iter()
                .map(|r| {
                    let mut r = r.clone();
                    if let Some(obj) = r.as_object_mut() {
                        obj.insert("chaos_calls".into(), json!(n));
                    }
                    r
                })
                .collect();
            Ok(vec![batch.derive(batch.kind, records)])
        }
    }

    export_processor!(Chaos);
}
