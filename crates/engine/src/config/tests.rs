use super::*;
use std::collections::HashMap;

struct FakeSecrets(HashMap<String, String>);
impl SecretSource for FakeSecrets {
    fn get(&self, name: &str) -> Option<String> {
        self.0.get(name).cloned()
    }
}
fn secrets(pairs: &[(&str, &str)]) -> FakeSecrets {
    FakeSecrets(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect())
}
fn no_secrets() -> FakeSecrets {
    FakeSecrets(HashMap::new())
}

const GOOD: &str = r#"
name: usdc-multichain
runtime:
  resource_size: m
sources:
  - name: eth_logs
    chain: ethereum
    mode: live
    query:
      logs:
        - address: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]
          topics: [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]]
      field_selection:
        log: [address, topic0, data, block_number, log_index, transaction_hash]
        block: [number, timestamp, hash]
processors:
  - name: decode
    module: builtin/evm-abi-decoder@1
    inputs: [eth_logs]
    config:
      abis: []
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [decode]
"#;

fn load(s: &str) -> Result<Config> {
    Config::load_str(s, &no_secrets())
}

#[test]
fn good_config_parses_and_validates() {
    let cfg = load(GOOD).expect("should parse");
    assert_eq!(cfg.name, "usdc-multichain");
    assert_eq!(cfg.sources.len(), 1);
    assert_eq!(cfg.sources[0].endpoint().chain_id, 1);
    assert_eq!(cfg.sources[0].endpoint().url, "https://eth.hypersync.xyz");
    assert_eq!(cfg.summary(), "valid: 1 source(s), 1 processor(s), 1 sink(s)");
    assert_eq!(cfg.profile().default_max_records, 5_000); // size m
}

#[test]
fn reorg_defaults_to_enabled_with_window_64() {
    let cfg = load(GOOD).unwrap();
    assert!(cfg.sources[0].reorg.enabled);
    assert_eq!(cfg.sources[0].reorg.window, 64);
}

#[test]
fn reorg_block_parses() {
    let s = GOOD.replace(
        "    mode: live\n",
        "    mode: live\n    reorg:\n      enabled: false\n",
    );
    let cfg = load(&s).unwrap();
    assert!(!cfg.sources[0].reorg.enabled);

    let s = GOOD.replace(
        "    mode: live\n",
        "    mode: live\n    reorg:\n      window: 200\n",
    );
    let cfg = load(&s).unwrap();
    assert!(cfg.sources[0].reorg.enabled);
    assert_eq!(cfg.sources[0].reorg.window, 200);
}

#[test]
fn err_reorg_window_bounds() {
    let s = GOOD.replace(
        "    mode: live\n",
        "    mode: live\n    reorg:\n      window: 0\n",
    );
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("reorg.window"), "got {err}");

    let s = GOOD.replace(
        "    mode: live\n",
        "    mode: live\n    reorg:\n      window: 999999\n",
    );
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("too large"), "got {err}");
}

#[test]
fn err_unprotected_head_ingestion() {
    // confirmations: 0 (at head) + reorg tracking off = corrupt data on any
    // reorg with no recovery path; refuse the combination for live sources.
    let s = GOOD.replace(
        "    mode: live\n",
        "    mode: live\n    confirmations: 0\n    reorg:\n      enabled: false\n",
    );
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("confirmations"), "got {err}");
    // safe-distance users: high confirmations with tracking off is fine
    let s = GOOD.replace(
        "    mode: live\n",
        "    mode: live\n    confirmations: 100\n    reorg:\n      enabled: false\n",
    );
    assert!(load(&s).is_ok());
}

#[test]
fn err_unknown_key_is_rejected() {
    let s = GOOD.replace("resource_size: m", "resource_size: m\n  bogus_key: 3");
    let err = load(&s).unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}

#[test]
fn err_no_sinks() {
    let s = r#"
name: x
sources:
  - name: s
    chain: ethereum
    query: {}
sinks: []
"#;
    let err = load(s).unwrap_err();
    assert!(err.to_string().contains("no sinks"), "got {err}");
}

#[test]
fn err_input_does_not_resolve() {
    let s = GOOD.replace("inputs: [eth_logs]", "inputs: [nonexistent]");
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("does not name any node"), "got {err}");
}

#[test]
fn err_cycle_detected() {
    // decode -> loopback -> decode
    let s = r#"
name: x
sources:
  - name: s
    chain: ethereum
    query: {}
processors:
  - name: a
    module: builtin/filter@1
    inputs: [s, b]
  - name: b
    module: builtin/filter@1
    inputs: [a]
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [b]
"#;
    let err = load(s).unwrap_err();
    assert!(err.to_string().contains("cycle"), "got {err}");
}

#[test]
fn err_backfill_without_to_block() {
    let s = GOOD.replace("mode: live", "mode: backfill");
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("requires `to_block`"), "got {err}");
}

#[test]
fn err_live_with_to_block() {
    let s = GOOD.replace("mode: live", "mode: live\n    to_block: 100");
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("forbids `to_block`"), "got {err}");
}

#[test]
fn err_unknown_chain() {
    let s = GOOD.replace("chain: ethereum", "chain: narnia");
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("unknown chain"), "got {err}");
}

#[test]
fn err_orphan_processor() {
    // `dead` produces but nothing consumes it
    let s = r#"
name: x
sources:
  - name: s
    chain: ethereum
    query: {}
processors:
  - name: dead
    module: builtin/filter@1
    inputs: [s]
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [s]
"#;
    let err = load(s).unwrap_err();
    assert!(err.to_string().contains("orphan"), "got {err}");
}

#[test]
fn err_missing_secret_lists_all() {
    let s = r#"
name: x
sources:
  - name: s
    chain: ethereum
    query: {}
sinks:
  - name: out
    module: builtin/postgres@1
    connections: [pg]
    config:
      connection: pg
      table: t
    inputs: [s]
connections:
  pg:
    type: postgres
    dsn: "postgres://${secret:PG_DSN}@h/db?x=${secret:OTHER}"
"#;
    let err = load(s).unwrap_err();
    match err {
        ConfigError::MissingSecrets(names) => {
            assert_eq!(names, vec!["OTHER".to_string(), "PG_DSN".to_string()]);
        }
        other => panic!("expected MissingSecrets, got {other:?}"),
    }
}

#[test]
fn secret_resolves_when_present() {
    let s = r#"
name: x
sources:
  - name: s
    chain: ethereum
    query: {}
sinks:
  - name: out
    module: builtin/postgres@1
    connections: [pg]
    config:
      connection: pg
      table: t
    inputs: [s]
connections:
  pg:
    type: postgres
    dsn: "postgres://${secret:PG_DSN}@h/db"
"#;
    let cfg = Config::load_str(s, &secrets(&[("PG_DSN", "u:p")])).unwrap();
    assert_eq!(
        cfg.connections["pg"].dsn.as_deref(),
        Some("postgres://u:p@h/db")
    );
}

#[test]
fn err_postgres_sink_missing_connection_grant() {
    let s = r#"
name: x
sources:
  - name: s
    chain: ethereum
    query: {}
sinks:
  - name: out
    module: builtin/postgres@1
    config:
      connection: pg
      table: t
    inputs: [s]
connections:
  pg:
    type: postgres
    dsn: "postgres://u:p@h/db"
"#;
    let err = load(s).unwrap_err();
    assert!(
        err.to_string().contains("must also be listed in `connections`"),
        "got {err}"
    );
}

#[test]
fn err_bad_builtin_ref() {
    let s = GOOD.replace("builtin/evm-abi-decoder@1", "builtin/evm-abi-decoder");
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("not a valid"), "got {err}");
}

#[test]
fn file_module_ref_parses() {
    let s = GOOD.replace(
        "module: builtin/evm-abi-decoder@1",
        "module:\n      file: ./modules/enrich.wasm",
    );
    let cfg = load(&s).unwrap();
    match &cfg.processors[0].module {
        ModuleRef::File { file } => assert_eq!(file.to_str().unwrap(), "./modules/enrich.wasm"),
        other => panic!("expected file ref, got {other:?}"),
    }
}

#[test]
fn err_orphan_source() {
    // `dead_src` feeds nothing — data would silently vanish.
    let s = r#"
name: x
sources:
  - name: s
    chain: ethereum
    query: {}
  - name: dead_src
    chain: base
    query: {}
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [s]
"#;
    let err = load(s).unwrap_err();
    assert!(
        err.to_string().contains("sources.dead_src") && err.to_string().contains("orphan"),
        "got {err}"
    );
}

#[test]
fn duplicate_node_name_rejected() {
    let s = GOOD.replace("name: out", "name: decode"); // sink named same as processor
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("duplicate node name"), "got {err}");
}
