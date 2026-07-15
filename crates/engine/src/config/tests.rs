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

// ---------------------------------------------------------------------------
// resolve_endpoint matrix (§5.1): chain / chain_id / url in every combination
// ---------------------------------------------------------------------------

/// Replace the source's `chain: ethereum` line with arbitrary endpoint keys.
fn with_endpoint(keys: &str) -> String {
    GOOD.replace("    chain: ethereum\n", keys)
}

#[test]
fn explicit_chain_id_and_url_win_over_a_named_chain() {
    // Both given: the explicit pair is authoritative — even the chain name is
    // not looked up (so an override works for chains not in the registry).
    let s = with_endpoint("    chain: narnia\n    chain_id: 31337\n    url: http://localhost:8080\n");
    let cfg = load(&s).unwrap();
    assert_eq!(cfg.sources[0].endpoint().chain_id, 31337);
    assert_eq!(cfg.sources[0].endpoint().url, "http://localhost:8080");
}

#[test]
fn named_chain_with_a_chain_id_override_keeps_the_registry_url() {
    let s = with_endpoint("    chain: ethereum\n    chain_id: 1337\n");
    let cfg = load(&s).unwrap();
    assert_eq!(cfg.sources[0].endpoint().chain_id, 1337);
    assert_eq!(cfg.sources[0].endpoint().url, "https://eth.hypersync.xyz");
}

#[test]
fn named_chain_with_a_url_override_keeps_the_registry_chain_id() {
    let s = with_endpoint("    chain: ethereum\n    url: http://127.0.0.1:8799\n");
    let cfg = load(&s).unwrap();
    assert_eq!(cfg.sources[0].endpoint().chain_id, 1);
    assert_eq!(cfg.sources[0].endpoint().url, "http://127.0.0.1:8799");
}

#[test]
fn err_chain_id_without_url_or_chain() {
    let s = with_endpoint("    chain_id: 1\n");
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("without `url`"), "got {err}");
}

#[test]
fn err_url_without_chain_id() {
    let s = with_endpoint("    url: http://127.0.0.1:8799\n");
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("`url` given without `chain_id`"), "got {err}");
}

#[test]
fn err_no_endpoint_at_all() {
    let s = with_endpoint("");
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("specify `chain:`"), "got {err}");
}

#[test]
fn err_unknown_chain_lists_the_known_ones() {
    let s = with_endpoint("    chain: narnia\n");
    let err = load(&s).unwrap_err().to_string();
    assert!(err.contains("unknown chain `narnia`"), "got {err}");
    assert!(err.contains("ethereum"), "the error must list what IS known: {err}");
}

// ---------------------------------------------------------------------------
// Validation branches
// ---------------------------------------------------------------------------

#[test]
fn err_non_hypersync_source_type() {
    let s = GOOD.replace("  - name: eth_logs\n", "  - name: eth_logs\n    type: rpc\n");
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("type `rpc` unsupported"), "got {err}");
}

#[test]
fn err_to_block_not_above_from_block() {
    let s = GOOD.replace(
        "    mode: live\n",
        "    mode: backfill\n    from_block: 100\n    to_block: 100\n",
    );
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("must be > from_block"), "got {err}");

    let s = GOOD.replace(
        "    mode: live\n",
        "    mode: backfill\n    from_block: 100\n    to_block: 50\n",
    );
    assert!(load(&s).is_err());
}

#[test]
fn mode_both_accepts_a_bounded_or_unbounded_range() {
    // `both` = backfill then follow the head; to_block is optional.
    let s = GOOD.replace("    mode: live\n", "    mode: both\n    to_block: 19000100\n");
    assert!(load(&s).is_ok());
    let s = GOOD.replace("    mode: live\n", "    mode: both\n");
    assert!(load(&s).is_ok());
}

#[test]
fn backfill_may_ingest_at_head_without_reorg_tracking() {
    // The exemption: a bounded historical range has no head to reorg under it,
    // so confirmations: 0 + reorg off is a legitimate "just fetch it all" job.
    let s = GOOD.replace(
        "    mode: live\n",
        "    mode: backfill\n    to_block: 19000100\n    confirmations: 0\n    reorg:\n      enabled: false\n",
    );
    let cfg = load(&s).expect("backfill is exempt from the unprotected-head rule");
    assert_eq!(cfg.sources[0].confirmations, 0);
    assert!(!cfg.sources[0].reorg.enabled);
}

#[test]
fn err_sink_grants_an_undefined_connection() {
    let s = GOOD.replace(
        "    module: builtin/stdout@1\n",
        "    module: builtin/stdout@1\n    connections: [ghost]\n",
    );
    let err = load(&s).unwrap_err();
    assert!(
        err.to_string().contains("connection `ghost` is not defined"),
        "got {err}"
    );
}

#[test]
fn err_postgres_connection_without_a_dsn() {
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
"#;
    let err = load(s).unwrap_err();
    assert!(err.to_string().contains("requires `dsn`"), "got {err}");
}

#[test]
fn err_postgres_sink_without_config_connection() {
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
      table: t
    inputs: [s]
connections:
  pg:
    type: postgres
    dsn: "postgres://u:p@h/db"
"#;
    let err = load(s).unwrap_err();
    assert!(
        err.to_string().contains("postgres sink requires `config.connection`"),
        "got {err}"
    );
}

#[test]
fn non_postgres_sinks_may_grant_connections_freely() {
    // Only the postgres builtin has the config.connection rule; an s3 sink
    // carries its connection name in its own config shape.
    let s = r#"
name: x
sources:
  - name: s
    chain: ethereum
    query: {}
sinks:
  - name: lake
    module: builtin/s3@1
    connections: [blob]
    config:
      connection: blob
    inputs: [s]
connections:
  blob:
    type: s3
    local_path: /tmp/lake
"#;
    assert!(load(s).is_ok());
}

#[test]
fn err_input_names_a_sink() {
    // Sinks are terminal: nothing may read from one.
    let s = GOOD.replace("    inputs: [decode]", "    inputs: [decode]\n    # sink `out` below")
        + r#"
  - name: after_sink
    module: builtin/blackhole@1
    inputs: [out]
"#;
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("is a sink (cannot be an input)"), "got {err}");
}

#[test]
fn err_node_lists_itself_as_input() {
    let s = GOOD.replace("    inputs: [eth_logs]", "    inputs: [decode]");
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("lists itself as input"), "got {err}");
}

#[test]
fn err_empty_inputs() {
    let s = GOOD.replace("    inputs: [decode]", "    inputs: []");
    let err = load(&s).unwrap_err();
    assert!(err.to_string().contains("has no `inputs`"), "got {err}");
}

#[test]
fn err_no_sources() {
    let s = r#"
name: x
sources: []
sinks:
  - name: out
    module: builtin/stdout@1
    inputs: [nothing]
"#;
    let err = load(s).unwrap_err();
    assert!(err.to_string().contains("no sources"), "got {err}");
}

// ---------------------------------------------------------------------------
// Accessors / profiles / refs
// ---------------------------------------------------------------------------

#[test]
fn nodes_lists_every_kind() {
    let nodes = load(GOOD).unwrap().nodes();
    assert_eq!(nodes.get("eth_logs"), Some(&NodeKind::Source));
    assert_eq!(nodes.get("decode"), Some(&NodeKind::Processor));
    assert_eq!(nodes.get("out"), Some(&NodeKind::Sink));
    assert_eq!(nodes.len(), 3);
}

#[test]
fn resource_profiles_match_the_documented_table() {
    // §11.1. These numbers are a user-facing contract (sizing docs quote them).
    let s = ResourceSize::S.profile();
    assert_eq!(s.instances_per_stage, 1);
    assert_eq!(s.channel_capacity, 4);
    assert_eq!(s.default_max_records, 1_000);
    assert_eq!(s.wasm_mem_bytes, 128 << 20);
    assert_eq!(s.epoch_deadline_secs, 5);

    let m = ResourceSize::M.profile();
    assert_eq!(m.instances_per_stage, 2);
    assert_eq!(m.channel_capacity, 8);
    assert_eq!(m.default_max_records, 5_000);
    assert_eq!(m.wasm_mem_bytes, 256 << 20);
    assert_eq!(m.epoch_deadline_secs, 5);

    let l = ResourceSize::L.profile();
    assert_eq!(l.instances_per_stage, 4);
    assert_eq!(l.channel_capacity, 16);
    assert_eq!(l.default_max_records, 10_000);
    assert_eq!(l.wasm_mem_bytes, 512 << 20);
    assert_eq!(l.epoch_deadline_secs, 10);

    // The default size is `s`, and Config::profile() reads it off the runtime.
    assert_eq!(ResourceSize::default(), ResourceSize::S);
    let defaulted = GOOD.replace("runtime:\n  resource_size: m\n", "");
    assert_eq!(load(&defaulted).unwrap().profile().default_max_records, 1_000);
}

#[test]
fn checkpoint_defaults() {
    let cfg = load(GOOD).unwrap();
    assert_eq!(cfg.runtime.checkpoint.store, CheckpointStore::Sqlite);
    assert_eq!(cfg.runtime.checkpoint.path, "./state/hyperpipe.db");
    assert_eq!(cfg.runtime.checkpoint.connection, None);
    assert_eq!(cfg.version, 1);
}

#[test]
fn referenced_secrets_finds_each_name_once() {
    assert!(referenced_secrets("no refs here").is_empty());
    assert_eq!(
        referenced_secrets("dsn: ${secret:PG_DSN}").into_iter().collect::<Vec<_>>(),
        vec!["PG_DSN"]
    );
    // repeated names collapse; multiple names come back sorted
    assert_eq!(
        referenced_secrets("${secret:B} ${secret:A} ${secret:B}")
            .into_iter()
            .collect::<Vec<_>>(),
        vec!["A", "B"]
    );
    // an unterminated ref is not a name (and must not hang the scan)
    assert!(referenced_secrets("${secret:UNTERMINATED").is_empty());
    assert_eq!(
        referenced_secrets("${secret:OK} then ${secret:BROKEN")
            .into_iter()
            .collect::<Vec<_>>(),
        vec!["OK"]
    );
}

#[test]
fn module_ref_display_and_parse() {
    assert_eq!(ModuleRef::Builtin("builtin/filter@1".into()).display(), "builtin/filter@1");
    assert_eq!(
        ModuleRef::File { file: PathBuf::from("./enrich.wasm") }.display(),
        "file:./enrich.wasm"
    );

    assert_eq!(
        ModuleRef::Builtin("builtin/filter@2".into()).builtin_parts(),
        Some(("filter".to_string(), 2))
    );
    assert_eq!(ModuleRef::File { file: PathBuf::from("x.wasm") }.builtin_parts(), None);

    // rejected shapes
    for bad in ["builtin/@1", "builtin/x@notanum", "x@1", "builtin/x", "", "builtin/x@"] {
        assert_eq!(parse_builtin_ref(bad), None, "`{bad}` must not parse");
    }
}

#[test]
fn load_from_a_file_path() {
    let dir = std::env::temp_dir().join(format!("hp-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("pipe.yaml");
    std::fs::write(&path, GOOD).unwrap();
    let cfg = Config::load(&path).unwrap();
    assert_eq!(cfg.name, "usdc-multichain");

    match Config::load(dir.join("nope.yaml")) {
        Err(ConfigError::Io { path, .. }) => assert!(path.ends_with("nope.yaml")),
        other => panic!("expected Io error, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}
