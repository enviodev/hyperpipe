//! Pipeline configuration: typed schema, secret resolution, chain resolution,
//! and full validation (§4, §4.1). Parse is strict — unknown keys are errors.

mod chains;
mod secret;

pub use chains::{ChainEntry, ChainRegistry};
pub use secret::{EnvSecrets, SecretSource};

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use serde::Deserialize;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parse error: {0}")]
    Parse(String),
    #[error("unresolved secrets (set env HYPERPIPE_SECRET_<NAME>): {}", .0.join(", "))]
    MissingSecrets(Vec<String>),
    #[error("{0}")]
    Validation(String),
}

impl ConfigError {
    fn v(msg: impl Into<String>) -> Self {
        ConfigError::Validation(msg.into())
    }
}

pub type Result<T> = std::result::Result<T, ConfigError>;

// ---------------------------------------------------------------------------
// Root
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub name: String,
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub runtime: Runtime,
    pub sources: Vec<Source>,
    #[serde(default)]
    pub processors: Vec<Processor>,
    pub sinks: Vec<Sink>,
    #[serde(default)]
    pub connections: BTreeMap<String, Connection>,
}

fn default_version() -> u32 {
    1
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    #[serde(default)]
    pub resource_size: ResourceSize,
    #[serde(default)]
    pub checkpoint: Checkpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResourceSize {
    #[default]
    S,
    M,
    L,
}

/// Concrete knobs derived from [`ResourceSize`] (§11.1).
#[derive(Debug, Clone, Copy)]
pub struct ResourceProfile {
    pub instances_per_stage: usize,
    pub channel_capacity: usize,
    pub default_max_records: usize,
    pub wasm_mem_bytes: usize,
    pub epoch_deadline_secs: u64,
}

impl ResourceSize {
    pub fn profile(self) -> ResourceProfile {
        match self {
            ResourceSize::S => ResourceProfile {
                instances_per_stage: 1,
                channel_capacity: 4,
                default_max_records: 1_000,
                wasm_mem_bytes: 128 << 20,
                epoch_deadline_secs: 5,
            },
            ResourceSize::M => ResourceProfile {
                instances_per_stage: 2,
                channel_capacity: 8,
                default_max_records: 5_000,
                wasm_mem_bytes: 256 << 20,
                epoch_deadline_secs: 5,
            },
            ResourceSize::L => ResourceProfile {
                instances_per_stage: 4,
                channel_capacity: 16,
                default_max_records: 10_000,
                wasm_mem_bytes: 512 << 20,
                epoch_deadline_secs: 10,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    #[serde(default)]
    pub store: CheckpointStore,
    #[serde(default = "default_ckpt_path")]
    pub path: String,
    /// For `store: postgres` — connection name (phase 1); unused for sqlite.
    #[serde(default)]
    pub connection: Option<String>,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Checkpoint {
            store: CheckpointStore::default(),
            path: default_ckpt_path(),
            connection: None,
        }
    }
}

fn default_ckpt_path() -> String {
    "./state/hyperpipe.db".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckpointStore {
    #[default]
    Sqlite,
    Postgres,
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub name: String,
    #[serde(default = "default_source_type")]
    pub r#type: String,
    /// Named chain from the registry. Mutually completes with `chain_id`/`url`.
    #[serde(default)]
    pub chain: Option<String>,
    #[serde(default)]
    pub chain_id: Option<u64>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub mode: SourceMode,
    #[serde(default)]
    pub from_block: Option<u64>,
    #[serde(default)]
    pub to_block: Option<u64>,
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
    #[serde(default)]
    pub reorg: ReorgCfg,
    #[serde(default)]
    pub batch: BatchCfg,
    pub query: Query,

    /// Filled by [`Config::validate`]; not part of the YAML.
    #[serde(skip)]
    pub resolved: Option<ResolvedEndpoint>,
}

fn default_source_type() -> String {
    "hypersync".to_string()
}
fn default_confirmations() -> u64 {
    10
}

/// Reorg stance (§5.2). Two layers, independently configurable:
/// - `confirmations` (above) lags the head so most reorgs never reach the
///   pipeline. Users who want zero rollback complexity set it high and may
///   turn tracking off.
/// - `reorg.enabled` tracks HyperSync's `rollback_guard` block hashes and, on
///   a mismatch, emits a `rollback` control record and rewinds the cursor.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReorgCfg {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// How many recent block hashes to keep per source — the maximum reorg
    /// depth that can be detected and rolled back.
    #[serde(default = "default_reorg_window")]
    pub window: u64,
}

impl Default for ReorgCfg {
    fn default() -> Self {
        ReorgCfg {
            enabled: true,
            window: default_reorg_window(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_reorg_window() -> u64 {
    64
}

#[derive(Debug, Clone)]
pub struct ResolvedEndpoint {
    pub chain_id: u64,
    pub url: String,
}

impl Source {
    /// Endpoint resolved during validation. Panics if called pre-validate.
    pub fn endpoint(&self) -> &ResolvedEndpoint {
        self.resolved
            .as_ref()
            .expect("Source::endpoint called before validate()")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceMode {
    #[default]
    Live,
    Backfill,
    Both,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchCfg {
    #[serde(default)]
    pub max_records: Option<usize>,
    #[serde(default = "default_max_interval")]
    pub max_interval_ms: u64,
}

impl Default for BatchCfg {
    fn default() -> Self {
        BatchCfg {
            max_records: None,
            max_interval_ms: default_max_interval(),
        }
    }
}

fn default_max_interval() -> u64 {
    500
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    #[serde(default)]
    pub logs: Vec<LogSelection>,
    #[serde(default)]
    pub field_selection: FieldSelection,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogSelection {
    #[serde(default)]
    pub address: Vec<String>,
    /// topic filters: outer = topic position (topic0..3), inner = OR-set.
    #[serde(default)]
    pub topics: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldSelection {
    #[serde(default)]
    pub log: Vec<String>,
    #[serde(default)]
    pub block: Vec<String>,
    #[serde(default)]
    pub transaction: Vec<String>,
}

// ---------------------------------------------------------------------------
// Processors / Sinks / shared module ref
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Processor {
    pub name: String,
    pub module: ModuleRef,
    pub inputs: Vec<String>,
    #[serde(default)]
    pub permissions: Permissions,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sink {
    pub name: String,
    pub module: ModuleRef,
    pub inputs: Vec<String>,
    #[serde(default)]
    pub connections: Vec<String>,
    #[serde(default)]
    pub permissions: Permissions,
    #[serde(default)]
    pub config: serde_json::Value,
}

/// `builtin/<name>@<major>` string form, or `{ file: path }` map form.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ModuleRef {
    Builtin(String),
    File { file: PathBuf },
}

impl ModuleRef {
    /// (name, major) for a builtin ref; None for a file ref.
    pub fn builtin_parts(&self) -> Option<(String, u32)> {
        match self {
            ModuleRef::Builtin(s) => parse_builtin_ref(s),
            ModuleRef::File { .. } => None,
        }
    }

    pub fn display(&self) -> String {
        match self {
            ModuleRef::Builtin(s) => s.clone(),
            ModuleRef::File { file } => format!("file:{}", file.display()),
        }
    }
}

/// Parse `builtin/<name>@<major>` -> (name, major).
fn parse_builtin_ref(s: &str) -> Option<(String, u32)> {
    let rest = s.strip_prefix("builtin/")?;
    let (name, major) = rest.split_once('@')?;
    if name.is_empty() {
        return None;
    }
    let major: u32 = major.parse().ok()?;
    Some((name.to_string(), major))
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Permissions {
    /// Hostname allowlist for the `host.http` import.
    #[serde(default)]
    pub http: Vec<String>,
}

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connection {
    pub r#type: ConnectionKind,
    // postgres
    #[serde(default)]
    pub dsn: Option<String>,
    #[serde(default)]
    pub pool: PoolCfg,
    // s3 / object store
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    /// Dev/test: write to a local directory instead of S3.
    #[serde(default)]
    pub local_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionKind {
    Postgres,
    Kafka,
    S3,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolCfg {
    #[serde(default = "default_pool_max")]
    pub max: u32,
}

impl Default for PoolCfg {
    fn default() -> Self {
        PoolCfg {
            max: default_pool_max(),
        }
    }
}

fn default_pool_max() -> u32 {
    8
}

// ---------------------------------------------------------------------------
// Node view (used by DAG validation + the runtime)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Source,
    Processor,
    Sink,
}

// ---------------------------------------------------------------------------
// Load + validate
// ---------------------------------------------------------------------------

impl Config {
    /// Read, parse (strict), resolve secrets, resolve chains, and validate.
    pub fn load(path: impl AsRef<Path>) -> Result<Config> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|e| ConfigError::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        Config::load_str(&text, &EnvSecrets)
    }

    /// Same as [`load`], but from a string and with an injectable secret source.
    pub fn load_str(text: &str, secrets: &dyn SecretSource) -> Result<Config> {
        // 1. YAML -> generic JSON document.
        let mut doc: serde_json::Value =
            serde_yaml::from_str(text).map_err(|e| ConfigError::Parse(e.to_string()))?;

        // 2. Resolve secrets everywhere; report all missing at once.
        let missing = secret::resolve_in_place(&mut doc, secrets);
        if !missing.is_empty() {
            return Err(ConfigError::MissingSecrets(missing.into_iter().collect()));
        }

        // 3. Strict typed deserialize (deny_unknown_fields catches typos).
        let mut config: Config =
            serde_json::from_value(doc).map_err(|e| ConfigError::Parse(e.to_string()))?;

        // 4. Semantic validation + chain resolution.
        config.validate()?;
        Ok(config)
    }

    /// The resource profile for this pipeline.
    pub fn profile(&self) -> ResourceProfile {
        self.runtime.resource_size.profile()
    }

    /// All node names -> kind, for the runtime and diagnostics.
    pub fn nodes(&self) -> BTreeMap<String, NodeKind> {
        let mut m = BTreeMap::new();
        for s in &self.sources {
            m.insert(s.name.clone(), NodeKind::Source);
        }
        for p in &self.processors {
            m.insert(p.name.clone(), NodeKind::Processor);
        }
        for s in &self.sinks {
            m.insert(s.name.clone(), NodeKind::Sink);
        }
        m
    }

    fn validate(&mut self) -> Result<()> {
        // ---- basic presence ----
        if self.sources.is_empty() {
            return Err(ConfigError::v("pipeline has no sources (need >= 1)"));
        }
        if self.sinks.is_empty() {
            return Err(ConfigError::v("pipeline has no sinks (need >= 1)"));
        }

        // ---- unique node names across the whole DAG ----
        // Owned keys so this map does not borrow `self` (we mutate `self.sources`
        // below to fill resolved endpoints).
        let mut node_kind: HashMap<String, NodeKind> = HashMap::new();
        for (name, kind) in self
            .sources
            .iter()
            .map(|s| (s.name.clone(), NodeKind::Source))
            .chain(self.processors.iter().map(|p| (p.name.clone(), NodeKind::Processor)))
            .chain(self.sinks.iter().map(|s| (s.name.clone(), NodeKind::Sink)))
        {
            if node_kind.insert(name.clone(), kind).is_some() {
                return Err(ConfigError::v(format!("duplicate node name `{name}`")));
            }
        }

        // ---- chain resolution + source mode rules ----
        let registry = ChainRegistry::builtin();
        for s in &mut self.sources {
            if s.r#type != "hypersync" {
                return Err(ConfigError::v(format!(
                    "sources.{}.type `{}` unsupported (only `hypersync`)",
                    s.name, s.r#type
                )));
            }
            s.resolved = Some(resolve_endpoint(s, &registry)?);

            match s.mode {
                SourceMode::Backfill => {
                    if s.to_block.is_none() {
                        return Err(ConfigError::v(format!(
                            "sources.{}: mode `backfill` requires `to_block`",
                            s.name
                        )));
                    }
                }
                SourceMode::Live => {
                    if s.to_block.is_some() {
                        return Err(ConfigError::v(format!(
                            "sources.{}: mode `live` forbids `to_block`",
                            s.name
                        )));
                    }
                }
                SourceMode::Both => {}
            }
            if let (Some(f), Some(t)) = (s.from_block, s.to_block) {
                if t <= f {
                    return Err(ConfigError::v(format!(
                        "sources.{}: to_block ({t}) must be > from_block ({f})",
                        s.name
                    )));
                }
            }

            // ---- reorg stance sanity ----
            if s.reorg.enabled {
                if s.reorg.window == 0 {
                    return Err(ConfigError::v(format!(
                        "sources.{}: reorg.window must be >= 1 when reorg tracking is enabled",
                        s.name
                    )));
                }
                if s.reorg.window > 10_000 {
                    return Err(ConfigError::v(format!(
                        "sources.{}: reorg.window {} too large (max 10000)",
                        s.name, s.reorg.window
                    )));
                }
            } else if s.confirmations == 0 && !matches!(s.mode, SourceMode::Backfill) {
                return Err(ConfigError::v(format!(
                    "sources.{}: confirmations: 0 with reorg.enabled: false would ingest \
                     unconfirmed blocks with no rollback path; set confirmations >= 1 \
                     (safe distance from head) or enable reorg tracking",
                    s.name
                )));
            }
        }

        // ---- edges: every input must resolve to a producer (source|processor) ----
        // adjacency: producer -> consumers, and consumer -> producers (for cycle check)
        let mut consumers_of: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut inputs_of: HashMap<&str, Vec<&str>> = HashMap::new();

        let check_inputs = |node: &str, inputs: &[String]| -> Result<()> {
            if inputs.is_empty() {
                return Err(ConfigError::v(format!("{node}: has no `inputs`")));
            }
            for inp in inputs {
                match node_kind.get(inp.as_str()) {
                    None => {
                        return Err(ConfigError::v(format!(
                            "{node}: input `{inp}` does not name any node"
                        )))
                    }
                    Some(NodeKind::Sink) => {
                        return Err(ConfigError::v(format!(
                            "{node}: input `{inp}` is a sink (cannot be an input)"
                        )))
                    }
                    Some(_) => {}
                }
                if inp == node {
                    return Err(ConfigError::v(format!("{node}: lists itself as input")));
                }
            }
            Ok(())
        };

        for p in &self.processors {
            check_inputs(&p.name, &p.inputs)?;
        }
        for s in &self.sinks {
            check_inputs(&s.name, &s.inputs)?;
        }

        // build adjacency after validation of names
        for p in &self.processors {
            for inp in &p.inputs {
                consumers_of.entry(inp.as_str()).or_default().push(&p.name);
                inputs_of.entry(p.name.as_str()).or_default().push(inp.as_str());
            }
        }
        for s in &self.sinks {
            for inp in &s.inputs {
                consumers_of.entry(inp.as_str()).or_default().push(&s.name);
                inputs_of.entry(s.name.as_str()).or_default().push(inp.as_str());
            }
        }

        // ---- no orphan processors or sources (output consumed by nothing) ----
        for p in &self.processors {
            if consumers_of.get(p.name.as_str()).map_or(true, |c| c.is_empty()) {
                return Err(ConfigError::v(format!(
                    "processors.{}: output is not consumed by any node (orphan)",
                    p.name
                )));
            }
        }
        for s in &self.sources {
            if consumers_of.get(s.name.as_str()).map_or(true, |c| c.is_empty()) {
                return Err(ConfigError::v(format!(
                    "sources.{}: no processor or sink consumes this source (orphan)",
                    s.name
                )));
            }
        }

        // ---- no cycles (DFS over consumer edges) ----
        let node_names: Vec<&str> = node_kind.keys().map(|s| s.as_str()).collect();
        detect_cycle(&node_names, &consumers_of)?;

        // ---- module ref sanity ----
        for (name, module) in self
            .processors
            .iter()
            .map(|p| (&p.name, &p.module))
            .chain(self.sinks.iter().map(|s| (&s.name, &s.module)))
        {
            if let ModuleRef::Builtin(s) = module {
                if parse_builtin_ref(s).is_none() {
                    return Err(ConfigError::v(format!(
                        "{name}: module `{s}` is not a valid `builtin/<name>@<major>` ref"
                    )));
                }
            }
        }

        // ---- sink connection grants must exist + type-match ----
        for s in &self.sinks {
            for conn in &s.connections {
                if !self.connections.contains_key(conn) {
                    return Err(ConfigError::v(format!(
                        "sinks.{}: connection `{conn}` is not defined under `connections`",
                        s.name
                    )));
                }
            }
            // A postgres builtin sink must be granted the connection it targets.
            if let Some((mod_name, _)) = s.module.builtin_parts() {
                if mod_name == "postgres" {
                    if let Some(conn) = s.config.get("connection").and_then(|v| v.as_str()) {
                        if !s.connections.iter().any(|c| c == conn) {
                            return Err(ConfigError::v(format!(
                                "sinks.{}: config.connection `{conn}` must also be listed in `connections`",
                                s.name
                            )));
                        }
                    } else {
                        return Err(ConfigError::v(format!(
                            "sinks.{}: postgres sink requires `config.connection`",
                            s.name
                        )));
                    }
                }
            }
        }

        // ---- connections referenced must carry a dsn where needed ----
        for (cname, conn) in &self.connections {
            if conn.r#type == ConnectionKind::Postgres && conn.dsn.is_none() {
                return Err(ConfigError::v(format!(
                    "connections.{cname}: postgres connection requires `dsn`"
                )));
            }
        }

        Ok(())
    }

    /// Summary string for the CLI (`validate` output).
    pub fn summary(&self) -> String {
        format!(
            "valid: {} source(s), {} processor(s), {} sink(s)",
            self.sources.len(),
            self.processors.len(),
            self.sinks.len()
        )
    }
}

fn resolve_endpoint(s: &Source, registry: &ChainRegistry) -> Result<ResolvedEndpoint> {
    // explicit url + chain_id win; otherwise resolve via named chain.
    let (chain_id, url) = match (&s.chain, s.chain_id, &s.url) {
        (_, Some(cid), Some(url)) => (cid, url.clone()),
        (Some(name), cid, url) => {
            let entry = registry.lookup(name).ok_or_else(|| {
                let known: Vec<&String> = registry.names().collect();
                ConfigError::v(format!(
                    "sources.{}: unknown chain `{name}`; known: {}. Use `chain_id:`+`url:` to override.",
                    s.name,
                    known.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
                ))
            })?;
            (cid.unwrap_or(entry.chain_id), url.clone().unwrap_or_else(|| entry.url.clone()))
        }
        (None, Some(cid), None) => {
            return Err(ConfigError::v(format!(
                "sources.{}: chain_id {cid} given without `url` and without a named `chain`",
                s.name
            )))
        }
        (None, None, Some(_)) => {
            return Err(ConfigError::v(format!(
                "sources.{}: `url` given without `chain_id`",
                s.name
            )))
        }
        (None, None, None) => {
            return Err(ConfigError::v(format!(
                "sources.{}: specify `chain:` (named) or `chain_id:`+`url:`",
                s.name
            )))
        }
    };
    Ok(ResolvedEndpoint { chain_id, url })
}

/// DFS 3-color cycle detection over consumer edges. Roots are sources.
fn detect_cycle(nodes: &[&str], consumers_of: &HashMap<&str, Vec<&str>>) -> Result<()> {
    #[derive(Clone, Copy, PartialEq)]
    enum Color {
        White,
        Gray,
        Black,
    }
    let mut color: HashMap<&str, Color> = nodes.iter().map(|k| (*k, Color::White)).collect();

    fn dfs<'a>(
        node: &'a str,
        color: &mut HashMap<&'a str, Color>,
        consumers_of: &HashMap<&'a str, Vec<&'a str>>,
        stack: &mut Vec<&'a str>,
    ) -> Result<()> {
        color.insert(node, Color::Gray);
        stack.push(node);
        if let Some(next) = consumers_of.get(node) {
            for &c in next {
                match color.get(c).copied().unwrap_or(Color::White) {
                    Color::Gray => {
                        // found a back edge -> cycle
                        let from = stack.iter().position(|n| *n == c).unwrap_or(0);
                        let mut cyc: Vec<&str> = stack[from..].to_vec();
                        cyc.push(c);
                        return Err(ConfigError::v(format!(
                            "cycle in DAG: {}",
                            cyc.join(" -> ")
                        )));
                    }
                    Color::White => dfs(c, color, consumers_of, stack)?,
                    Color::Black => {}
                }
            }
        }
        stack.pop();
        color.insert(node, Color::Black);
        Ok(())
    }

    for &n in nodes {
        if color.get(n).copied().unwrap_or(Color::White) == Color::White {
            let mut stack = Vec::new();
            dfs(n, &mut color, consumers_of, &mut stack)?;
        }
    }
    Ok(())
}

/// Names of secrets referenced anywhere in the raw text (for `validate` UX).
pub fn referenced_secrets(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut rest = text;
    const OPEN: &str = "${secret:";
    while let Some(i) = rest.find(OPEN) {
        let after = &rest[i + OPEN.len()..];
        if let Some(end) = after.find('}') {
            out.insert(after[..end].to_string());
            rest = &after[end + 1..];
        } else {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests;
