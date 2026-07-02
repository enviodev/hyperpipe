//! HyperPipe batch envelope + codecs.
//!
//! This crate is shared by the native host and by guest (wasm) modules, so it
//! stays dependency-light and target-agnostic: only `serde`/`serde_json`. See
//! ARCHITECTURE.md §3.1 (record envelope) and §6.2 (WIT boundary encoding).

use serde::{Deserialize, Serialize};

pub const SCHEMA_V1: &str = "hyperpipe/batch/v1";

/// Boundary encoding tag. Mirrors the WIT `encoding` enum. v1 ships JSON;
/// CBOR / Arrow-IPC are non-breaking upgrades keyed off this tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Encoding {
    Json,
    Cbor,
    ArrowIpc,
}

impl Encoding {
    /// Stable u8 tag, matching the ordinal of the WIT enum (json=0, cbor=1, arrow-ipc=2).
    pub fn as_u8(self) -> u8 {
        match self {
            Encoding::Json => 0,
            Encoding::Cbor => 1,
            Encoding::ArrowIpc => 2,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Encoding::Json),
            1 => Some(Encoding::Cbor),
            2 => Some(Encoding::ArrowIpc),
            _ => None,
        }
    }
}

/// The kind of records a batch carries. `control` batches carry pipeline
/// signals (eof / rollback) rather than data — see [`ControlRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BatchKind {
    Log,
    Transaction,
    Block,
    Trace,
    Decoded,
    Custom,
    Control,
}

/// Half-open block range `[from, to)` — mirrors HyperSync semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRange(pub u64, pub u64);

impl BlockRange {
    pub fn from(&self) -> u64 {
        self.0
    }
    pub fn to(&self) -> u64 {
        self.1
    }
}

/// The batch envelope. Everything crossing a stage boundary is one of these.
///
/// `records` are untyped JSON values whose shape depends on `kind`. Log /
/// transaction / block / trace records mirror the HyperSync `field_selection`
/// 1:1; `decoded` records follow [`DecodedLog`]; `control` records follow
/// [`ControlRecord`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    /// Envelope schema version. Always `SCHEMA_V1` for now.
    pub schema: String,
    /// Deterministic id: `source:from:to:seq`. Stable across retries/replays.
    pub batch_id: String,
    /// Node name of the source this batch originated from.
    pub source: String,
    pub chain_id: u64,
    pub block_range: BlockRange,
    /// Cursor-advance override (§8.2 rule 1). When a block range is split into
    /// several chunk batches, only the FINAL chunk may advance the cursor to
    /// `block_range.to()`; earlier chunks carry `Some(block_range.from())` so a
    /// crash between chunk acks replays the whole range instead of losing the
    /// tail. `None` = ack the full range (the common single-batch case).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack_block: Option<u64>,
    pub kind: BatchKind,
    pub records: Vec<serde_json::Value>,
}

impl Batch {
    pub fn new(
        source: impl Into<String>,
        chain_id: u64,
        block_range: BlockRange,
        seq: u64,
        kind: BatchKind,
        records: Vec<serde_json::Value>,
    ) -> Self {
        let source = source.into();
        let batch_id = make_batch_id(&source, block_range, seq);
        Batch {
            schema: SCHEMA_V1.to_string(),
            batch_id,
            source,
            chain_id,
            block_range,
            ack_block: None,
            kind,
            records,
        }
    }

    /// Build a control batch (eof / rollback / checkpoint-marker).
    pub fn control(source: impl Into<String>, chain_id: u64, ctrl: ControlRecord) -> Self {
        let source = source.into();
        let at = ctrl.at_block();
        let rec = serde_json::to_value(&ctrl).expect("control record serializes");
        Batch {
            schema: SCHEMA_V1.to_string(),
            batch_id: format!("{source}:ctrl:{at}"),
            source,
            chain_id,
            block_range: BlockRange(at, at),
            ack_block: None,
            kind: BatchKind::Control,
            records: vec![rec],
        }
    }

    /// The block a sink may advance its cursor to after acking this batch.
    pub fn ack_block(&self) -> u64 {
        self.ack_block.unwrap_or(self.block_range.1)
    }

    pub fn is_control(&self) -> bool {
        matches!(self.kind, BatchKind::Control)
    }

    /// Produce a new batch from this one with different `kind`/`records`,
    /// preserving identity (`batch_id`, `source`, `chain_id`, `block_range`,
    /// `ack_block`). Keeping `batch_id` stable is what lets downstream sinks
    /// dedupe across processing stages; keeping `ack_block` is what keeps the
    /// chunk-ack safety of §8.2 intact through transforms.
    pub fn derive(&self, kind: BatchKind, records: Vec<serde_json::Value>) -> Batch {
        Batch {
            schema: self.schema.clone(),
            batch_id: self.batch_id.clone(),
            source: self.source.clone(),
            chain_id: self.chain_id,
            block_range: self.block_range,
            ack_block: self.ack_block,
            kind,
            records,
        }
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Parse the single control record out of a control batch, if present.
    pub fn as_control(&self) -> Option<ControlRecord> {
        if !self.is_control() {
            return None;
        }
        self.records
            .first()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }
}

/// `source:from:to:seq` — deterministic so retries/replays reuse the same id
/// (this is the key downstream sinks dedupe on).
pub fn make_batch_id(source: &str, range: BlockRange, seq: u64) -> String {
    format!("{source}:{}:{}:{seq}", range.0, range.1)
}

/// Pipeline control signals. Carried in `control` batches, in stream order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "control", rename_all = "lowercase")]
pub enum ControlRecord {
    /// Reorg: everything strictly after `invalidate_after_block` is void (phase 2).
    Rollback {
        chain_id: u64,
        invalidate_after_block: u64,
    },
    /// A backfill source exhausted its configured range; lets job-mode terminate.
    Eof { source: String, at_block: u64 },
}

impl ControlRecord {
    fn at_block(&self) -> u64 {
        match self {
            ControlRecord::Rollback {
                invalidate_after_block,
                ..
            } => *invalidate_after_block,
            ControlRecord::Eof { at_block, .. } => *at_block,
        }
    }
}

/// Canonical `decoded` record produced by the ABI decoder (§3.1).
/// Kept as a typed helper for producers/consumers; still serialized as a plain
/// JSON object into `Batch::records`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodedLog {
    pub chain_id: u64,
    pub block_number: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_timestamp: Option<u64>,
    pub transaction_hash: String,
    pub log_index: u64,
    pub address: String,
    pub event: String,
    pub signature: String,
    /// Decoded params. Integers >= 2^53 MUST be decimal strings (§3.1).
    pub params: serde_json::Map<String, serde_json::Value>,
}

/// Module-scoped durable key-value store (§6.2 `kv-get`/`kv-set`). Defined here
/// so both the host and the checkpoint store can reference it without a crate
/// cycle. Keys are namespaced by module name.
pub trait KvStore: Send + Sync {
    fn get(&self, module: &str, key: &str) -> Option<Vec<u8>>;
    fn set(&self, module: &str, key: &str, value: Vec<u8>);
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("unsupported encoding for this build: {0:?}")]
    Unsupported(Encoding),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// Decode a batch from tagged bytes. Only JSON is wired in v1.
pub fn decode(encoding: Encoding, data: &[u8]) -> Result<Batch, CodecError> {
    match encoding {
        Encoding::Json => Ok(serde_json::from_slice(data)?),
        other => Err(CodecError::Unsupported(other)),
    }
}

/// Encode a batch to tagged bytes. Only JSON is wired in v1.
pub fn encode(encoding: Encoding, batch: &Batch) -> Result<Vec<u8>, CodecError> {
    match encoding {
        Encoding::Json => Ok(serde_json::to_vec(batch)?),
        other => Err(CodecError::Unsupported(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn batch_id_is_deterministic() {
        let r = BlockRange(100, 200);
        let a = Batch::new("eth", 1, r, 0, BatchKind::Log, vec![]);
        let b = Batch::new("eth", 1, r, 0, BatchKind::Log, vec![]);
        assert_eq!(a.batch_id, b.batch_id);
        assert_eq!(a.batch_id, "eth:100:200:0");
    }

    #[test]
    fn roundtrip_json() {
        let batch = Batch::new(
            "eth_usdc",
            1,
            BlockRange(19_000_000, 19_000_100),
            0,
            BatchKind::Log,
            vec![json!({"address": "0xabc", "value": "1250000000"})],
        );
        let bytes = encode(Encoding::Json, &batch).unwrap();
        let back = decode(Encoding::Json, &bytes).unwrap();
        assert_eq!(back.batch_id, batch.batch_id);
        assert_eq!(back.records.len(), 1);
        assert_eq!(back.records[0]["value"], "1250000000");
    }

    #[test]
    fn control_roundtrip() {
        let b = Batch::control(
            "eth",
            1,
            ControlRecord::Eof {
                source: "eth".into(),
                at_block: 19_100_000,
            },
        );
        assert!(b.is_control());
        match b.as_control().unwrap() {
            ControlRecord::Eof { at_block, .. } => assert_eq!(at_block, 19_100_000),
            _ => panic!("wrong control"),
        }
    }

    #[test]
    fn encoding_tag_stable() {
        assert_eq!(Encoding::Json.as_u8(), 0);
        assert_eq!(Encoding::from_u8(2), Some(Encoding::ArrowIpc));
        assert_eq!(Encoding::from_u8(9), None);
    }

    #[test]
    fn ack_block_defaults_to_range_end() {
        let b = Batch::new("s", 1, BlockRange(100, 200), 0, BatchKind::Log, vec![]);
        assert_eq!(b.ack_block(), 200);
    }

    #[test]
    fn ack_block_override_survives_roundtrip_and_derive() {
        // Non-final chunk: acking it must NOT advance past the range start.
        let mut b = Batch::new("s", 1, BlockRange(100, 200), 0, BatchKind::Log, vec![json!({"x":1})]);
        b.ack_block = Some(100);
        assert_eq!(b.ack_block(), 100);

        // Survives the wire (WASM boundary)...
        let bytes = encode(Encoding::Json, &b).unwrap();
        let back = decode(Encoding::Json, &bytes).unwrap();
        assert_eq!(back.ack_block(), 100);

        // ...and survives a transform stage.
        let derived = back.derive(BatchKind::Decoded, vec![json!({"y":2})]);
        assert_eq!(derived.ack_block(), 100);
        assert_eq!(derived.batch_id, b.batch_id);
    }

    #[test]
    fn missing_ack_block_in_old_payloads_falls_back() {
        // Envelope written before the field existed must still parse.
        let json = r#"{"schema":"hyperpipe/batch/v1","batch_id":"s:1:2:0","source":"s",
                       "chain_id":1,"block_range":[1,2],"kind":"log","records":[]}"#;
        let b: Batch = serde_json::from_str(json).unwrap();
        assert_eq!(b.ack_block, None);
        assert_eq!(b.ack_block(), 2);
    }
}
