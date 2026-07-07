//! Pure buffered-flush logic for the s3 sink (§7.1). Native-testable.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use serde_json::Value;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Format {
    Parquet,
    Ndjson,
}

impl Format {
    pub fn ext(self) -> &'static str {
        match self {
            Format::Parquet => "parquet",
            Format::Ndjson => "ndjson",
        }
    }
}

pub struct S3Config {
    pub connection: String,
    pub format: Format,
    pub flush_rows: usize,
}

impl S3Config {
    pub fn from_json(config: &Value) -> Result<Self, String> {
        let connection = config
            .get("connection")
            .and_then(|v| v.as_str())
            .ok_or("s3: config.connection required")?
            .to_string();
        let format = match config.get("format").and_then(|v| v.as_str()) {
            Some("ndjson") => Format::Ndjson,
            Some("parquet") | None => Format::Parquet,
            Some(other) => return Err(format!("s3: unknown format `{other}`")),
        };
        let flush_rows = config
            .get("flush_rows")
            .and_then(|v| v.as_u64())
            .unwrap_or(100_000)
            .max(1) as usize;
        Ok(S3Config {
            connection,
            format,
            flush_rows,
        })
    }
}

/// Accumulates records for ONE chain until a flush trigger fires.
#[derive(Default)]
pub struct Buffer {
    pub records: Vec<Value>,
    pub chain_id: u64,
    pub first_block: u64,
    pub last_block: u64,
    seen: bool,
}

impl Buffer {
    pub fn push(&mut self, chain_id: u64, block_lo: u64, block_hi: u64, records: &[Value]) {
        if records.is_empty() {
            return;
        }
        self.chain_id = chain_id;
        if !self.seen {
            self.first_block = block_lo;
            self.seen = true;
        }
        self.last_block = self.last_block.max(block_hi);
        self.records.extend_from_slice(records);
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Deterministic object key: `{chain_id}/{first}-{last}-{rows}.{ext}`. The
    /// host prepends the connection prefix. Stable range → idempotent overwrite.
    pub fn object_key(&self, format: Format) -> String {
        format!(
            "{}/{}-{}-{}.{}",
            self.chain_id,
            self.first_block,
            self.last_block,
            self.records.len(),
            format.ext()
        )
    }

    /// Encode + return (key, bytes), then reset. Empty buffer -> None.
    pub fn take(&mut self, format: Format) -> Result<Option<(String, Vec<u8>)>, String> {
        if self.records.is_empty() {
            return Ok(None);
        }
        let key = self.object_key(format);
        let bytes = match format {
            Format::Ndjson => to_ndjson(&self.records)?,
            Format::Parquet => to_parquet(&self.records)?,
        };
        self.records.clear();
        self.seen = false;
        self.last_block = 0;
        Ok(Some((key, bytes)))
    }

    /// Reorg rollback: drop buffered records past the fork before they ever
    /// reach an object. Records without a parseable `block_number` are kept
    /// (can't be judged; the sink is append-only for those anyway).
    pub fn rollback(&mut self, invalidate_after_block: u64) {
        self.records.retain(|r| {
            record_block_number(r).map_or(true, |b| b <= invalidate_after_block)
        });
        if self.records.is_empty() {
            self.seen = false;
            self.first_block = 0;
            self.last_block = 0;
        } else {
            self.last_block = self.last_block.min(invalidate_after_block);
        }
    }
}

/// A record's block number: JSON number, decimal string, or 0x-hex.
fn record_block_number(rec: &Value) -> Option<u64> {
    match rec.get("block_number")? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => {
            if let Some(h) = s.strip_prefix("0x") {
                u64::from_str_radix(h, 16).ok()
            } else {
                s.parse().ok()
            }
        }
        _ => None,
    }
}

/// Per-chain buffers (§7.1). A sink fed by a multi-chain DAG (fan-in) must NOT
/// mix chains in one object: the `{chain_id}/{first}-{last}` key would lie and
/// interleaving would make object contents non-deterministic across replays —
/// breaking the idempotent-overwrite guarantee. Records within one chain arrive
/// in block order, so per-chain objects stay deterministic.
#[derive(Default)]
pub struct ChainBuffers {
    by_chain: std::collections::BTreeMap<u64, Buffer>,
}

impl ChainBuffers {
    pub fn push(&mut self, chain_id: u64, block_lo: u64, block_hi: u64, records: &[Value]) {
        self.by_chain
            .entry(chain_id)
            .or_default()
            .push(chain_id, block_lo, block_hi, records);
    }

    /// Flush every chain whose buffer reached `flush_rows`.
    pub fn take_ready(
        &mut self,
        format: Format,
        flush_rows: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, String> {
        let mut out = Vec::new();
        for buf in self.by_chain.values_mut() {
            if buf.len() >= flush_rows {
                if let Some(obj) = buf.take(format)? {
                    out.push(obj);
                }
            }
        }
        Ok(out)
    }

    /// Flush everything (shutdown / checkpoint barrier).
    pub fn take_all(&mut self, format: Format) -> Result<Vec<(String, Vec<u8>)>, String> {
        let mut out = Vec::new();
        for buf in self.by_chain.values_mut() {
            if let Some(obj) = buf.take(format)? {
                out.push(obj);
            }
        }
        Ok(out)
    }

    /// Reorg rollback for one chain's buffer (others are untouched).
    pub fn rollback(&mut self, chain_id: u64, invalidate_after_block: u64) {
        if let Some(buf) = self.by_chain.get_mut(&chain_id) {
            buf.rollback(invalidate_after_block);
        }
    }
}

fn to_ndjson(records: &[Value]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    for r in records {
        out.extend_from_slice(&serde_json::to_vec(r).map_err(|e| e.to_string())?);
        out.push(b'\n');
    }
    Ok(out)
}

/// Column order = UNION of keys over all records, sorted (deterministic).
/// Records in one buffer can differ in shape (optional fields, mixed event
/// types); taking only the first record's keys would silently drop columns.
fn column_order(records: &[Value]) -> Vec<String> {
    let mut cols: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for r in records {
        if let Some(o) = r.as_object() {
            cols.extend(o.keys().cloned());
        }
    }
    cols.into_iter().collect()
}

fn cell_to_string(v: Option<&Value>) -> Option<String> {
    match v {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(other) => Some(other.to_string()), // nested object/array -> JSON text
    }
}

/// Encode records as a single-row-group Parquet file. All columns are Utf8 —
/// safe for blockchain data (uint256 stays a decimal string) and schema-stable.
fn to_parquet(records: &[Value]) -> Result<Vec<u8>, String> {
    let cols = column_order(records);
    if cols.is_empty() {
        return Err("s3: cannot infer schema from empty records".into());
    }
    let fields: Vec<Field> = cols.iter().map(|c| Field::new(c, DataType::Utf8, true)).collect();
    let schema = Arc::new(Schema::new(fields));

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for c in &cols {
        let vals: Vec<Option<String>> =
            records.iter().map(|r| cell_to_string(r.get(c))).collect();
        arrays.push(Arc::new(StringArray::from(vals)) as ArrayRef);
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
        writer.close().map_err(|e| e.to_string())?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn recs(n: usize) -> Vec<Value> {
        (0..n)
            .map(|i| json!({ "chain_id": 1, "block_number": 19000000 + i, "amount": "5000000000000" }))
            .collect()
    }

    #[test]
    fn key_is_deterministic() {
        let mut b = Buffer::default();
        b.push(1, 19000000, 19000010, &recs(3));
        assert_eq!(b.object_key(Format::Parquet), "1/19000000-19000010-3.parquet");
    }

    #[test]
    fn flushes_and_resets() {
        let mut b = Buffer::default();
        b.push(1, 100, 110, &recs(3));
        let (key, bytes) = b.take(Format::Ndjson).unwrap().unwrap();
        assert!(key.ends_with(".ndjson"));
        assert_eq!(String::from_utf8(bytes).unwrap().lines().count(), 3);
        assert!(b.is_empty());
        assert!(b.take(Format::Ndjson).unwrap().is_none());
    }

    #[test]
    fn parquet_encodes_nonempty() {
        let mut b = Buffer::default();
        b.push(1, 100, 110, &recs(5));
        let (_key, bytes) = b.take(Format::Parquet).unwrap().unwrap();
        // Parquet magic header/footer is "PAR1".
        assert_eq!(&bytes[0..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    }

    #[test]
    fn config_defaults_to_parquet() {
        let c = S3Config::from_json(&json!({ "connection": "lake" })).unwrap();
        assert_eq!(c.format, Format::Parquet);
        assert_eq!(c.flush_rows, 100_000);
    }

    #[test]
    fn chains_never_mix_in_one_object() {
        // Fan-in from a multi-chain decode: chain 1 and chain 8453 interleave.
        let mut cb = ChainBuffers::default();
        cb.push(1, 100, 110, &recs(2));
        cb.push(8453, 500, 510, &recs(3));
        cb.push(1, 110, 120, &recs(2));

        let all = cb.take_all(Format::Ndjson).unwrap();
        let keys: Vec<&str> = all.iter().map(|(k, _)| k.as_str()).collect();
        // One object per chain, each spanning only its own chain's blocks.
        assert_eq!(keys, vec!["1/100-120-4.ndjson", "8453/500-510-3.ndjson"]);
    }

    #[test]
    fn take_ready_flushes_only_full_chains() {
        let mut cb = ChainBuffers::default();
        cb.push(1, 100, 110, &recs(5));    // reaches flush_rows
        cb.push(8453, 500, 510, &recs(2)); // still below
        let ready = cb.take_ready(Format::Ndjson, 5).unwrap();
        assert_eq!(ready.len(), 1);
        assert!(ready[0].0.starts_with("1/"));
        // The lagging chain flushes later, on the barrier.
        let rest = cb.take_all(Format::Ndjson).unwrap();
        assert_eq!(rest.len(), 1);
        assert!(rest[0].0.starts_with("8453/"));
    }

    #[test]
    fn parquet_schema_is_union_of_all_records() {
        // First record lacks `amount`; a first-record-only schema would drop it.
        let records = vec![
            json!({ "chain_id": 1, "block_number": 100 }),
            json!({ "chain_id": 1, "block_number": 101, "amount": "5" }),
        ];
        assert_eq!(column_order(&records), vec!["amount", "block_number", "chain_id"]);
        let bytes = to_parquet(&records).unwrap();
        assert_eq!(&bytes[0..4], b"PAR1");
        // non-object first record must not break schema inference either
        let mixed = vec![json!("junk"), json!({ "a": 1 })];
        assert_eq!(column_order(&mixed), vec!["a"]);
    }

    #[test]
    fn rollback_purges_buffered_records_past_fork() {
        let mut cb = ChainBuffers::default();
        cb.push(1, 100, 106, &[
            json!({ "block_number": 100, "v": "a" }),
            json!({ "block_number": "0x69", "v": "b" }), // 105, hex form
            json!({ "no_block": true }),                  // unjudgeable -> kept
        ]);
        cb.push(8453, 500, 510, &recs(2)); // other chain untouched
        cb.rollback(1, 102);

        let all = cb.take_all(Format::Ndjson).unwrap();
        let chain1 = all.iter().find(|(k, _)| k.starts_with("1/")).unwrap();
        let body = String::from_utf8(chain1.1.clone()).unwrap();
        assert!(body.contains("\"block_number\":100"));
        assert!(!body.contains("0x69"), "reorged record must be purged");
        assert!(body.contains("no_block"));
        // key's last-block clamps to the fork
        assert!(chain1.0.starts_with("1/100-102-"), "got {}", chain1.0);
        assert!(all.iter().any(|(k, _)| k.starts_with("8453/")));
    }

    #[test]
    fn rollback_emptying_buffer_resets_it() {
        let mut cb = ChainBuffers::default();
        cb.push(1, 100, 110, &[json!({ "block_number": 105 })]);
        cb.rollback(1, 90);
        assert!(cb.take_all(Format::Ndjson).unwrap().is_empty());
        // rollback on an unknown chain is a no-op
        cb.rollback(999, 0);
    }

    #[test]
    fn replay_produces_identical_key_and_bytes() {
        // Idempotent-overwrite guarantee: same input range -> same object.
        let build = || {
            let mut cb = ChainBuffers::default();
            cb.push(1, 100, 110, &recs(3));
            cb.take_all(Format::Ndjson).unwrap()
        };
        assert_eq!(build(), build());
    }
}
