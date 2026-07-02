//! Pure ABI decode logic — target-agnostic so it unit-tests natively.

use std::collections::HashMap;

use alloy_dyn_abi::{DynSolValue, EventExt};
use alloy_json_abi::JsonAbi;
use alloy_primitives::{Address, B256};
use serde_json::{Map, Value};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OnUndecodable {
    Drop,
    Passthrough,
    Error,
}

impl OnUndecodable {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "drop" => Ok(OnUndecodable::Drop),
            "passthrough" => Ok(OnUndecodable::Passthrough),
            "error" => Ok(OnUndecodable::Error),
            other => Err(format!("on_undecodable: unknown value `{other}`")),
        }
    }
}

struct EventEntry {
    event: alloy_json_abi::Event,
    name: String,
    signature: String,
}

pub struct Decoder {
    by_topic0: HashMap<B256, EventEntry>,
    on_undecodable: OnUndecodable,
}

impl Decoder {
    /// Build a decoder from the module `config` JSON (§7). Each `abis[]` entry
    /// must carry an inline `abi` array (the engine substitutes `file:` for
    /// `abi:` at load time, since modules have no filesystem).
    pub fn from_config(config: &Value) -> Result<Self, String> {
        let on_undecodable = config
            .get("on_undecodable")
            .and_then(|v| v.as_str())
            .map(OnUndecodable::parse)
            .transpose()?
            .unwrap_or(OnUndecodable::Drop);

        let abis = config
            .get("abis")
            .and_then(|v| v.as_array())
            .ok_or("config.abis must be an array")?;

        let mut by_topic0 = HashMap::new();
        for (i, entry) in abis.iter().enumerate() {
            if entry.get("file").is_some() && entry.get("abi").is_none() {
                return Err(format!(
                    "abis[{i}]: `file` was not substituted with `abi` contents by the engine"
                ));
            }
            let abi_val = entry
                .get("abi")
                .ok_or_else(|| format!("abis[{i}]: missing `abi` contents"))?;
            let abi: JsonAbi = serde_json::from_value(abi_val.clone())
                .map_err(|e| format!("abis[{i}]: invalid ABI json: {e}"))?;

            let filter: Option<Vec<String>> = entry
                .get("events")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect());

            for event in abi.events() {
                if let Some(f) = &filter {
                    if !f.iter().any(|n| n == &event.name) {
                        continue;
                    }
                }
                let selector = event.selector();
                by_topic0.insert(
                    selector,
                    EventEntry {
                        event: event.clone(),
                        name: event.name.clone(),
                        signature: event.signature(),
                    },
                );
            }
        }

        if by_topic0.is_empty() {
            return Err("no events selected across all ABIs".into());
        }

        Ok(Decoder {
            by_topic0,
            on_undecodable,
        })
    }

    /// Decode one log record. `Ok(Some(v))` = a decoded record; `Ok(None)` =
    /// drop; `Ok(Some(original))` on passthrough; `Err` on error mode.
    pub fn decode_record(&self, chain_id: u64, rec: &Value) -> Result<Option<Value>, String> {
        let topic0 = match rec.get("topic0").and_then(|v| v.as_str()) {
            Some(t) => t.parse::<B256>().map_err(|e| format!("bad topic0: {e}"))?,
            None => return self.undecodable(rec, "no topic0"),
        };
        let Some(entry) = self.by_topic0.get(&topic0) else {
            return self.undecodable(rec, "unknown event");
        };

        let mut topics = vec![topic0];
        for k in ["topic1", "topic2", "topic3"] {
            if let Some(s) = rec.get(k).and_then(|v| v.as_str()) {
                if !s.is_empty() {
                    topics.push(s.parse::<B256>().map_err(|e| format!("bad {k}: {e}"))?);
                }
            }
        }

        let data_hex = rec.get("data").and_then(|v| v.as_str()).unwrap_or("0x");
        let data = alloy_primitives::hex::decode(data_hex.trim_start_matches("0x"))
            .map_err(|e| format!("bad data hex: {e}"))?;

        let decoded = entry
            .event
            .decode_log_parts(topics.iter().copied(), &data, false)
            .map_err(|e| format!("decode {}: {e}", entry.name))?;

        // Pair input names with values, respecting indexed vs body ordering.
        let mut params = Map::new();
        let mut indexed = decoded.indexed.into_iter();
        let mut body = decoded.body.into_iter();
        for (i, input) in entry.event.inputs.iter().enumerate() {
            let val = if input.indexed {
                indexed.next()
            } else {
                body.next()
            };
            let Some(val) = val else { continue };
            let key = if input.name.is_empty() {
                format!("arg{i}")
            } else {
                input.name.clone()
            };
            params.insert(key, dyn_to_json(&val));
        }

        let mut out = Map::new();
        out.insert("chain_id".into(), Value::from(chain_id));
        out.insert(
            "block_number".into(),
            Value::from(u64_field(rec, "block_number").unwrap_or(0)),
        );
        if let Some(ts) = u64_field(rec, "block_timestamp") {
            out.insert("block_timestamp".into(), Value::from(ts));
        }
        if let Some(bh) = rec.get("block_hash").and_then(|v| v.as_str()) {
            out.insert("block_hash".into(), Value::from(bh));
        }
        if let Some(tx) = rec.get("transaction_hash").and_then(|v| v.as_str()) {
            out.insert("transaction_hash".into(), Value::from(tx));
        }
        out.insert(
            "log_index".into(),
            Value::from(u64_field(rec, "log_index").unwrap_or(0)),
        );
        if let Some(addr) = rec.get("address").and_then(|v| v.as_str()) {
            out.insert("address".into(), Value::from(addr));
        }
        out.insert("event".into(), Value::from(entry.name.clone()));
        out.insert("signature".into(), Value::from(entry.signature.clone()));
        out.insert("params".into(), Value::Object(params));

        Ok(Some(Value::Object(out)))
    }

    fn undecodable(&self, rec: &Value, why: &str) -> Result<Option<Value>, String> {
        match self.on_undecodable {
            OnUndecodable::Drop => Ok(None),
            OnUndecodable::Passthrough => Ok(Some(rec.clone())),
            OnUndecodable::Error => Err(format!("undecodable log ({why})")),
        }
    }
}

/// Accept a u64 field encoded as a JSON number, a decimal string, or a 0x-hex
/// string (HyperSync numeric fields vary).
fn u64_field(rec: &Value, key: &str) -> Option<u64> {
    match rec.get(key)? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => {
            if let Some(h) = s.strip_prefix("0x") {
                u64::from_str_radix(h, 16).ok()
            } else {
                s.parse::<u64>().ok()
            }
        }
        _ => None,
    }
}

/// Convert a decoded value to JSON. Integers are rendered as decimal strings so
/// values >= 2^53 survive JSON without float truncation (§3.1).
fn dyn_to_json(v: &DynSolValue) -> Value {
    match v {
        DynSolValue::Bool(b) => Value::Bool(*b),
        DynSolValue::Int(i, _) => Value::String(i.to_string()),
        DynSolValue::Uint(u, _) => Value::String(u.to_string()),
        DynSolValue::Address(a) => Value::String(a.to_checksum(None)),
        DynSolValue::FixedBytes(b, _) => Value::String(format!("0x{}", alloy_primitives::hex::encode(b))),
        DynSolValue::Bytes(b) => Value::String(format!("0x{}", alloy_primitives::hex::encode(b))),
        DynSolValue::String(s) => Value::String(s.clone()),
        DynSolValue::Array(items) | DynSolValue::FixedArray(items) | DynSolValue::Tuple(items) => {
            Value::Array(items.iter().map(dyn_to_json).collect())
        }
        DynSolValue::Function(f) => Value::String(format!("{f:?}")),
    }
}

#[allow(dead_code)]
fn addr_topic(a: Address) -> B256 {
    a.into_word()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const TRANSFER_TOPIC0: &str =
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
    const APPROVAL_TOPIC0: &str =
        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";

    fn erc20_abi() -> Value {
        json!([
            {"anonymous":false,"type":"event","name":"Transfer","inputs":[
                {"indexed":true,"name":"from","type":"address"},
                {"indexed":true,"name":"to","type":"address"},
                {"indexed":false,"name":"value","type":"uint256"}]},
            {"anonymous":false,"type":"event","name":"Approval","inputs":[
                {"indexed":true,"name":"owner","type":"address"},
                {"indexed":true,"name":"spender","type":"address"},
                {"indexed":false,"name":"value","type":"uint256"}]}
        ])
    }

    fn addr_word(hex40: &str) -> String {
        format!("0x000000000000000000000000{hex40}")
    }

    fn u256_data(val: u128) -> String {
        format!("0x{:064x}", val)
    }

    #[test]
    fn decodes_transfer_with_decimal_value() {
        let cfg = json!({ "abis": [{ "abi": erc20_abi(), "events": ["Transfer"] }], "on_undecodable": "drop" });
        let dec = Decoder::from_config(&cfg).unwrap();

        let rec = json!({
            "topic0": TRANSFER_TOPIC0,
            "topic1": addr_word("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            "topic2": addr_word("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            "data": u256_data(1_250_000_000),
            "block_number": 19000042,
            "log_index": 12,
            "transaction_hash": "0xdead",
            "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "block_timestamp": 1719900000u64
        });

        let out = dec.decode_record(1, &rec).unwrap().unwrap();
        assert_eq!(out["event"], "Transfer");
        assert_eq!(out["signature"], "Transfer(address,address,uint256)");
        assert_eq!(out["chain_id"], 1);
        assert_eq!(out["block_number"], 19000042);
        assert_eq!(out["params"]["value"], "1250000000");
        assert_eq!(
            out["params"]["from"].as_str().unwrap().to_lowercase(),
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            out["params"]["to"].as_str().unwrap().to_lowercase(),
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        assert_eq!(out["params"]["value"].is_string(), true);
    }

    #[test]
    fn filters_out_unselected_event() {
        // Only Transfer selected; an Approval log must hit on_undecodable=drop.
        let cfg = json!({ "abis": [{ "abi": erc20_abi(), "events": ["Transfer"] }], "on_undecodable": "drop" });
        let dec = Decoder::from_config(&cfg).unwrap();
        let rec = json!({
            "topic0": APPROVAL_TOPIC0,
            "topic1": addr_word("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            "topic2": addr_word("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            "data": u256_data(5)
        });
        assert!(dec.decode_record(1, &rec).unwrap().is_none());
    }

    #[test]
    fn on_undecodable_error_raises() {
        let cfg = json!({ "abis": [{ "abi": erc20_abi(), "events": ["Transfer"] }], "on_undecodable": "error" });
        let dec = Decoder::from_config(&cfg).unwrap();
        let rec = json!({ "topic0": "0x0000000000000000000000000000000000000000000000000000000000000000" });
        assert!(dec.decode_record(1, &rec).is_err());
    }

    #[test]
    fn passthrough_keeps_original_record() {
        let cfg = json!({ "abis": [{ "abi": erc20_abi(), "events": ["Transfer"] }], "on_undecodable": "passthrough" });
        let dec = Decoder::from_config(&cfg).unwrap();
        let rec = json!({ "topic0": APPROVAL_TOPIC0, "raw": true }); // unknown to the filter set
        let out = dec.decode_record(1, &rec).unwrap().unwrap();
        assert_eq!(out, rec, "passthrough must return the record unchanged");
    }

    #[test]
    fn unnamed_params_get_positional_names() {
        // Same Transfer shape but with empty input names.
        let abi = json!([
            {"anonymous":false,"type":"event","name":"Transfer","inputs":[
                {"indexed":true,"name":"","type":"address"},
                {"indexed":true,"name":"","type":"address"},
                {"indexed":false,"name":"","type":"uint256"}]}
        ]);
        let cfg = json!({ "abis": [{ "abi": abi }] });
        let dec = Decoder::from_config(&cfg).unwrap();
        let rec = json!({
            "topic0": TRANSFER_TOPIC0,
            "topic1": addr_word("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            "topic2": addr_word("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            "data": u256_data(7)
        });
        let out = dec.decode_record(1, &rec).unwrap().unwrap();
        let params = out["params"].as_object().unwrap();
        assert!(params.contains_key("arg0"), "keys: {:?}", params.keys().collect::<Vec<_>>());
        assert!(params.contains_key("arg2"));
        assert_eq!(params["arg2"], "7");
    }

    #[test]
    fn both_events_when_unfiltered() {
        let cfg = json!({ "abis": [{ "abi": erc20_abi() }], "on_undecodable": "drop" });
        let dec = Decoder::from_config(&cfg).unwrap();
        let rec = json!({
            "topic0": APPROVAL_TOPIC0,
            "topic1": addr_word("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            "topic2": addr_word("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            "data": u256_data(5)
        });
        let out = dec.decode_record(1, &rec).unwrap().unwrap();
        assert_eq!(out["event"], "Approval");
    }
}
