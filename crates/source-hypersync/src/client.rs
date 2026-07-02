//! HyperSync HTTP `/query` client: request building + tolerant response parse.

use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use hp_engine::config::Query;
use serde::Serialize;
use serde_json::Value;

/// A ready HyperSync client for one chain endpoint.
pub struct HyperSyncClient {
    http: reqwest::Client,
    query_url: String,
    height_url: String,
    token: Option<String>,
    logs: Vec<LogSelWire>,
    field_selection: FieldSelWire,
}

#[derive(Serialize, Clone)]
struct LogSelWire {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    address: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    topics: Vec<Vec<String>>,
}

#[derive(Serialize, Clone)]
struct FieldSelWire {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    log: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    block: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    transaction: Vec<String>,
}

#[derive(Serialize)]
struct QueryReq {
    from_block: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    to_block: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    logs: Vec<LogSelWire>,
    field_selection: FieldSelWire,
}

/// Parsed, denormalized response: log records with block metadata joined in.
pub struct QueryResponse {
    pub archive_height: Option<u64>,
    pub next_block: u64,
    pub records: Vec<Value>,
}

impl HyperSyncClient {
    pub fn new(base_url: String, token: Option<String>, query: &Query) -> Self {
        let base = base_url.trim_end_matches('/').to_string();
        let logs = query
            .logs
            .iter()
            .map(|l| LogSelWire {
                address: l.address.clone(),
                topics: l.topics.clone(),
            })
            .collect();
        let field_selection = FieldSelWire {
            log: query.field_selection.log.clone(),
            block: query.field_selection.block.clone(),
            transaction: query.field_selection.transaction.clone(),
        };
        HyperSyncClient {
            http: reqwest::Client::builder()
                .user_agent("hyperpipe/0.1")
                .build()
                .expect("http client"),
            query_url: format!("{base}/query"),
            height_url: format!("{base}/height"),
            token,
            logs,
            field_selection,
        }
    }

    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }

    /// Current confirmed chain height known to HyperSync.
    pub async fn archive_height(&self) -> Result<u64> {
        let resp = self
            .auth(self.http.get(&self.height_url))
            .send()
            .await
            .context("height request")?;
        let v: Value = resp.json().await.context("height json")?;
        v.get("height")
            .and_then(|h| h.as_u64())
            .ok_or_else(|| anyhow!("height response missing `height`"))
    }

    /// Run one `/query` from `from_block` (optionally bounded by `to_block`).
    pub async fn query(&self, from_block: u64, to_block: Option<u64>) -> Result<QueryResponse> {
        let req = QueryReq {
            from_block,
            to_block,
            logs: self.logs.clone(),
            field_selection: self.field_selection.clone(),
        };
        let resp = self
            .auth(self.http.post(&self.query_url))
            .json(&req)
            .send()
            .await
            .context("query request")?;
        let status = resp.status();
        let body: Value = resp.json().await.context("query json")?;
        if !status.is_success() {
            let msg = body
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown error");
            return Err(anyhow!("hypersync {status}: {msg}"));
        }
        parse_response(&body, from_block)
    }
}

/// Parse a `/query` response body into denormalized log records. Tolerant of
/// `data` being either an object `{logs,blocks,...}` or an array of such objects.
fn parse_response(body: &Value, from_block: u64) -> Result<QueryResponse> {
    let archive_height = body.get("archive_height").and_then(|v| v.as_u64());
    let next_block = body
        .get("next_block")
        .and_then(|v| v.as_u64())
        .unwrap_or(from_block);

    let data = body.get("data").unwrap_or(&Value::Null);
    let mut logs: Vec<Value> = Vec::new();
    let mut blocks: HashMap<u64, (Option<u64>, Option<String>)> = HashMap::new();

    let mut ingest = |obj: &Value| {
        if let Some(bs) = obj.get("blocks").and_then(|v| v.as_array()) {
            for b in bs {
                if let Some(num) = flex_u64(b.get("number")) {
                    let ts = flex_u64(b.get("timestamp"));
                    let hash = b.get("hash").and_then(|h| h.as_str()).map(String::from);
                    blocks.insert(num, (ts, hash));
                }
            }
        }
        if let Some(ls) = obj.get("logs").and_then(|v| v.as_array()) {
            logs.extend(ls.iter().cloned());
        }
    };

    match data {
        Value::Array(arr) => arr.iter().for_each(|o| ingest(o)),
        Value::Object(_) => ingest(data),
        _ => {}
    }

    // Denormalize block metadata into each log (§5.1 block metadata join).
    for log in &mut logs {
        if let Some(bn) = flex_u64(log.get("block_number")) {
            if let Some((ts, hash)) = blocks.get(&bn) {
                if let Some(obj) = log.as_object_mut() {
                    if let Some(ts) = ts {
                        obj.entry("block_timestamp").or_insert(Value::from(*ts));
                    }
                    if let Some(hash) = hash {
                        obj.entry("block_hash").or_insert(Value::from(hash.clone()));
                    }
                }
            }
        }
    }

    Ok(QueryResponse {
        archive_height,
        next_block,
        records: logs,
    })
}

/// Accept a numeric field as JSON number, decimal string, or 0x-hex string.
fn flex_u64(v: Option<&Value>) -> Option<u64> {
    match v? {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_data_object_and_denormalizes_block() {
        let body = json!({
            "archive_height": 19000100,
            "next_block": 19000006,
            "data": {
                "blocks": [{"number": 19000005, "timestamp": "0x655a1234", "hash": "0xblockhash"}],
                "logs": [{"address":"0xusdc","topic0":"0xddf2","block_number":19000005,"log_index":3}]
            }
        });
        let r = parse_response(&body, 19000000).unwrap();
        assert_eq!(r.next_block, 19000006);
        assert_eq!(r.archive_height, Some(19000100));
        assert_eq!(r.records.len(), 1);
        let log = &r.records[0];
        assert_eq!(log["block_hash"], "0xblockhash");
        assert_eq!(log["block_timestamp"], 0x655a1234u64);
    }

    #[test]
    fn parses_data_array_shape() {
        // Some HyperSync responses page `data` as an array of row-batches.
        let body = json!({
            "next_block": 100,
            "data": [
                { "blocks": [{"number": 10, "timestamp": 111, "hash": "0xa"}],
                  "logs": [{"block_number": 10, "log_index": 0}] },
                { "blocks": [], "logs": [{"block_number": 10, "log_index": 1}] }
            ]
        });
        let r = parse_response(&body, 0).unwrap();
        assert_eq!(r.records.len(), 2);
        assert_eq!(r.records[0]["block_timestamp"], 111);
        // second log shares block 10 metadata via the map
        assert_eq!(r.records[1]["block_hash"], "0xa");
    }

    #[test]
    fn missing_next_block_defaults_to_from() {
        let body = json!({ "data": { "logs": [] } });
        let r = parse_response(&body, 555).unwrap();
        assert_eq!(r.next_block, 555);
        assert!(r.records.is_empty());
    }
}
