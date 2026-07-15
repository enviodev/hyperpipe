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
    /// Whether the user's block field_selection asked for timestamp/hash —
    /// controls which block fields get denormalized into log records. The wire
    /// selection may request more (reorg tracking force-adds number+hash), but
    /// record shape must follow the user's config only.
    join_ts: bool,
    join_hash: bool,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    include_all_blocks: Option<bool>,
    field_selection: FieldSelWire,
}

/// HyperSync's reorg-detection metadata, present when a response covers blocks
/// near the chain tip (see docs.envio.dev → HyperSync query → rollback guard).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackGuard {
    /// Last block scanned in this query.
    pub block_number: u64,
    /// Hash of the last block scanned.
    pub hash: String,
    /// First block scanned in this query.
    pub first_block_number: u64,
    /// Parent hash of the first block scanned (= hash of `first_block_number - 1`).
    pub first_parent_hash: String,
    /// Timestamp of the last block scanned.
    pub timestamp: Option<u64>,
}

/// Parsed, denormalized response: log records with block metadata joined in.
pub struct QueryResponse {
    pub archive_height: Option<u64>,
    pub next_block: u64,
    pub records: Vec<Value>,
    /// (block_number, block_hash) for every block in the response `data`.
    pub block_hashes: Vec<(u64, String)>,
    pub rollback_guard: Option<RollbackGuard>,
}

impl HyperSyncClient {
    /// `track_blocks` force-adds `number`+`hash` to the wire block selection so
    /// reorg tracking always has hashes to work with; record shape still
    /// follows the user's own field_selection.
    pub fn new(base_url: String, token: Option<String>, query: &Query, track_blocks: bool) -> Self {
        let base = base_url.trim_end_matches('/').to_string();
        let logs = query
            .logs
            .iter()
            .map(|l| LogSelWire {
                address: l.address.clone(),
                topics: l.topics.clone(),
            })
            .collect();
        let join_ts = query.field_selection.block.iter().any(|f| f == "timestamp");
        let join_hash = query.field_selection.block.iter().any(|f| f == "hash");
        let mut block_sel = query.field_selection.block.clone();
        if track_blocks {
            for required in ["number", "hash"] {
                if !block_sel.iter().any(|f| f == required) {
                    block_sel.push(required.to_string());
                }
            }
        }
        let field_selection = FieldSelWire {
            log: query.field_selection.log.clone(),
            block: block_sel,
            transaction: query.field_selection.transaction.clone(),
        };
        HyperSyncClient {
            // Timeouts keep the cursor loop's retry/backoff in charge: a hung
            // connection must surface as an Err, not wedge the source forever.
            http: reqwest::Client::builder()
                .user_agent("hyperpipe/0.1")
                .connect_timeout(std::time::Duration::from_secs(10))
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .expect("http client"),
            query_url: format!("{base}/query"),
            height_url: format!("{base}/height"),
            token,
            logs,
            field_selection,
            join_ts,
            join_hash,
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
            include_all_blocks: None,
            field_selection: self.field_selection.clone(),
        };
        let body = self.post_query(&req).await?;
        parse_response(&body, from_block, self.join_ts, self.join_hash)
    }

    /// Canonical block hashes for `[from, to)` — a headers-only query used to
    /// locate the fork point after a reorg. Pages through `next_block` until
    /// the range is covered; bails out if the server stops advancing.
    pub async fn block_hashes(&self, from: u64, to: u64) -> Result<Vec<(u64, String)>> {
        let mut out = Vec::new();
        let mut cursor = from;
        while cursor < to {
            let req = QueryReq {
                from_block: cursor,
                to_block: Some(to),
                logs: Vec::new(),
                include_all_blocks: Some(true),
                field_selection: FieldSelWire {
                    log: Vec::new(),
                    block: vec!["number".into(), "hash".into()],
                    transaction: Vec::new(),
                },
            };
            let body = self.post_query(&req).await?;
            let resp = parse_response(&body, cursor, false, false)?;
            out.extend(resp.block_hashes);
            if resp.next_block <= cursor {
                break; // server made no progress; return what we have
            }
            cursor = resp.next_block;
        }
        Ok(out)
    }

    async fn post_query(&self, req: &QueryReq) -> Result<Value> {
        let resp = self
            .auth(self.http.post(&self.query_url))
            .json(req)
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
        Ok(body)
    }
}

/// Parse a `/query` response body into denormalized log records. Tolerant of
/// `data` being either an object `{logs,blocks,...}` or an array of such objects.
/// `join_ts`/`join_hash` gate which block fields get copied into log records
/// (only the ones the user's field_selection actually asked for).
fn parse_response(body: &Value, from_block: u64, join_ts: bool, join_hash: bool) -> Result<QueryResponse> {
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
                    if let (true, Some(ts)) = (join_ts, ts) {
                        obj.entry("block_timestamp").or_insert(Value::from(*ts));
                    }
                    if let (true, Some(hash)) = (join_hash, hash) {
                        obj.entry("block_hash").or_insert(Value::from(hash.clone()));
                    }
                }
            }
        }
    }

    let block_hashes: Vec<(u64, String)> = blocks
        .iter()
        .filter_map(|(num, (_, hash))| hash.clone().map(|h| (*num, h)))
        .collect();

    Ok(QueryResponse {
        archive_height,
        next_block,
        records: logs,
        block_hashes,
        rollback_guard: parse_rollback_guard(body.get("rollback_guard")),
    })
}

/// Parse the optional `rollback_guard` object. Tolerant of numeric fields
/// arriving as numbers, decimal strings, or 0x-hex; returns None if the guard
/// is absent or missing any required field.
fn parse_rollback_guard(v: Option<&Value>) -> Option<RollbackGuard> {
    let g = v?.as_object()?;
    Some(RollbackGuard {
        block_number: flex_u64(g.get("block_number"))?,
        hash: g.get("hash")?.as_str()?.to_string(),
        first_block_number: flex_u64(g.get("first_block_number"))?,
        first_parent_hash: g.get("first_parent_hash")?.as_str()?.to_string(),
        timestamp: flex_u64(g.get("timestamp")),
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
        let r = parse_response(&body, 19000000, true, true).unwrap();
        assert_eq!(r.next_block, 19000006);
        assert_eq!(r.archive_height, Some(19000100));
        assert_eq!(r.records.len(), 1);
        let log = &r.records[0];
        assert_eq!(log["block_hash"], "0xblockhash");
        assert_eq!(log["block_timestamp"], 0x655a1234u64);
        assert_eq!(r.block_hashes, vec![(19000005, "0xblockhash".to_string())]);
    }

    #[test]
    fn join_flags_gate_denormalized_fields() {
        // Reorg tracking force-selects block hash on the wire, but the user did
        // not ask for it: records must NOT grow a block_hash field.
        let body = json!({
            "next_block": 100,
            "data": {
                "blocks": [{"number": 10, "timestamp": 111, "hash": "0xa"}],
                "logs": [{"block_number": 10, "log_index": 0}]
            }
        });
        let r = parse_response(&body, 0, true, false).unwrap();
        assert_eq!(r.records[0]["block_timestamp"], 111);
        assert!(r.records[0].get("block_hash").is_none());
        // hashes are still available for the reorg tracker
        assert_eq!(r.block_hashes, vec![(10, "0xa".to_string())]);
    }

    #[test]
    fn parses_rollback_guard() {
        let body = json!({
            "next_block": 105,
            "data": { "logs": [] },
            "rollback_guard": {
                "block_number": 104,
                "timestamp": "0x655a1234",
                "hash": "0xtip",
                "first_block_number": 100,
                "first_parent_hash": "0xparent"
            }
        });
        let g = parse_response(&body, 100, true, true).unwrap().rollback_guard.unwrap();
        assert_eq!(g.block_number, 104);
        assert_eq!(g.hash, "0xtip");
        assert_eq!(g.first_block_number, 100);
        assert_eq!(g.first_parent_hash, "0xparent");
        assert_eq!(g.timestamp, Some(0x655a1234));
    }

    #[test]
    fn absent_or_malformed_rollback_guard_is_none() {
        let none = parse_response(&json!({"data": {"logs": []}}), 0, true, true).unwrap();
        assert!(none.rollback_guard.is_none());
        // missing required field -> None, not a parse error
        let partial = json!({"data": {"logs": []}, "rollback_guard": {"block_number": 5}});
        assert!(parse_response(&partial, 0, true, true).unwrap().rollback_guard.is_none());
        let not_obj = json!({"data": {"logs": []}, "rollback_guard": null});
        assert!(parse_response(&not_obj, 0, true, true).unwrap().rollback_guard.is_none());
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
        let r = parse_response(&body, 0, true, true).unwrap();
        assert_eq!(r.records.len(), 2);
        assert_eq!(r.records[0]["block_timestamp"], 111);
        // second log shares block 10 metadata via the map
        assert_eq!(r.records[1]["block_hash"], "0xa");
    }

    #[test]
    fn missing_next_block_defaults_to_from() {
        let body = json!({ "data": { "logs": [] } });
        let r = parse_response(&body, 555, true, true).unwrap();
        assert_eq!(r.next_block, 555);
        assert!(r.records.is_empty());
    }

    #[test]
    fn logs_whose_block_is_absent_from_the_map_are_left_alone() {
        // A log for a block the response didn't include metadata for: no join,
        // no panic, the record still ships.
        let body = json!({
            "next_block": 100,
            "data": {
                "blocks": [{"number": 10, "timestamp": 111, "hash": "0xa"}],
                "logs": [{"block_number": 99, "log_index": 0}]
            }
        });
        let r = parse_response(&body, 0, true, true).unwrap();
        assert_eq!(r.records.len(), 1);
        assert!(r.records[0].get("block_timestamp").is_none());
        assert!(r.records[0].get("block_hash").is_none());
    }

    #[test]
    fn a_join_never_overwrites_a_field_the_log_already_has() {
        let body = json!({
            "next_block": 100,
            "data": {
                "blocks": [{"number": 10, "timestamp": 111, "hash": "0xfrom-block"}],
                "logs": [{"block_number": 10, "block_hash": "0xfrom-log"}]
            }
        });
        let r = parse_response(&body, 0, true, true).unwrap();
        assert_eq!(r.records[0]["block_hash"], "0xfrom-log");
    }

    #[test]
    fn odd_data_shapes_yield_no_records() {
        for data in [json!(null), json!(42), json!("text"), json!(true)] {
            let body = json!({ "next_block": 10, "data": data });
            let r = parse_response(&body, 0, true, true).unwrap();
            assert!(r.records.is_empty(), "data={data} should parse to no records");
            assert!(r.block_hashes.is_empty());
        }
        // `data` missing entirely
        let r = parse_response(&json!({ "next_block": 10 }), 0, true, true).unwrap();
        assert!(r.records.is_empty());
    }

    #[test]
    fn blocks_without_a_hash_are_not_tracked() {
        let body = json!({
            "next_block": 100,
            "data": { "blocks": [{"number": 10, "timestamp": 1}, {"number": 11, "hash": "0xb"}],
                      "logs": [] }
        });
        let r = parse_response(&body, 0, true, true).unwrap();
        assert_eq!(r.block_hashes, vec![(11, "0xb".to_string())]);
    }

    #[test]
    fn flex_u64_accepts_only_real_numbers() {
        assert_eq!(flex_u64(Some(&json!(42))), Some(42));
        assert_eq!(flex_u64(Some(&json!("42"))), Some(42));
        assert_eq!(flex_u64(Some(&json!("0x2a"))), Some(42));
        assert_eq!(flex_u64(Some(&json!(-1))), None, "negative is not a block number");
        assert_eq!(flex_u64(Some(&json!(1.5))), None);
        assert_eq!(flex_u64(Some(&json!("0xzz"))), None);
        assert_eq!(flex_u64(Some(&json!("not a number"))), None);
        assert_eq!(flex_u64(Some(&json!(true))), None);
        assert_eq!(flex_u64(Some(&json!(null))), None);
        assert_eq!(flex_u64(None), None);
    }

    // ---- HTTP-level tests (real socket, scripted server) --------------------

    use hp_testutil::{MockServer, Reply};
    use hp_engine::config::{FieldSelection, LogSelection};

    fn query_cfg() -> Query {
        Query {
            logs: vec![LogSelection {
                address: vec!["0xusdc".into()],
                topics: vec![vec!["0xddf2".into()]],
            }],
            field_selection: FieldSelection {
                log: vec!["address".into(), "block_number".into()],
                block: vec!["number".into(), "timestamp".into()],
                transaction: vec![],
            },
        }
    }

    #[tokio::test]
    async fn archive_height_happy_path() {
        let srv = MockServer::start(|path, _b, _i| {
            assert_eq!(path, "/height");
            Reply::json(json!({ "height": 19_000_000 }))
        })
        .await;
        let c = HyperSyncClient::new(srv.url.clone(), None, &Query::default(), false);
        assert_eq!(c.archive_height().await.unwrap(), 19_000_000);
    }

    #[tokio::test]
    async fn archive_height_errors_on_a_body_without_height() {
        let srv = MockServer::start(|_p, _b, _i| Reply::json(json!({ "not_height": 1 }))).await;
        let c = HyperSyncClient::new(srv.url.clone(), None, &Query::default(), false);
        let e = c.archive_height().await.unwrap_err();
        assert!(format!("{e:#}").contains("missing `height`"), "got {e:#}");
    }

    #[tokio::test]
    async fn archive_height_errors_on_a_non_json_body() {
        let srv = MockServer::start(|_p, _b, _i| Reply::raw("<html>gateway error</html>")).await;
        let c = HyperSyncClient::new(srv.url.clone(), None, &Query::default(), false);
        let e = c.archive_height().await.unwrap_err();
        assert!(format!("{e:#}").contains("height json"), "got {e:#}");
    }

    #[tokio::test]
    async fn query_sends_the_configured_selection_faithfully() {
        let srv = MockServer::start(|_p, _b, _i| {
            Reply::json(json!({ "next_block": 200, "data": { "logs": [] } }))
        })
        .await;
        let c = HyperSyncClient::new(srv.url.clone(), None, &query_cfg(), false);
        c.query(100, Some(500)).await.unwrap();

        let sent = &srv.query_bodies()[0];
        assert_eq!(sent["from_block"], 100);
        assert_eq!(sent["to_block"], 500);
        assert_eq!(sent["logs"][0]["address"], json!(["0xusdc"]));
        assert_eq!(sent["logs"][0]["topics"], json!([["0xddf2"]]));
        assert_eq!(sent["field_selection"]["log"], json!(["address", "block_number"]));
        assert_eq!(sent["field_selection"]["block"], json!(["number", "timestamp"]));
        // omitted rather than sent as null/empty
        assert!(sent.get("include_all_blocks").is_none());
        assert!(sent["field_selection"].get("transaction").is_none());
    }

    #[tokio::test]
    async fn an_unbounded_query_omits_to_block() {
        let srv = MockServer::start(|_p, _b, _i| {
            Reply::json(json!({ "next_block": 200, "data": { "logs": [] } }))
        })
        .await;
        let c = HyperSyncClient::new(srv.url.clone(), None, &query_cfg(), false);
        c.query(100, None).await.unwrap();
        assert!(srv.query_bodies()[0].get("to_block").is_none());
    }

    #[tokio::test]
    async fn the_bearer_token_is_sent_only_when_configured() {
        let srv = MockServer::start(|_p, _b, _i| {
            Reply::json(json!({ "next_block": 1, "data": { "logs": [] } }))
        })
        .await;

        let with = HyperSyncClient::new(srv.url.clone(), Some("tok-123".into()), &Query::default(), false);
        with.query(0, None).await.unwrap();
        assert_eq!(srv.requests()[0].auth.as_deref(), Some("Bearer tok-123"));

        let without = HyperSyncClient::new(srv.url.clone(), None, &Query::default(), false);
        without.query(0, None).await.unwrap();
        assert_eq!(srv.requests()[1].auth, None);
    }

    #[tokio::test]
    async fn non_2xx_surfaces_the_servers_error_message() {
        let srv = MockServer::start(|_p, _b, _i| {
            Reply::status(429, json!({ "error": "rate limited, slow down" }))
        })
        .await;
        let c = HyperSyncClient::new(srv.url.clone(), None, &Query::default(), false);
        let e = c.query(0, None).await.err().unwrap();
        let msg = format!("{e:#}");
        assert!(msg.contains("429"), "got {msg}");
        assert!(msg.contains("rate limited, slow down"), "got {msg}");
    }

    #[tokio::test]
    async fn non_2xx_without_an_error_field_still_errors() {
        let srv = MockServer::start(|_p, _b, _i| Reply::status(500, json!({ "oops": true }))).await;
        let c = HyperSyncClient::new(srv.url.clone(), None, &Query::default(), false);
        let e = c.query(0, None).await.err().unwrap();
        assert!(format!("{e:#}").contains("unknown error"), "got {e:#}");
    }

    #[tokio::test]
    async fn block_hashes_pages_until_the_range_is_covered() {
        let srv = MockServer::start(|_p, body, _i| {
            let from = body["from_block"].as_u64().unwrap();
            // headers-only query shape
            assert_eq!(body["include_all_blocks"], true);
            assert_eq!(body["field_selection"]["block"], json!(["number", "hash"]));
            assert!(body.get("logs").is_none(), "no log selection on a hash sweep");
            match from {
                100 => Reply::json(json!({ "next_block": 102,
                    "data": { "blocks": [{"number": 100, "hash": "0xa"}, {"number": 101, "hash": "0xb"}] } })),
                102 => Reply::json(json!({ "next_block": 104,
                    "data": { "blocks": [{"number": 102, "hash": "0xc"}, {"number": 103, "hash": "0xd"}] } })),
                _ => panic!("unexpected page from {from}"),
            }
        })
        .await;
        let c = HyperSyncClient::new(srv.url.clone(), None, &Query::default(), false);
        let mut hashes = c.block_hashes(100, 104).await.unwrap();
        hashes.sort();
        assert_eq!(
            hashes,
            vec![
                (100, "0xa".to_string()),
                (101, "0xb".to_string()),
                (102, "0xc".to_string()),
                (103, "0xd".to_string())
            ]
        );
        assert_eq!(srv.call_count(), 2);
    }

    #[tokio::test]
    async fn block_hashes_gives_up_when_the_server_stops_advancing() {
        // next_block <= cursor forever would loop until the test times out;
        // the client must return what it has instead.
        let srv = MockServer::start(|_p, _b, _i| {
            Reply::json(json!({ "next_block": 100, "data": { "blocks": [{"number": 100, "hash": "0xa"}] } }))
        })
        .await;
        let c = HyperSyncClient::new(srv.url.clone(), None, &Query::default(), false);
        let hashes = c.block_hashes(100, 200).await.unwrap();
        assert_eq!(hashes, vec![(100, "0xa".to_string())]);
        assert_eq!(srv.call_count(), 1, "one page, then bail — not an infinite loop");
    }

    #[tokio::test]
    async fn block_hashes_over_an_empty_range_makes_no_request() {
        let srv = MockServer::start(|_p, _b, _i| panic!("must not be called")).await;
        let c = HyperSyncClient::new(srv.url.clone(), None, &Query::default(), false);
        assert!(c.block_hashes(100, 100).await.unwrap().is_empty());
        assert_eq!(srv.call_count(), 0);
    }

    #[tokio::test]
    async fn tracking_blocks_forces_number_and_hash_onto_the_wire_selection() {
        let srv = MockServer::start(|_p, _b, _i| {
            Reply::json(json!({ "next_block": 1, "data": { "logs": [] } }))
        })
        .await;
        // user asked for block.timestamp only; reorg tracking needs number+hash
        let mut q = query_cfg();
        q.field_selection.block = vec!["timestamp".into()];
        let c = HyperSyncClient::new(srv.url.clone(), None, &q, true);
        c.query(0, None).await.unwrap();
        assert_eq!(
            srv.query_bodies()[0]["field_selection"]["block"],
            json!(["timestamp", "number", "hash"])
        );
    }

    #[tokio::test]
    async fn tracking_blocks_does_not_duplicate_existing_entries() {
        let srv = MockServer::start(|_p, _b, _i| {
            Reply::json(json!({ "next_block": 1, "data": { "logs": [] } }))
        })
        .await;
        let mut q = query_cfg();
        q.field_selection.block = vec!["number".into(), "hash".into()];
        let c = HyperSyncClient::new(srv.url.clone(), None, &q, true);
        c.query(0, None).await.unwrap();
        assert_eq!(
            srv.query_bodies()[0]["field_selection"]["block"],
            json!(["number", "hash"])
        );
    }

    #[tokio::test]
    async fn a_forced_block_hash_does_not_leak_into_records() {
        // track_blocks selects `hash` on the wire, but the user's field_selection
        // is the contract for record shape.
        let srv = MockServer::start(|_p, _b, _i| {
            Reply::json(json!({
                "next_block": 11,
                "data": { "blocks": [{"number": 10, "timestamp": 111, "hash": "0xa"}],
                          "logs": [{"block_number": 10, "log_index": 0}] }
            }))
        })
        .await;
        let c = HyperSyncClient::new(srv.url.clone(), None, &query_cfg(), true);
        let r = c.query(10, None).await.unwrap();
        assert_eq!(r.records[0]["block_timestamp"], 111, "the user asked for timestamp");
        assert!(r.records[0].get("block_hash").is_none(), "the user did not ask for hash");
        assert_eq!(r.block_hashes, vec![(10, "0xa".to_string())], "but the tracker gets it");
    }

    #[tokio::test]
    async fn a_trailing_slash_in_the_base_url_does_not_double_up() {
        let srv = MockServer::start(|path, _b, _i| {
            assert_eq!(path, "/query", "path must not become //query");
            Reply::json(json!({ "next_block": 1, "data": { "logs": [] } }))
        })
        .await;
        let c = HyperSyncClient::new(format!("{}/", srv.url), None, &Query::default(), false);
        c.query(0, None).await.unwrap();
        assert_eq!(srv.call_count(), 1);
    }
}
