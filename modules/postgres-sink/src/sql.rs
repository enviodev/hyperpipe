//! Pure SQL generation for the postgres sink (native-testable).

use std::collections::BTreeMap;

use serde_json::Value;

pub struct PgConfig {
    pub connection: String,
    pub table: String,
    pub upsert: bool,
    pub unique_key: Vec<String>,
    pub create_table: bool,
    /// record path (dotted) -> column name.
    pub column_map: BTreeMap<String, String>,
    /// Reorg rollback: delete rows past the fork when a `rollback` control
    /// record arrives. `None` = ignore rollbacks (stale rows remain).
    pub rollback_block_column: Option<String>,
    /// Optional chain filter for the rollback delete (multi-chain tables).
    pub rollback_chain_column: Option<String>,
}

impl PgConfig {
    pub fn from_json(config: &Value) -> Result<Self, String> {
        let connection = config
            .get("connection")
            .and_then(|v| v.as_str())
            .ok_or("postgres: config.connection required")?
            .to_string();
        let table = config
            .get("table")
            .and_then(|v| v.as_str())
            .ok_or("postgres: config.table required")?
            .to_string();
        let upsert = matches!(config.get("mode").and_then(|v| v.as_str()), Some("upsert"));
        let unique_key: Vec<String> = config
            .get("unique_key")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default();
        let create_table = config.get("create_table").and_then(|v| v.as_bool()).unwrap_or(false);
        let mut column_map = BTreeMap::new();
        if let Some(m) = config.get("column_map").and_then(|v| v.as_object()) {
            for (path, col) in m {
                if let Some(c) = col.as_str() {
                    column_map.insert(path.clone(), c.to_string());
                }
            }
        }
        if upsert && unique_key.is_empty() {
            return Err("postgres: mode upsert requires unique_key".into());
        }
        // rollback: true (defaults), false/absent (off), or
        // { block_number_column, chain_id_column } (chain_id_column: null
        // disables the chain filter).
        let (rollback_block_column, rollback_chain_column) = match config.get("rollback") {
            None | Some(Value::Bool(false)) => (None, None),
            Some(Value::Bool(true)) => {
                (Some("block_number".to_string()), Some("chain_id".to_string()))
            }
            Some(Value::Object(o)) => {
                let block = match o.get("block_number_column") {
                    None => "block_number".to_string(),
                    Some(v) => v
                        .as_str()
                        .ok_or("postgres: rollback.block_number_column must be a string")?
                        .to_string(),
                };
                let chain = match o.get("chain_id_column") {
                    None => Some("chain_id".to_string()),
                    Some(Value::Null) => None,
                    Some(v) => Some(
                        v.as_str()
                            .ok_or("postgres: rollback.chain_id_column must be a string or null")?
                            .to_string(),
                    ),
                };
                (Some(block), chain)
            }
            Some(_) => {
                return Err("postgres: config.rollback must be a bool or an object".into())
            }
        };
        Ok(PgConfig {
            connection,
            table,
            upsert,
            unique_key,
            create_table,
            column_map,
            rollback_block_column,
            rollback_chain_column,
        })
    }
}

/// Resolve a record into (column, value) pairs: all top-level scalar fields,
/// plus `column_map` entries (which may pull nested paths and rename). Sorted by
/// column name for a stable column order across rows.
pub fn resolve_columns(record: &Value, column_map: &BTreeMap<String, String>) -> Vec<(String, Value)> {
    let mut cols: Vec<(String, Value)> = Vec::new();
    if let Some(obj) = record.as_object() {
        for (k, v) in obj {
            if v.is_object() || v.is_array() {
                continue; // nested values only enter via column_map
            }
            cols.push((k.clone(), v.clone()));
        }
    }
    for (path, col) in column_map {
        if let Some(v) = resolve_path(record, path) {
            if let Some(slot) = cols.iter_mut().find(|(c, _)| c == col) {
                slot.1 = v.clone();
            } else {
                cols.push((col.clone(), v.clone()));
            }
        }
    }
    cols.sort_by(|a, b| a.0.cmp(&b.0));
    cols
}

fn resolve_path<'a>(rec: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = rec;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn col_type(v: &Value) -> &'static str {
    match v {
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "bigint",
        Value::Number(_) => "double precision",
        Value::String(s) if is_decimal(s) => "numeric",
        Value::String(_) => "text",
        _ => "jsonb",
    }
}

fn is_decimal(s: &str) -> bool {
    let t = s.strip_prefix('-').unwrap_or(s);
    !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit())
}

/// Bind placeholder for column `i`, casting decimal-string values to numeric so
/// they can go into a `numeric` column (the host binds them as text).
fn placeholder(i: usize, v: &Value) -> String {
    match v {
        Value::String(s) if is_decimal(s) => format!("${i}::numeric"),
        _ => format!("${i}"),
    }
}

/// Union of columns over all rows (first-seen sample value per column, sorted
/// by name) — the DDL sample. Rows in one batch may have different key sets
/// (optional fields, mixed event types), so the table must cover all of them.
pub fn union_columns(rows: &[Vec<(String, Value)>]) -> Vec<(String, Value)> {
    let mut union: BTreeMap<String, Value> = BTreeMap::new();
    for row in rows {
        for (c, v) in row {
            union.entry(c.clone()).or_insert_with(|| v.clone());
        }
    }
    union.into_iter().collect()
}

/// Group rows by their column signature. Each group shares one prepared
/// statement; binding a row against a statement built from a row with a
/// DIFFERENT column set would silently put values in the wrong columns (or
/// fail on parameter count), so heterogeneous batches must be split.
pub fn group_by_signature(rows: Vec<Vec<(String, Value)>>) -> Vec<Vec<Vec<(String, Value)>>> {
    let mut groups: BTreeMap<Vec<String>, Vec<Vec<(String, Value)>>> = BTreeMap::new();
    for row in rows {
        let sig: Vec<String> = row.iter().map(|(c, _)| c.clone()).collect();
        groups.entry(sig).or_default().push(row);
    }
    groups.into_values().collect()
}

/// `DELETE` for a reorg rollback: void every row past the fork block.
pub fn rollback_delete_stmt(table: &str, block_col: &str, chain_col: Option<&str>) -> String {
    match chain_col {
        Some(c) => format!(
            "DELETE FROM {} WHERE {} > $1 AND {} = $2",
            quote_ident(table),
            quote_ident(block_col),
            quote_ident(c)
        ),
        None => format!(
            "DELETE FROM {} WHERE {} > $1",
            quote_ident(table),
            quote_ident(block_col)
        ),
    }
}

/// `CREATE TABLE IF NOT EXISTS` from a sample row.
pub fn ddl(table: &str, sample: &[(String, Value)], unique_key: &[String]) -> String {
    let mut parts: Vec<String> = sample
        .iter()
        .map(|(c, v)| format!("{} {}", quote_ident(c), col_type(v)))
        .collect();
    if !unique_key.is_empty() {
        let pk: Vec<String> = unique_key.iter().map(|c| quote_ident(c)).collect();
        parts.push(format!("PRIMARY KEY ({})", pk.join(", ")));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_ident(table),
        parts.join(", ")
    )
}

/// `INSERT ... VALUES ($1..)` with optional `ON CONFLICT ... DO UPDATE`.
///
/// `row` is a sample (column, value) list: its values drive per-column casts so
/// decimal-string params (uint256) land in `numeric` columns without a text/numeric
/// type error. Column order is taken from `row`.
pub fn upsert_stmt(table: &str, row: &[(String, Value)], unique_key: &[String], upsert: bool) -> String {
    let columns: Vec<String> = row.iter().map(|(c, _)| c.clone()).collect();
    let cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
    let placeholders: Vec<String> = row
        .iter()
        .enumerate()
        .map(|(i, (_, v))| placeholder(i + 1, v))
        .collect();
    let mut stmt = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_ident(table),
        cols.join(", "),
        placeholders.join(", ")
    );
    if upsert && !unique_key.is_empty() {
        let conflict: Vec<String> = unique_key.iter().map(|c| quote_ident(c)).collect();
        let updates: Vec<String> = columns
            .iter()
            .filter(|c| !unique_key.iter().any(|u| u == *c))
            .map(|c| format!("{0} = EXCLUDED.{0}", quote_ident(c)))
            .collect();
        if updates.is_empty() {
            stmt.push_str(&format!(" ON CONFLICT ({}) DO NOTHING", conflict.join(", ")));
        } else {
            stmt.push_str(&format!(
                " ON CONFLICT ({}) DO UPDATE SET {}",
                conflict.join(", "),
                updates.join(", ")
            ));
        }
    }
    stmt
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn decoded() -> Value {
        json!({
            "chain_id": 1, "block_number": 19000042, "log_index": 12,
            "transaction_hash": "0xdead", "address": "0xusdc",
            "event": "Transfer", "signature": "Transfer(address,address,uint256)",
            "params": { "from": "0xaaa", "to": "0xbbb", "value": "5000000000000" }
        })
    }

    #[test]
    fn columns_flatten_params_via_map() {
        let cm = map(&[("params.from", "from_address"), ("params.to", "to_address"), ("params.value", "amount")]);
        let cols = resolve_columns(&decoded(), &cm);
        let names: Vec<&str> = cols.iter().map(|(c, _)| c.as_str()).collect();
        // top-level scalars + the three mapped columns, sorted, no `params` object
        assert!(names.contains(&"chain_id"));
        assert!(names.contains(&"from_address"));
        assert!(names.contains(&"amount"));
        assert!(!names.contains(&"params"));
        let amount = &cols.iter().find(|(c, _)| c == "amount").unwrap().1;
        assert_eq!(amount, &json!("5000000000000"));
    }

    #[test]
    fn ddl_types_decimal_string_as_numeric() {
        let cm = map(&[("params.value", "amount")]);
        let cols = resolve_columns(&decoded(), &cm);
        let sql = ddl("usdc_transfers", &cols, &["chain_id".into(), "block_number".into(), "log_index".into()]);
        assert!(sql.contains("\"amount\" numeric"), "got {sql}");
        assert!(sql.contains("\"chain_id\" bigint"), "got {sql}");
        assert!(sql.contains("PRIMARY KEY (\"chain_id\", \"block_number\", \"log_index\")"), "got {sql}");
    }

    #[test]
    fn upsert_sets_non_key_columns_and_casts_numeric() {
        let row = vec![
            ("chain_id".to_string(), json!(1)),
            ("block_number".to_string(), json!(19000042)),
            ("log_index".to_string(), json!(12)),
            ("amount".to_string(), json!("5000000000000")),
        ];
        let uk = vec!["chain_id".to_string(), "block_number".to_string(), "log_index".to_string()];
        let sql = upsert_stmt("t", &row, &uk, true);
        assert!(sql.contains("INSERT INTO \"t\""));
        // decimal-string column gets a numeric cast; plain numbers do not
        assert!(sql.contains("VALUES ($1, $2, $3, $4::numeric)"), "got {sql}");
        assert!(sql.contains("ON CONFLICT (\"chain_id\", \"block_number\", \"log_index\") DO UPDATE SET \"amount\" = EXCLUDED.\"amount\""));
    }

    #[test]
    fn insert_mode_has_no_conflict_clause() {
        let row = vec![("a".to_string(), json!(1)), ("b".to_string(), json!("x"))];
        let sql = upsert_stmt("t", &row, &[], false);
        assert!(!sql.contains("ON CONFLICT"));
    }

    #[test]
    fn heterogeneous_rows_split_into_signature_groups() {
        let transfer = vec![
            ("amount".to_string(), json!("5")),
            ("block_number".to_string(), json!(1)),
        ];
        let approval = vec![
            ("block_number".to_string(), json!(2)),
            ("owner".to_string(), json!("0xaaa")),
        ];
        let groups = group_by_signature(vec![transfer.clone(), approval.clone(), transfer.clone()]);
        assert_eq!(groups.len(), 2);
        let sizes: Vec<usize> = groups.iter().map(|g| g.len()).collect();
        assert!(sizes.contains(&2) && sizes.contains(&1));
        // every row in a group shares the exact column list
        for g in &groups {
            let sig: Vec<&String> = g[0].iter().map(|(c, _)| c).collect();
            for row in g {
                assert_eq!(row.iter().map(|(c, _)| c).collect::<Vec<_>>(), sig);
            }
        }
    }

    #[test]
    fn ddl_covers_union_of_heterogeneous_rows() {
        let rows = vec![
            vec![("a".to_string(), json!(1)), ("b".to_string(), json!("x"))],
            vec![("a".to_string(), json!(2)), ("c".to_string(), json!("123"))],
        ];
        let sample = union_columns(&rows);
        let names: Vec<&str> = sample.iter().map(|(c, _)| c.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
        let sql = ddl("t", &sample, &[]);
        assert!(sql.contains("\"b\" text") && sql.contains("\"c\" numeric"), "got {sql}");
    }

    #[test]
    fn rollback_delete_quotes_identifiers() {
        assert_eq!(
            rollback_delete_stmt("transfers", "block_number", Some("chain_id")),
            "DELETE FROM \"transfers\" WHERE \"block_number\" > $1 AND \"chain_id\" = $2"
        );
        assert_eq!(
            rollback_delete_stmt("t", "bn", None),
            "DELETE FROM \"t\" WHERE \"bn\" > $1"
        );
        // injection attempt in an identifier stays quoted
        let evil = rollback_delete_stmt("t\"; DROP TABLE x; --", "bn", None);
        assert!(evil.starts_with("DELETE FROM \"t\"\"; DROP TABLE x; --\""), "got {evil}");
    }

    #[test]
    fn rollback_config_parses_bool_object_and_rejects_junk() {
        let base = json!({"connection": "pg", "table": "t"});
        let cfg = PgConfig::from_json(&base).unwrap();
        assert_eq!(cfg.rollback_block_column, None);

        let on = json!({"connection": "pg", "table": "t", "rollback": true});
        let cfg = PgConfig::from_json(&on).unwrap();
        assert_eq!(cfg.rollback_block_column.as_deref(), Some("block_number"));
        assert_eq!(cfg.rollback_chain_column.as_deref(), Some("chain_id"));

        let custom = json!({"connection": "pg", "table": "t",
                            "rollback": {"block_number_column": "blk", "chain_id_column": null}});
        let cfg = PgConfig::from_json(&custom).unwrap();
        assert_eq!(cfg.rollback_block_column.as_deref(), Some("blk"));
        assert_eq!(cfg.rollback_chain_column, None);

        let junk = json!({"connection": "pg", "table": "t", "rollback": 7});
        assert!(PgConfig::from_json(&junk).is_err());
    }
}
