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
        Ok(PgConfig {
            connection,
            table,
            upsert,
            unique_key,
            create_table,
            column_map,
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
}
