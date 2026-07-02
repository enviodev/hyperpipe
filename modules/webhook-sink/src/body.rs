//! Pure NDJSON body builder (native-testable).

use serde_json::Value;

/// Serialize a slice of records as newline-delimited JSON.
pub fn ndjson(records: &[Value]) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    for r in records {
        let line = serde_json::to_vec(r).map_err(|e| e.to_string())?;
        body.extend_from_slice(&line);
        body.push(b'\n');
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builds_ndjson() {
        let recs = vec![json!({"a":1}), json!({"b":"x"})];
        let body = ndjson(&recs).unwrap();
        let s = String::from_utf8(body).unwrap();
        assert_eq!(s, "{\"a\":1}\n{\"b\":\"x\"}\n");
    }

    #[test]
    fn empty_is_empty() {
        assert!(ndjson(&[]).unwrap().is_empty());
    }
}
