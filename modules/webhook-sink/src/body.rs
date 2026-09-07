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

/// `scheme://host[:port]` of a URL, for log and error messages. Webhook URLs
/// routinely carry the credential in the path (Slack, Discord, Teams), and
/// everything a sink returns as an error is logged by the engine on every
/// retry, so the path and query must never appear in one.
pub fn redact_url(url: &str) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some(x) => x,
        None => return "<invalid url>".to_string(),
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = authority.rsplit('@').next().unwrap_or("");
    if host.is_empty() {
        return "<invalid url>".to_string();
    }
    format!("{scheme}://{host}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redact_url_keeps_only_scheme_and_host() {
        assert_eq!(
            redact_url("https://hooks.slack.com/services/T000/B000/secretsecret"),
            "https://hooks.slack.com"
        );
        assert_eq!(redact_url("http://127.0.0.1:8080/hook?token=abc#x"), "http://127.0.0.1:8080");
        assert_eq!(redact_url("https://user:pw@example.com/x"), "https://example.com");
        assert_eq!(redact_url("https://example.com"), "https://example.com");
        assert_eq!(redact_url("garbage"), "<invalid url>");
        assert_eq!(redact_url("https://"), "<invalid url>");
    }

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
