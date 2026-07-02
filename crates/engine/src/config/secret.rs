//! `${secret:NAME}` resolution over a parsed JSON document (§9 MVP).
//!
//! Secrets resolve from env var `HYPERPIPE_SECRET_<NAME>`. We walk every string
//! leaf of the document so refs work uniformly in `connections.*.dsn`, sink
//! `config.url`, etc. Missing refs are collected and reported all at once.

use std::collections::BTreeSet;

/// A resolver over env vars. Split out so tests can inject a fake source.
pub trait SecretSource {
    fn get(&self, name: &str) -> Option<String>;
}

pub struct EnvSecrets;

impl SecretSource for EnvSecrets {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(format!("HYPERPIPE_SECRET_{name}")).ok()
    }
}

/// Resolve all `${secret:NAME}` occurrences in-place. Returns the sorted set of
/// names that could not be resolved (empty = success).
pub fn resolve_in_place(
    value: &mut serde_json::Value,
    src: &dyn SecretSource,
) -> BTreeSet<String> {
    let mut missing = BTreeSet::new();
    walk(value, src, &mut missing);
    missing
}

fn walk(value: &mut serde_json::Value, src: &dyn SecretSource, missing: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::String(s) => {
            if let Some(replaced) = replace_refs(s, src, missing) {
                *s = replaced;
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                walk(v, src, missing);
            }
        }
        serde_json::Value::Object(map) => {
            for (_k, v) in map.iter_mut() {
                walk(v, src, missing);
            }
        }
        _ => {}
    }
}

/// Replace every `${secret:NAME}` in `s`. Returns None if there were no refs.
fn replace_refs(s: &str, src: &dyn SecretSource, missing: &mut BTreeSet<String>) -> Option<String> {
    const OPEN: &str = "${secret:";
    if !s.contains(OPEN) {
        return None;
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find(OPEN) {
        out.push_str(&rest[..start]);
        let after = &rest[start + OPEN.len()..];
        match after.find('}') {
            Some(end) => {
                let name = &after[..end];
                match src.get(name) {
                    Some(val) => out.push_str(&val),
                    None => {
                        missing.insert(name.to_string());
                        // leave a visible placeholder so the string stays well-formed
                        out.push_str("${secret:");
                        out.push_str(name);
                        out.push('}');
                    }
                }
                rest = &after[end + 1..];
            }
            None => {
                // no closing brace; treat remainder as literal
                out.push_str(&rest[start..]);
                rest = "";
                break;
            }
        }
    }
    out.push_str(rest);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct Map(HashMap<String, String>);
    impl SecretSource for Map {
        fn get(&self, name: &str) -> Option<String> {
            self.0.get(name).cloned()
        }
    }

    fn src(pairs: &[(&str, &str)]) -> Map {
        Map(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect())
    }

    #[test]
    fn resolves_present_secret() {
        let mut v = serde_json::json!({ "dsn": "postgres://${secret:PG}@h/db" });
        let missing = resolve_in_place(&mut v, &src(&[("PG", "user:pw")]));
        assert!(missing.is_empty());
        assert_eq!(v["dsn"], "postgres://user:pw@h/db");
    }

    #[test]
    fn collects_all_missing() {
        let mut v = serde_json::json!({
            "a": "${secret:ONE}",
            "b": ["x", "${secret:TWO}", "${secret:ONE}"],
        });
        let missing = resolve_in_place(&mut v, &src(&[]));
        assert_eq!(missing.into_iter().collect::<Vec<_>>(), vec!["ONE", "TWO"]);
    }

    #[test]
    fn multiple_refs_one_string() {
        let mut v = serde_json::json!({ "u": "${secret:A}-${secret:B}" });
        let missing = resolve_in_place(&mut v, &src(&[("A", "1"), ("B", "2")]));
        assert!(missing.is_empty());
        assert_eq!(v["u"], "1-2");
    }
}
