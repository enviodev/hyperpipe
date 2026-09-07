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

/// Outcome of a resolution pass.
#[derive(Debug, Default)]
pub struct Resolution {
    /// Names that could not be resolved, sorted.
    pub missing: BTreeSet<String>,
    /// `(name, value)` of every secret substituted into the document, so a
    /// caller can scrub the values back out of anything it is about to print.
    pub resolved: Vec<(String, String)>,
}

impl Resolution {
    /// Replace every resolved secret value in `text` with its `${secret:NAME}`
    /// reference. Longest values first, so a value that is a prefix of another
    /// cannot leave a fragment behind.
    pub fn redact(&self, text: &str) -> String {
        let mut pairs: Vec<&(String, String)> =
            self.resolved.iter().filter(|(_, v)| !v.is_empty()).collect();
        pairs.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
        let mut out = text.to_string();
        for (name, value) in pairs {
            out = out.replace(value, &format!("${{secret:{name}}}"));
        }
        out
    }
}

/// Resolve all `${secret:NAME}` occurrences in-place. Returns the sorted set of
/// names that could not be resolved (empty = success).
pub fn resolve_in_place(
    value: &mut serde_json::Value,
    src: &dyn SecretSource,
) -> BTreeSet<String> {
    resolve_in_place_report(value, src).missing
}

/// Like [`resolve_in_place`], but also reports which values were substituted.
pub fn resolve_in_place_report(value: &mut serde_json::Value, src: &dyn SecretSource) -> Resolution {
    let mut res = Resolution::default();
    walk(value, src, &mut res);
    res
}

fn walk(value: &mut serde_json::Value, src: &dyn SecretSource, res: &mut Resolution) {
    match value {
        serde_json::Value::String(s) => {
            if let Some(replaced) = replace_refs(s, src, res) {
                *s = replaced;
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                walk(v, src, res);
            }
        }
        serde_json::Value::Object(map) => {
            for (_k, v) in map.iter_mut() {
                walk(v, src, res);
            }
        }
        _ => {}
    }
}

/// Replace every `${secret:NAME}` in `s`. Returns None if there were no refs.
fn replace_refs(s: &str, src: &dyn SecretSource, res: &mut Resolution) -> Option<String> {
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
                    Some(val) => {
                        out.push_str(&val);
                        res.resolved.push((name.to_string(), val));
                    }
                    None => {
                        res.missing.insert(name.to_string());
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
    fn redact_scrubs_resolved_values_longest_first() {
        let mut v = serde_json::json!({ "a": "${secret:SHORT}", "b": "${secret:LONG}" });
        let res = resolve_in_place_report(&mut v, &src(&[("SHORT", "hunter"), ("LONG", "hunter2-extended")]));
        assert!(res.missing.is_empty());
        let msg = "bad value hunter2-extended near hunter and hunter2-extended";
        assert_eq!(
            res.redact(msg),
            "bad value ${secret:LONG} near ${secret:SHORT} and ${secret:LONG}"
        );
        // An empty secret must not turn every position into a marker.
        let res = resolve_in_place_report(&mut serde_json::json!({ "a": "${secret:E}" }), &src(&[("E", "")]));
        assert_eq!(res.redact("unchanged"), "unchanged");
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

    #[test]
    fn strings_without_refs_are_left_alone() {
        let mut v = serde_json::json!({ "plain": "no refs", "n": 5, "b": true, "nil": null });
        let missing = resolve_in_place(&mut v, &src(&[]));
        assert!(missing.is_empty());
        assert_eq!(v["plain"], "no refs");
        assert_eq!(v["n"], 5);
        assert!(replace_refs("nothing to do", &src(&[]), &mut Resolution::default()).is_none());
    }

    #[test]
    fn an_unterminated_ref_stays_literal() {
        // `${secret:PG` (no closing brace) is not a ref — it is passed through
        // untouched rather than swallowing the rest of the string.
        let mut v = serde_json::json!({ "dsn": "postgres://${secret:PG@h/db" });
        let missing = resolve_in_place(&mut v, &src(&[("PG", "user:pw")]));
        assert!(missing.is_empty(), "an unterminated ref is not a missing secret");
        assert_eq!(v["dsn"], "postgres://${secret:PG@h/db");

        // A well-formed ref before a broken one still resolves.
        let mut v = serde_json::json!({ "u": "${secret:A}/${secret:B" });
        let missing = resolve_in_place(&mut v, &src(&[("A", "1")]));
        assert!(missing.is_empty());
        assert_eq!(v["u"], "1/${secret:B");
    }

    #[test]
    fn a_missing_secret_leaves_the_placeholder_in_place() {
        // The document must stay well-formed for the error path to render it.
        let mut v = serde_json::json!({ "dsn": "postgres://${secret:PG}@h/db" });
        let missing = resolve_in_place(&mut v, &src(&[]));
        assert_eq!(missing.into_iter().collect::<Vec<_>>(), vec!["PG"]);
        assert_eq!(v["dsn"], "postgres://${secret:PG}@h/db");
    }

    #[test]
    fn refs_resolve_at_every_depth() {
        let mut v = serde_json::json!({
            "sinks": [{ "config": { "url": "${secret:HOOK}" } }],
            "nested": { "deep": { "deeper": ["${secret:HOOK}"] } }
        });
        let missing = resolve_in_place(&mut v, &src(&[("HOOK", "https://h/x")]));
        assert!(missing.is_empty());
        assert_eq!(v["sinks"][0]["config"]["url"], "https://h/x");
        assert_eq!(v["nested"]["deep"]["deeper"][0], "https://h/x");
    }

    #[test]
    fn env_secrets_reads_the_prefixed_var() {
        // The prefix is the contract users configure against.
        std::env::set_var("HYPERPIPE_SECRET_HP_TEST_ONLY", "resolved");
        assert_eq!(EnvSecrets.get("HP_TEST_ONLY").as_deref(), Some("resolved"));
        assert_eq!(EnvSecrets.get("HP_TEST_DEFINITELY_UNSET"), None);
        std::env::remove_var("HYPERPIPE_SECRET_HP_TEST_ONLY");
    }
}
