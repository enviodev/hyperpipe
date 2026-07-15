//! Pure predicate-filter logic (§7). Config form:
//!
//! ```yaml
//! config:
//!   all:                          # every predicate must hold (AND)
//!     - { field: params.value, op: gte, value: "1000000000000" }
//!   any:                          # optional: at least one must hold (OR)
//!     - { field: event, op: eq, value: Transfer }
//! ```
//!
//! Numeric comparisons are decimal-string aware, so uint256 values that arrive
//! as strings (§3.1) compare correctly without floating-point loss.

use serde_json::Value;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
}

impl Op {
    fn parse(s: &str) -> Result<Op, String> {
        Ok(match s {
            "eq" => Op::Eq,
            "ne" => Op::Ne,
            "gt" => Op::Gt,
            "gte" => Op::Gte,
            "lt" => Op::Lt,
            "lte" => Op::Lte,
            "in" => Op::In,
            other => return Err(format!("filter: unknown op `{other}`")),
        })
    }
}

struct Predicate {
    field: String,
    op: Op,
    value: Value,
}

pub struct Filter {
    all: Vec<Predicate>,
    any: Vec<Predicate>,
}

impl Filter {
    pub fn from_config(config: &Value) -> Result<Self, String> {
        let all = parse_predicates(config.get("all"))?;
        let any = parse_predicates(config.get("any"))?;
        if all.is_empty() && any.is_empty() {
            return Err("filter: config must have `all` and/or `any` predicates".into());
        }
        Ok(Filter { all, any })
    }

    /// True if the record passes the filter.
    pub fn passes(&self, rec: &Value) -> bool {
        let all_ok = self.all.iter().all(|p| p.eval(rec));
        let any_ok = self.any.is_empty() || self.any.iter().any(|p| p.eval(rec));
        all_ok && any_ok
    }
}

fn parse_predicates(v: Option<&Value>) -> Result<Vec<Predicate>, String> {
    let Some(arr) = v else { return Ok(vec![]) };
    let arr = arr.as_array().ok_or("filter: `all`/`any` must be arrays")?;
    let mut out = Vec::new();
    for p in arr {
        let field = p
            .get("field")
            .and_then(|f| f.as_str())
            .ok_or("filter: predicate missing `field`")?
            .to_string();
        let op = Op::parse(p.get("op").and_then(|o| o.as_str()).ok_or("filter: predicate missing `op`")?)?;
        let value = p.get("value").cloned().unwrap_or(Value::Null);
        out.push(Predicate { field, op, value });
    }
    Ok(out)
}

impl Predicate {
    fn eval(&self, rec: &Value) -> bool {
        let lhs = resolve_path(rec, &self.field);
        match self.op {
            Op::Eq => values_eq(lhs, Some(&self.value)),
            Op::Ne => !values_eq(lhs, Some(&self.value)),
            Op::In => match &self.value {
                Value::Array(items) => items.iter().any(|it| values_eq(lhs, Some(it))),
                _ => false,
            },
            Op::Gt | Op::Gte | Op::Lt | Op::Lte => match (lhs, num_str(&self.value)) {
                (Some(l), Some(r)) => match (num_str(l), Some(r)) {
                    (Some(ls), Some(rs)) => {
                        let c = cmp_decimal(&ls, &rs);
                        match self.op {
                            Op::Gt => c == std::cmp::Ordering::Greater,
                            Op::Gte => c != std::cmp::Ordering::Less,
                            Op::Lt => c == std::cmp::Ordering::Less,
                            Op::Lte => c != std::cmp::Ordering::Greater,
                            _ => unreachable!(),
                        }
                    }
                    _ => false,
                },
                _ => false,
            },
        }
    }
}

/// Resolve a dotted path like `params.value` against a JSON object.
fn resolve_path<'a>(rec: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = rec;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

fn values_eq(lhs: Option<&Value>, rhs: Option<&Value>) -> bool {
    match (lhs, rhs) {
        (Some(a), Some(b)) => {
            // Compare numbers and numeric strings by value.
            if let (Some(x), Some(y)) = (num_str(a), num_str(b)) {
                cmp_decimal(&x, &y) == std::cmp::Ordering::Equal
            } else {
                a == b
            }
        }
        _ => false,
    }
}

/// Render a JSON scalar as a decimal-integer string if it is one, else None.
fn num_str(v: &Value) -> Option<String> {
    match v {
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => {
            let t = s.strip_prefix('-').unwrap_or(s);
            if !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()) {
                Some(s.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Compare two decimal-integer strings by numeric value, sign-aware — int256
/// values (e.g. Chainlink `AnswerUpdated.current`) decode as possibly-negative
/// decimal strings.
fn cmp_decimal(a: &str, b: &str) -> std::cmp::Ordering {
    let (a_neg, a_mag) = split_sign(a);
    let (b_neg, b_mag) = split_sign(b);
    match (a_neg, b_neg) {
        (false, true) => std::cmp::Ordering::Greater,
        (true, false) => std::cmp::Ordering::Less,
        (false, false) => cmp_magnitude(a_mag, b_mag),
        (true, true) => cmp_magnitude(b_mag, a_mag), // both negative: larger magnitude is smaller
    }
}

/// Split sign, treating "-0" as non-negative zero.
fn split_sign(s: &str) -> (bool, &str) {
    let (neg, mag) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let trimmed = mag.trim_start_matches('0');
    if trimmed.is_empty() {
        (false, "") // zero: sign is irrelevant
    } else {
        (neg, trimmed)
    }
}

fn cmp_magnitude(a: &str, b: &str) -> std::cmp::Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn gte_on_uint256_decimal_string() {
        let cfg = json!({ "all": [{ "field": "params.value", "op": "gte", "value": "1000000000000" }] });
        let f = Filter::from_config(&cfg).unwrap();
        assert!(f.passes(&json!({ "params": { "value": "5000000000000" } })));
        assert!(!f.passes(&json!({ "params": { "value": "999999999999" } })));
        // huge value beyond u64/f64 precision still compares correctly
        assert!(f.passes(&json!({ "params": { "value": "123456789012345678901234567890" } })));
    }

    #[test]
    fn eq_and_in() {
        let cfg = json!({ "all": [{ "field": "event", "op": "in", "value": ["Transfer", "Approval"] }] });
        let f = Filter::from_config(&cfg).unwrap();
        assert!(f.passes(&json!({ "event": "Transfer" })));
        assert!(!f.passes(&json!({ "event": "Mint" })));
    }

    #[test]
    fn any_combines_with_all() {
        let cfg = json!({
            "all": [{ "field": "chain_id", "op": "eq", "value": 1 }],
            "any": [
                { "field": "event", "op": "eq", "value": "Transfer" },
                { "field": "event", "op": "eq", "value": "Approval" }
            ]
        });
        let f = Filter::from_config(&cfg).unwrap();
        assert!(f.passes(&json!({ "chain_id": 1, "event": "Transfer" })));
        assert!(!f.passes(&json!({ "chain_id": 1, "event": "Mint" })));
        assert!(!f.passes(&json!({ "chain_id": 8453, "event": "Transfer" })));
    }

    #[test]
    fn missing_field_fails_closed() {
        let cfg = json!({ "all": [{ "field": "params.value", "op": "gte", "value": "10" }] });
        let f = Filter::from_config(&cfg).unwrap();
        assert!(!f.passes(&json!({ "params": {} })));
    }

    #[test]
    fn negative_decimals_compare_by_value() {
        // int256 answers (e.g. Chainlink AnswerUpdated) can go negative.
        let cfg = json!({ "all": [{ "field": "answer", "op": "gt", "value": "-100" }] });
        let f = Filter::from_config(&cfg).unwrap();
        assert!(f.passes(&json!({ "answer": "-50" })));   // -50 > -100
        assert!(f.passes(&json!({ "answer": "0" })));
        assert!(f.passes(&json!({ "answer": "7" })));
        assert!(!f.passes(&json!({ "answer": "-100" })));  // not strictly greater
        assert!(!f.passes(&json!({ "answer": "-200" })));  // larger magnitude, smaller value
    }

    #[test]
    fn negative_zero_equals_zero() {
        let cfg = json!({ "all": [{ "field": "v", "op": "eq", "value": "0" }] });
        let f = Filter::from_config(&cfg).unwrap();
        assert!(f.passes(&json!({ "v": "-0" })));
    }

    #[test]
    fn leading_zeros_do_not_confuse_compare() {
        let cfg = json!({ "all": [{ "field": "v", "op": "lt", "value": "010" }] });
        let f = Filter::from_config(&cfg).unwrap();
        assert!(f.passes(&json!({ "v": "0009" })));
        assert!(!f.passes(&json!({ "v": "10" })));
    }

    #[test]
    fn config_errors() {
        // unknown op
        let e = Filter::from_config(&json!({ "all": [{ "field": "a", "op": "matches", "value": 1 }] }))
            .err()
            .unwrap();
        assert!(e.contains("unknown op `matches`"), "got {e}");

        // predicate missing field / op
        let e = Filter::from_config(&json!({ "all": [{ "op": "eq", "value": 1 }] })).err().unwrap();
        assert!(e.contains("missing `field`"), "got {e}");
        let e = Filter::from_config(&json!({ "all": [{ "field": "a", "value": 1 }] })).err().unwrap();
        assert!(e.contains("missing `op`"), "got {e}");

        // all/any must be arrays
        let e = Filter::from_config(&json!({ "all": { "field": "a" } })).err().unwrap();
        assert!(e.contains("must be arrays"), "got {e}");
        assert!(Filter::from_config(&json!({ "any": "nope" })).is_err());

        // no predicates at all — an accept-everything filter is a config mistake
        let e = Filter::from_config(&json!({})).err().unwrap();
        assert!(e.contains("must have `all` and/or `any`"), "got {e}");
        assert!(Filter::from_config(&json!({ "all": [], "any": [] })).is_err());
    }

    #[test]
    fn any_alone_is_a_valid_config() {
        let f = Filter::from_config(&json!({ "any": [{ "field": "e", "op": "eq", "value": "T" }] }))
            .unwrap();
        assert!(f.passes(&json!({ "e": "T" })));
        assert!(!f.passes(&json!({ "e": "X" })));
    }

    #[test]
    fn ne_is_the_inverse_of_eq() {
        let f = Filter::from_config(&json!({ "all": [{ "field": "e", "op": "ne", "value": "T" }] }))
            .unwrap();
        assert!(!f.passes(&json!({ "e": "T" })));
        assert!(f.passes(&json!({ "e": "X" })));
        // A missing field is not equal to anything, so `ne` passes it.
        assert!(f.passes(&json!({})));
    }

    #[test]
    fn every_comparison_op() {
        let mk = |op: &str, v: &str| {
            Filter::from_config(&json!({ "all": [{ "field": "v", "op": op, "value": v }] })).unwrap()
        };
        // lt
        assert!(mk("lt", "10").passes(&json!({ "v": "9" })));
        assert!(!mk("lt", "10").passes(&json!({ "v": "10" })));
        // lte
        assert!(mk("lte", "10").passes(&json!({ "v": "10" })));
        assert!(!mk("lte", "10").passes(&json!({ "v": "11" })));
        // gt
        assert!(mk("gt", "10").passes(&json!({ "v": "11" })));
        assert!(!mk("gt", "10").passes(&json!({ "v": "10" })));
        // gte
        assert!(mk("gte", "10").passes(&json!({ "v": "10" })));
        assert!(!mk("gte", "10").passes(&json!({ "v": "9" })));
    }

    #[test]
    fn in_needs_an_array_and_coerces_numeric_strings() {
        // non-array `value` -> nothing can be "in" it
        let f = Filter::from_config(&json!({ "all": [{ "field": "e", "op": "in", "value": "T" }] }))
            .unwrap();
        assert!(!f.passes(&json!({ "e": "T" })));

        // "5" (string) and 5 (number) are the same value
        let f = Filter::from_config(&json!({ "all": [{ "field": "v", "op": "in", "value": [5, 7] }] }))
            .unwrap();
        assert!(f.passes(&json!({ "v": "5" })));
        assert!(f.passes(&json!({ "v": 7 })));
        assert!(!f.passes(&json!({ "v": "6" })));
    }

    #[test]
    fn comparisons_fail_closed_on_non_numeric_operands() {
        // non-numeric lhs
        let f = Filter::from_config(&json!({ "all": [{ "field": "v", "op": "gt", "value": "10" }] }))
            .unwrap();
        assert!(!f.passes(&json!({ "v": "abc" })));
        assert!(!f.passes(&json!({ "v": true })));
        assert!(!f.passes(&json!({ "v": null })));
        assert!(!f.passes(&json!({})), "missing field must not pass");

        // non-numeric rhs (config asks to compare against a word)
        let f = Filter::from_config(&json!({ "all": [{ "field": "v", "op": "gt", "value": "abc" }] }))
            .unwrap();
        assert!(!f.passes(&json!({ "v": "10" })));
    }

    #[test]
    fn equality_on_non_numeric_values() {
        // plain string equality
        let f = Filter::from_config(&json!({ "all": [{ "field": "e", "op": "eq", "value": "Transfer" }] }))
            .unwrap();
        assert!(f.passes(&json!({ "e": "Transfer" })));
        assert!(!f.passes(&json!({ "e": "transfer" })), "eq is case-sensitive");

        // bools compare structurally
        let f = Filter::from_config(&json!({ "all": [{ "field": "b", "op": "eq", "value": true }] }))
            .unwrap();
        assert!(f.passes(&json!({ "b": true })));
        assert!(!f.passes(&json!({ "b": false })));

        // a predicate with no `value` compares against null
        let f = Filter::from_config(&json!({ "all": [{ "field": "v", "op": "eq" }] })).unwrap();
        assert!(f.passes(&json!({ "v": null })));
        assert!(!f.passes(&json!({ "v": 1 })));
        assert!(!f.passes(&json!({})), "absent != present-and-null");
    }

    #[test]
    fn num_str_classifies_scalars() {
        assert_eq!(num_str(&json!(42)), Some("42".to_string()));
        assert_eq!(num_str(&json!(-42)), Some("-42".to_string()));
        assert_eq!(num_str(&json!("42")), Some("42".to_string()));
        assert_eq!(num_str(&json!("-42")), Some("-42".to_string()));
        assert_eq!(num_str(&json!("")), None);
        assert_eq!(num_str(&json!("-")), None);
        assert_eq!(num_str(&json!("4.2")), None);
        assert_eq!(num_str(&json!(true)), None);
        assert_eq!(num_str(&json!(null)), None);
        assert_eq!(num_str(&json!([1])), None);
    }
}
