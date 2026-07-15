//! Built-in chain registry (name -> chain_id + HyperSync url). See §5.1.

use std::collections::HashMap;

const CHAINS_TOML: &str = include_str!("chains.toml");

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ChainEntry {
    pub chain_id: u64,
    pub url: String,
}

#[derive(Debug, Clone)]
pub struct ChainRegistry {
    entries: HashMap<String, ChainEntry>,
}

impl ChainRegistry {
    /// Parse the embedded `chains.toml`. Panics only if the bundled file is
    /// malformed, which is a build-time invariant (covered by a unit test).
    pub fn builtin() -> Self {
        let entries: HashMap<String, ChainEntry> =
            toml::from_str(CHAINS_TOML).expect("bundled chains.toml is valid");
        ChainRegistry { entries }
    }

    pub fn lookup(&self, name: &str) -> Option<&ChainEntry> {
        self.entries.get(name)
    }

    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.entries.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_registry_parses() {
        let reg = ChainRegistry::builtin();
        assert_eq!(reg.lookup("ethereum").unwrap().chain_id, 1);
        assert_eq!(reg.lookup("base").unwrap().chain_id, 8453);
        assert!(reg.lookup("does-not-exist").is_none());
    }

    #[test]
    fn every_entry_has_a_usable_url() {
        let reg = ChainRegistry::builtin();
        let names: Vec<&String> = reg.names().collect();
        assert!(!names.is_empty(), "the bundled registry must not be empty");
        for name in names {
            let e = reg.lookup(name).unwrap();
            assert!(e.url.starts_with("http"), "{name}: url `{}` is not a URL", e.url);
            assert!(e.chain_id > 0, "{name}: chain_id must be set");
        }
    }

    #[test]
    fn lookup_is_case_sensitive() {
        // Names are keys, matched verbatim — `Ethereum` is not `ethereum`.
        // Users who want a different spelling override with chain_id + url.
        let reg = ChainRegistry::builtin();
        assert!(reg.lookup("Ethereum").is_none());
        assert!(reg.lookup(" ethereum").is_none());
        assert!(reg.lookup("ethereum").is_some());
    }
}
