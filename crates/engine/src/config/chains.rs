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
}
