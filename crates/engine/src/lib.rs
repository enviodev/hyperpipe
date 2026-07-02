//! HyperPipe engine: configuration, DAG runtime, and checkpoint coordination.
//!
//! Milestone status: `config` is complete (M1). `runtime` and `checkpoint`
//! land in M3 and M7 respectively.

pub mod checkpoint;
pub mod config;

pub use checkpoint::CheckpointStore;
pub use config::{Config, ConfigError};
