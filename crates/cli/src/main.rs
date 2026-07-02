//! `hyperpipe` — run and validate WASM data pipelines on HyperSync.

mod health;
mod pipeline;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "hyperpipe", version, about = "WASM data pipelines on HyperSync")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Validate a pipeline file (parse + full semantic checks + module dry-run).
    Validate {
        /// Path to the pipeline YAML.
        file: PathBuf,
    },
    /// Run a pipeline in the foreground until EOF or ctrl-c.
    Run {
        /// Path to the pipeline YAML.
        file: PathBuf,
        /// Print every record to stdout as NDJSON (debug; bypasses sinks).
        #[arg(long)]
        debug_stdout: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hyperpipe=info,hp_engine=info,hp_source_hypersync=info,hp_wasm_host=info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Validate { file } => cmd_validate(file).await,
        Command::Run { file, debug_stdout } => cmd_run(file, debug_stdout).await,
    }
}

async fn cmd_validate(file: PathBuf) -> Result<()> {
    match hp_engine::Config::load(&file) {
        Ok(cfg) => {
            println!("{}", cfg.summary());
            Ok(())
        }
        Err(e) => {
            eprintln!("invalid pipeline: {e}");
            std::process::exit(1);
        }
    }
}

async fn cmd_run(file: PathBuf, debug_stdout: bool) -> Result<()> {
    pipeline::run(&file, debug_stdout).await
}
