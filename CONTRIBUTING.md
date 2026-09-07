# Contributing

Thanks for your interest in HyperPipe. Issues and pull requests are welcome.

## Building

```bash
cargo build                                  # native workspace
rustup target add wasm32-wasip2
./scripts/build-modules.sh                   # guest modules -> modules/target/wasm32-wasip2/debug/
```

See [docs/getting-started.md](./docs/getting-started.md) for running a pipeline and
[docs/codebase.md](./docs/codebase.md) for a tour of the crates.

## Testing

```bash
cargo test                                   # native: config, source, host, cli
(cd modules && cargo test)                   # guest module unit tests
./scripts/e2e/run-all.sh                     # end-to-end (needs docker + the built binary)
```

The host integration tests load the real `.wasm` artifacts and soft-skip when they
are not built, so run `./scripts/build-modules.sh` first.

## Pull requests

- Keep each PR to one change. Describe what it fixes and how you verified it.
- Run `cargo fmt` and `cargo clippy --all-targets` before pushing.
- `wit/hyperpipe.wit` is the module contract. Changes to it need a note in the PR
  about compatibility for existing modules.
- Never commit tokens, DSNs or webhook URLs. Secrets belong in an env file
  (`*.env` is gitignored) and are referenced from YAML as `${secret:NAME}`.

## Reporting security issues

See [SECURITY.md](./SECURITY.md). Please do not file security problems as public issues.

## License

By contributing you agree that your contributions are licensed under the same terms
as the project (MIT OR Apache-2.0).
