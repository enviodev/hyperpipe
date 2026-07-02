# Running a pipeline

One command takes a pipeline YAML and an env file, builds anything missing, validates, and
starts the system:

```bash
./scripts/run.sh <pipeline.yaml> [env-file] [-- extra hyperpipe args]
```

## Quick start

```bash
# 1. create your env file
cp examples/pipeline.env.example pipeline.env
$EDITOR pipeline.env          # set HYPERSYNC_BEARER_TOKEN + any ${secret:...} values

# 2. run
./scripts/run.sh examples/usdc-multichain.yaml pipeline.env
```

No token yet? Run the offline demo path against the mock server (see [DEMO.md](./DEMO.md)):

```bash
PORT=8799 python3 scripts/mock-hypersync.py &        # terminal 1
./scripts/run.sh my.yaml pipeline.env -- --debug-stdout   # terminal 2
```

## What the launcher does

1. **Loads the env file** — `KEY=VALUE` lines (comments/blank lines ignored, `export` prefix and
   surrounding quotes handled). These become process env, so:
   - `HYPERSYNC_BEARER_TOKEN` authenticates HyperSync,
   - `HYPERPIPE_SECRET_<NAME>` resolves `${secret:NAME}` in the YAML,
   - `AWS_*` feed the S3 sink, `RUST_LOG` sets log verbosity.
2. **Builds the binary** if `target/<profile>/hyperpipe` is missing.
3. **Builds the WASM modules** if they're missing (and adds the `wasm32-wasip2` target if needed),
   then points `HYPERPIPE_MODULE_DIR` at them.
4. **Validates** the pipeline (fails fast on cycles, unknown chains, missing secrets, typos).
5. **Runs** it in the foreground (`exec`, so ctrl-c stops it cleanly and drains).

The env file argument is optional: if omitted, the launcher looks for `.env` next to the YAML,
then `.env` in the current directory.

## Env file format

```dotenv
# comment
HYPERSYNC_BEARER_TOKEN=eyJ...
HYPERPIPE_SECRET_PG_MAIN_DSN=postgres://user:pw@host:5432/db
export RUST_LOG=hyperpipe=info      # `export` prefix is fine
```

See [`examples/pipeline.env.example`](./examples/pipeline.env.example).

## Knobs

| Env var | Default | Effect |
|---|---|---|
| `HYPERPIPE_PROFILE` | `release` | binary build profile (`release` \| `debug`) |
| `HYPERPIPE_MODULE_PROFILE` | `debug` | module build profile (`debug` \| `release`) |
| `HYPERPIPE_REBUILD` | `0` | `1` forces a rebuild of the binary + modules |

## Pass-through args

Anything after `--` goes straight to `hyperpipe run`:

```bash
./scripts/run.sh my.yaml my.env -- --debug-stdout
```

## Troubleshooting

- **`validation failed`** — the message names the exact YAML path (e.g. `sinks[0].config...`). Fix and rerun.
- **live sources 401** — `HYPERSYNC_BEARER_TOKEN` is unset or invalid.
- **`read builtin ... build modules first`** — modules didn't build; run `HYPERPIPE_REBUILD=1 ./scripts/run.sh ...` or `./scripts/build-modules.sh`.
- **secret parse errors** — put `${secret:...}` on its own YAML line (block style), not inside a flow map `{ ... }`.
