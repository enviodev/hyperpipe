# Security policy

## Reporting a vulnerability

Please do not open a public issue for security problems.

Report them privately through GitHub's
[private vulnerability reporting](https://github.com/enviodev/hyperpipe/security/advisories/new)
for this repository. Include the affected version or commit, a description of the
issue, and reproduction steps or a proof of concept if you have one.

You will receive an acknowledgement within a few business days. We will work with
you on a fix and coordinate disclosure once a release is available.

## Scope

HyperPipe's security model is described in [docs/modules.md](./docs/modules.md):
WASM modules do compute only, and every network or storage access goes through
host imports gated by the `permissions` and `connections` declared in the pipeline
YAML. Reports of any way for a module to reach a host, connection or file it was
not granted, to read a secret, or to escape its memory and time limits are
especially welcome.

## Supported versions

HyperPipe is in beta. Fixes land on `main` and are included in the next release;
there are no long-term support branches.
