# yard-plugins

Glue and EMR provider plugins for the [yard](https://github.com/sean-mca/yard) CLI. Each plugin is a standalone binary that implements the `PluginHandler` trait from `yard-plugin-sdk` and communicates with the yard host over JSON-over-stdio.

## Architecture

Rust workspace with two binary crates:

- **yard-plugin-glue** — AWS Glue provider: PySpark codegen, Glue job deploy/destroy/verify via aws-sdk-glue
- **yard-plugin-emr** — AWS EMR provider: PySpark codegen, EMR step deploy/destroy/verify via aws-sdk-emr

Both crates depend on `yard-plugin-sdk` (from the yard repo) which provides:
- `PluginHandler` trait — 8 required methods: `name`, `version`, `validate`, `codegen`, `deploy`, `destroy`, `verify`, `schema`
- `PluginServer::run()` — stdio protocol server (handshake, request dispatch, response serialization)
- Re-exports of all protocol types (`CodegenResponse`, `DeployResponse`, etc.)

## Plugin Protocol

Each plugin binary is spawned as a child process by the yard host:

1. Plugin writes a handshake line to stdout (JSON: protocol_version, name, version, capabilities)
2. Host writes a request line to stdin and closes stdin
3. Plugin reads the request, optionally emits progress lines to stdout
4. Plugin writes a response line to stdout
5. Plugin exits

**stdout is the protocol channel.** All logging goes to stderr via `tracing` (re-exported by the SDK). Never use `println!` — it corrupts the protocol stream.

## Reference Implementation

The Glue and EMR provider logic previously lived in `yard-core` and was removed in yard v2.0 (Phase 70). The deleted code is the implementation reference:

- **Glue provider:** `yard/yard-core/src/providers/glue.rs` (before commit `1cfa880`)
- **EMR provider:** `yard/yard-core/src/providers/emr.rs` (before commit `1cfa880`)
- **Glue codegen:** `yard/yard-core/src/codegen/` (PySpark generation via Tera templates)
- **Glue template:** `yard/yard-core/src/templates/glue.py.tera`
- **EMR template:** `yard/yard-core/src/templates/emr.py.tera`

To view this code: `cd ../yard && git show 1cfa880^:yard-core/src/providers/glue.rs`

## Rules

- **All Rust code MUST adhere to every rule in `../yard/rules/`.** This applies to all agents and sub-agents — no exceptions.
- Never modify `Cargo.toml` without asking first
- Never bump versions unless explicitly asked
- `unwrap()` is fine in tests, never in production code
- `unsafe {}` never, anywhere
- Every PR must pass `cargo clippy -D warnings` with zero issues
- Prefer stdlib over adding crates for simple tasks
- stdout is reserved for the plugin protocol — never write to stdout directly in handler code
- Each plugin binary handles exactly one request per invocation (spawn-per-operation model)
- Plugin binaries must work cross-platform: aarch64-apple-darwin, x86_64-apple-darwin, x86_64-unknown-linux-gnu, aarch64-unknown-linux-gnu

## Binary Naming Convention

Release binaries follow the pattern: `yard-plugin-{name}-{version}-{os}-{arch}`

Examples:
- `yard-plugin-glue-0.1.0-aarch64-apple-darwin`
- `yard-plugin-emr-0.1.0-x86_64-unknown-linux-gnu`

## Dependencies

- `yard-plugin-sdk` — the only yard dependency needed (re-exports protocol types + `anyhow` + `serde_json` + `tracing`)
- `aws-sdk-glue` / `aws-sdk-emr` — AWS SDK clients for the respective services
- `aws-config` — AWS credential/region resolution
- `tera` — template engine for PySpark codegen (Glue plugin only)
- `tokio` — async runtime for AWS SDK calls

## Current Milestone

See `.planning/ROADMAP.md` for phases and `.planning/STATE.md` for current position.
