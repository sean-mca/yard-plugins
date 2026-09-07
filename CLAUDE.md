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

## Region Handling

Region resolution differs between deploy and destroy/verify due to the `PluginHandler` trait signature:

| Operation | Region source | Credential source | Why |
|-----------|--------------|-------------------|-----|
| `deploy` | `job_config` JSON (`glue.region`) | `job_config` JSON + env vars | `deploy(job_name, job_config, artifact)` receives the full config |
| `destroy` | `AWS_DEFAULT_REGION` env var (fallback: `us-east-1`) | Env vars only (`YARD_AWS_ASSUME_ROLE`, `YARD_AWS_SESSION_NAME`, `YARD_AWS_EXTERNAL_ID`) | `destroy(job_name, resources)` has no config |
| `verify` | `AWS_DEFAULT_REGION` env var (fallback: `us-east-1`) | Env vars only (same as destroy) | `verify(job_name, resources)` has no config |

**Host responsibility:** Before spawning a plugin for `destroy` or `verify`, the yard host **must** set `AWS_DEFAULT_REGION` to the region used during the original deploy (available in the job state file). If the host does not set it, the plugin falls back to `us-east-1`, which may target the wrong region and fail silently or destroy resources in the wrong account.

**AssumeRole in destroy/verify:** Because destroy and verify pass `None` for the `aws_cfg` parameter, AssumeRole credentials come exclusively from the `YARD_AWS_ASSUME_ROLE`, `YARD_AWS_SESSION_NAME`, and `YARD_AWS_EXTERNAL_ID` environment variables — not from config JSON. The host must set these env vars if cross-account access is needed.

This is a structural consequence of the spawn-per-operation plugin model. The reference implementation (`yard-core`) used a long-lived `GlueProvider` struct that stored the client at construction time, so all operations shared the same region. In the plugin model, each invocation is independent and must resolve region from the environment.

## Reference Implementation

The Glue and EMR provider logic previously lived in `yard-core` and was removed in yard v2.0 (Phase 70). The deleted code is the implementation reference:

- **Glue provider:** `yard/yard-core/src/providers/glue.rs` (before commit `1cfa880`)
- **EMR provider:** `yard/yard-core/src/providers/emr.rs` (before commit `1cfa880`)
- **Glue codegen:** `yard/yard-core/src/codegen/` (PySpark generation via Tera templates)
- **Glue template:** `yard/yard-core/src/templates/glue.py.tera`
- **EMR template:** `yard/yard-core/src/templates/emr.py.tera`

To view this code: `cd ../yard && git show 1cfa880^:yard-core/src/providers/glue.rs`

## Running Integration Tests

The Glue plugin's lifecycle tests run against **ministack**, a local MIT-licensed AWS emulator listening on port 4566. No AWS account and no credentials are needed.

| Command | What it does |
|---------|--------------|
| `make ministack-up` | Starts the pinned ministack container and waits for the gateway to answer |
| `make ministack-down` | Stops and removes the container and its volumes |
| `make test-integration` | Exports `YARD_TEST_AWS_ENDPOINT=http://127.0.0.1:4566` and runs the Glue suite |
| `make test` | The offline suite (`cargo test --workspace`). Needs no Docker and must stay green at all times |

**A skipped test is not a passing test.** When `YARD_TEST_AWS_ENDPOINT` is unset the lifecycle tests return early and write a skip note to stderr, so the offline suite stays green on a machine with no emulator. Any genuine check of the INTG requirements needs the gated run.

`make ministack-up` is optional — point `YARD_TEST_AWS_ENDPOINT` at any already-running ministack instead, which is the way around a port-4566 conflict with a container started outside this repo.

Write the endpoint as the IP literal `127.0.0.1`, not `localhost`: an IP-literal endpoint makes the S3 client select path-style addressing, which is portable across all four target platforms.

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
