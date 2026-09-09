# yard-plugins

Provider plugins for the [yard](https://github.com/sean-mca/yard) CLI — a data
engineering tool with terragrunt-style hierarchical configuration.

yard v2 loads providers as standalone executables rather than compiling them
into the CLI, the way Terraform loads providers. Each plugin here is its own
binary that speaks a small JSON-over-stdio protocol: the host spawns it, sends
one request, reads one response, and the process exits.

This repository holds two of them.

| Plugin | Language | Target | Status |
|---|---|---|---|
| `yard-plugin-glue` | Rust | AWS Glue ETL jobs | Implemented — validate, codegen, deploy, destroy, verify, schema |
| `yard-plugin-airflow` | Python | Airflow DAG files on S3 | Implemented — validate, codegen, deploy, destroy, verify, schema |

The Airflow plugin exists in Python on purpose: it demonstrates that the
protocol is language-agnostic. It reimplements the same handshake and dispatch
lifecycle as the Rust SDK's `PluginServer::run()` with nothing but the standard
library, Jinja2, and boto3.

## The protocol

A plugin binary handles exactly one request per invocation:

1. The plugin writes a handshake line to stdout — `protocol_version`, `name`,
   `version`, and the list of operations it supports.
2. The host writes one request line to stdin, then closes stdin.
3. The plugin may emit progress lines to stdout.
4. The plugin writes one response line to stdout and exits.

**stdout is the protocol channel.** Every log line goes to stderr — `tracing`
in the Rust plugins, the `logging` module in the Python one. A stray `println!`
corrupts the stream and the host fails to parse the response.

Six operations make up the handler surface:

| Operation | What it does |
|---|---|
| `validate` | Check a job's config block and return field-level errors |
| `codegen` | Render the job config into a runnable artifact (PySpark script, Airflow DAG) |
| `deploy` | Upload the artifact and create or update the remote resource |
| `destroy` | Delete the resources recorded from a previous deploy |
| `verify` | Report whether each recorded resource still exists |
| `schema` | Describe the config fields the plugin accepts |

## Layout

```
yard-plugin-common/     shared Rust library: PySpark codegen + AWS helpers
  src/codegen/          source, transform, sink, and PII-masking codegen
  src/templates/        Tera template for the generated PySpark
  src/aws/              config resolution and S3 script upload/delete
  tests/                snapshot tests over 8 YAML job fixtures
yard-plugin-glue/       AWS Glue provider binary
yard-plugin-airflow/    Airflow DAG provider, single-file Python
```

Codegen is snapshot-tested with `insta`: each fixture in
`yard-plugin-common/tests/fixtures/` renders to a checked-in `.snap` file, so
any drift in the generated PySpark shows up as a diff rather than a surprise in
production.

## Building

The Rust workspace depends on `yard-plugin-sdk` through a path dependency on a
sibling checkout of the yard repository:

```
yard-plugin-sdk = { path = "../yard/yard-plugin-sdk" }
```

So both repositories need to sit next to each other:

```
some-dir/
├── yard/
└── yard-plugins/
```

Then:

```sh
cargo build --workspace --release
```

The Python plugin needs no build step — `pip install -r
yard-plugin-airflow/requirements.txt` and run the script directly.

Release binaries are named `yard-plugin-{name}-{version}-{os}-{arch}`, for
example `yard-plugin-glue-0.1.0-aarch64-apple-darwin`. The supported targets are
`aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-gnu`, and
`aarch64-unknown-linux-gnu`.

## Testing

```sh
make test              # offline suite, no Docker, no network, no credentials
make lint              # cargo clippy --workspace --all-targets -- -D warnings
make ministack-up      # start the local AWS emulator
make test-integration  # Glue lifecycle tests against the emulator
make ministack-down
```

The offline suite must stay green at all times. It covers protocol handshakes,
validation rules, and codegen snapshots — none of which touch a network.

The Glue lifecycle tests are opt-in and run against
[ministack](https://hub.docker.com/r/ministackorg/ministack), a local AWS
emulator on port 4566. No AWS account and no credentials are involved. They read
`YARD_TEST_AWS_ENDPOINT` and skip themselves cleanly when it is unset, which is
what keeps `make test` green on a machine with no Docker. **A skipped test is
not a passing test** — checking the integration behavior means running the gated
suite.

`make ministack-up` is optional. Pointing `YARD_TEST_AWS_ENDPOINT` at any
already-running ministack works just as well, and is the way around a port 4566
conflict with a container started elsewhere:

```sh
YARD_TEST_AWS_ENDPOINT=http://127.0.0.1:4566 make test-integration
```

Write the endpoint as the IP literal `127.0.0.1` rather than `localhost`. An
IP-literal endpoint makes the S3 client select path-style addressing, which
resolves consistently across all four target platforms; a hostname yields
`<bucket>.localhost:4566` virtual-host URLs whose DNS resolution is not portable.

## Region and credentials

Region resolution differs between operations, and the reason is structural. The
`PluginHandler` trait hands `deploy` the full job config, but gives `destroy`
and `verify` only a resource list:

| Operation | Region source | Credential source |
|---|---|---|
| `deploy` | `glue.region` in the job config | Job config plus environment |
| `destroy` | `AWS_DEFAULT_REGION` (falls back to `us-east-1`) | Environment only |
| `verify` | `AWS_DEFAULT_REGION` (falls back to `us-east-1`) | Environment only |

**This puts a requirement on the host.** Before spawning a plugin for `destroy`
or `verify`, yard must set `AWS_DEFAULT_REGION` to the region recorded in the
job state file from the original deploy. Without it the plugin falls back to
`us-east-1` and may target the wrong region entirely.

The same applies to cross-account access: because `destroy` and `verify` receive
no config, AssumeRole details come only from `YARD_AWS_ASSUME_ROLE`,
`YARD_AWS_SESSION_NAME`, and `YARD_AWS_EXTERNAL_ID` in the environment.

This is the cost of the spawn-per-operation model. The provider logic these
plugins replace lived inside yard itself as a long-lived struct that built its
client once, so every operation shared a region by construction. Each plugin
invocation is independent and has to resolve the region from what it is handed.

## Conventions

- No `unwrap()` in production code; tests are free to use it
- No `unsafe`, anywhere
- `cargo clippy -D warnings` passes with zero issues
- Never write to stdout from handler code
