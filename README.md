# jackdaw-workflow-library

Reusable, composable workflows for the [Jackdaw](https://crates.io/crates/jackdaw-wf) durable workflow engine.

## Categories & Cargo Features

All workflow categories are opt-in (`default = []`) so your binary only compiles the dependencies it uses:

| Feature | Module | Dependencies | Description |
|---|---|---|---|
| `fetch` | `fetch` | `gosh-dl`, `jackdaw-wf`, `tokio` | Multi-source, resumable HTTP(S) and `file://`/local download workflows (`FetchFile`, `FetchBatch`) with mirror fallback and SHA-256 verification. |
| `duckdb` | `data` | `duckdb`, `jackdaw-wf`, `tokio`, `regex` | In-process DuckDB SQL step workflows (`RunSqlStep`, `RunSqlPipeline`, `QueryJson`), `@NAME@` template rendering, Arrow batch streaming, and `sql_steps!`. Links prebuilt `libduckdb` by default. |
| `duckdb-bundled` | — | `duckdb/bundled` | Compiles DuckDB from source into the binary (self-contained; ~3–15 minutes cold). |
| `full` | `fetch`, `data` | — | Enables all workflow categories without `duckdb-bundled`. |

## Quick Start

Add `jackdaw-workflow-library` to your `Cargo.toml`:

```toml
[dependencies]
jackdaw-wf = "0.3.0"
jackdaw-workflow-library = { version = "0.1.0", features = ["fetch", "duckdb"] }
```

Register the library's tasks and workflows on your Jackdaw `EngineBuilder`, then compose them inside your own `#[workflow]`:

```rust,no_run
use jackdaw::prelude::*;
use jackdaw_workflow_library::data::{RunSqlStep, SqlSource, SqlStep};
use jackdaw_workflow_library::fetch::{FetchFile, FetchSpec};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[workflow(namespace = "example", name = "ingest", version = "1.0.0")]
struct IngestWorkflow;

#[workflow_methods]
impl IngestWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, dir: PathBuf) -> WorkflowResult<u64> {
        // 1. Fetch a Parquet file with mirror fallback
        let spec = FetchSpec::new("https://example.com/data.parquet", dir.join("raw.parquet"))
            .with_mirrors(["https://mirror.example.com/data.parquet".into()]);
        let fetched = ctx.run(FetchFile::run, &spec).await?;

        // 2. Transform it idempotently with DuckDB
        let step = SqlStep {
            dir,
            name: "summarize".into(),
            sql: SqlSource::Inline(
                "COPY (SELECT count(*) AS n FROM read_parquet('raw.parquet')) TO 'summary.parquet' (FORMAT parquet)".into(),
            ),
            vars: BTreeMap::new(),
            env: BTreeMap::new(),
            outputs: vec!["summary.parquet".into()],
            memory: None,
            temp: None,
        };
        ctx.run(RunSqlStep::run, &step).await?;
        Ok(fetched.bytes)
    }
}
```

## DuckDB Builds

By default (`duckdb` feature), `duckdb-rs` links a prebuilt `libduckdb` v2.0.0 (`DUCKDB_LIB_DIR` / `DUCKDB_INCLUDE_DIR`) for fast incremental builds and CI. Enable `duckdb-bundled` in release builds when you want DuckDB compiled statically into the binary.

## Development & Testing

```bash
# Format & Clippy (pedantic + panic-free)
cargo fmt --all -- --check
cargo clippy --all-targets --features full -- -D warnings

# Run BDD scenarios, unit tests, and doc tests
cargo test --features full --lib --doc
cargo test --features full --test bdd

# Verify docs.rs build
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
