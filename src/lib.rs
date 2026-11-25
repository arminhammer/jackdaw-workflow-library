#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(
    not(all(feature = "fetch", feature = "duckdb")),
    allow(rustdoc::broken_intra_doc_links)
)]
//! Reusable, composable workflows for the [Jackdaw](https://docs.rs/jackdaw-wf) durable workflow engine.
//!
//! Each workflow category is gated behind a Cargo feature flag (`default = []`) so consumers only compile
//! the dependencies they need:
//!
//! | Feature | Module | What it provides |
//! |---|---|---|
//! | `fetch` | [`fetch`] | Multi-source, resumable HTTP(S) and local file download workflows backed by `gosh-dl`. |
//! | `duckdb` | [`data`] | In-process `DuckDB` SQL step workflows, `@NAME@` templates, and Arrow batch streaming. |
//! | `duckdb-bundled` | — | Compiles `DuckDB` from source instead of linking a prebuilt `libduckdb`. |
//! | `full` | — | Enables `fetch` and `duckdb`. |
//!
//! # Composing Workflows
//!
//! Register all enabled workflows and tasks with [`register`], then compose them via
//! `ctx.run(...)`:
//!
//! ```no_run
//! use jackdaw::prelude::*;
//! use jackdaw_workflow_library::fetch::{FetchFile, FetchSpec};
//!
//! # async fn example(ctx: &mut WorkflowContext<FetchFile>) -> WorkflowResult<()> {
//! let spec = FetchSpec::new("https://example.com/file.parquet", "/tmp/file.parquet");
//! let _fetched = ctx.run(FetchFile::run, &spec).await?;
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "duckdb")]
#[cfg_attr(docsrs, doc(cfg(feature = "duckdb")))]
pub mod data;
#[cfg(feature = "fetch")]
#[cfg_attr(docsrs, doc(cfg(feature = "fetch")))]
pub mod fetch;

#[cfg(any(feature = "fetch", feature = "duckdb"))]
use jackdaw::prelude::EngineBuilder;

/// Registers all enabled tasks and workflows from this library onto `builder`.
#[cfg(any(feature = "fetch", feature = "duckdb"))]
#[must_use]
pub fn register(builder: EngineBuilder) -> EngineBuilder {
    #[cfg(feature = "fetch")]
    let builder = fetch::register(builder);
    #[cfg(feature = "duckdb")]
    let builder = data::register(builder);
    builder
}
