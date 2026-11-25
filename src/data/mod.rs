//! In-process DuckDB SQL step and query workflows.
//!
//! - [`duckdb`]: connection management, parameterized script execution, Arrow batch streaming,
//!   and cancellation-safe async sessions with per-session working directories.
//! - [`sql`]: `@NAME@` template rendering, idempotent step execution (`RunSqlStep`), multi-step
//!   pipeline composition (`RunSqlPipeline`), JSON querying (`QueryJson`), and the [`crate::sql_steps!`]
//!   macro.
//! - [`obs`]: optional memory/spill sampling and tracing spans for DuckDB SQL steps.

pub mod duckdb;
pub mod obs;
pub mod sql;

pub use sql::{
    DataError, QueryJson, QueryJsonInput, QueryJsonTask, RunSqlPipeline, RunSqlStep,
    RunSqlStepTask, SqlOutcome, SqlSource, SqlStep, SqlValue, query_json, register, render_sql,
    run_sql_step,
};
