//! Templated SQL steps, idempotent execution, and Jackdaw data workflows.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use jackdaw::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::Instrument as _;

use super::duckdb::{
    DuckConfig, DuckError, Param, Params, note_start, query_json_rows, run_script,
    run_script_profiled, run_session,
};
use super::obs;

/// Errors returned by SQL step and query workflows.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum DataError {
    /// A transient or DuckDB execution error that may succeed on retry.
    #[error("retryable data error: {0}")]
    Retryable(String),
    /// A permanent error (missing template, bad placeholder, missing expected output).
    #[error("data step failed: {0}")]
    Fatal(String),
}

impl DataError {
    /// Converts this error into a [`TaskError`], preserving retryability.
    #[must_use]
    pub fn into_task_error(self) -> TaskError {
        match self {
            Self::Retryable(msg) => TaskError::retryable(msg),
            Self::Fatal(msg) => TaskError::non_retryable(msg),
        }
    }
}

/// Placeholder value substituted into `@NAME@` markers by [`render_sql`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlValue {
    /// Substituted verbatim (for numbers or identifiers).
    Raw(String),
    /// Substituted as a SQL-escaped path inside surrounding single quotes.
    Path(String),
    /// Substituted as a comma-separated list of single-quoted paths.
    Files(Vec<String>),
}

/// Source of SQL text for a [`SqlStep`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlSource {
    /// Built-in template name (without `.sql`) looked up in a caller-supplied template table.
    Template(String),
    /// Inline SQL script text.
    Inline(String),
}

/// Specification for a single idempotent DuckDB SQL step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlStep {
    /// Working directory where the SQL runs and relative paths resolve.
    pub dir: PathBuf,
    /// Step name (used for `logs/<name>.log` and `logs/<name>.running`).
    pub name: String,
    /// SQL template or inline script.
    pub sql: SqlSource,
    /// Values for `@NAME@` template placeholders.
    #[serde(default)]
    pub vars: BTreeMap<String, SqlValue>,
    /// Parameters bound as DuckDB variables `env_<lowercase_name>` (`getvariable('env_...')`).
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Output files relative to `dir`; when all exist (and no `.running` marker exists), the step is skipped.
    #[serde(default)]
    pub outputs: Vec<String>,
    /// Optional DuckDB memory limit (e.g. `"8GB"`).
    #[serde(default)]
    pub memory: Option<String>,
    /// Optional DuckDB spill directory.
    #[serde(default)]
    pub temp: Option<String>,
}

impl SqlStep {
    /// Returns a copy of this step using a different named template.
    #[must_use]
    pub fn with_template(mut self, template: &str) -> Self {
        self.sql = SqlSource::Template(template.to_string());
        self
    }
}

/// Outcome of executing a [`SqlStep`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SqlOutcome {
    /// Step name.
    pub step: String,
    /// `true` if all expected outputs already existed cleanly and DuckDB was not invoked.
    pub skipped: bool,
    /// Configured output paths.
    pub outputs: Vec<String>,
    /// Elapsed execution seconds (`0.0` when skipped).
    #[serde(default)]
    pub seconds: f64,
}

/// Converts `(KEY, value)` pairs into DuckDB `Params` named `env_<lowercase_key>`.
#[must_use]
pub fn env_params<'a>(env: impl IntoIterator<Item = (&'a String, &'a String)>) -> Params {
    env.into_iter()
        .map(|(k, v)| (format!("env_{}", k.to_lowercase()), Param::Text(v.clone())))
        .collect()
}

/// Renders `@NAME@` placeholders in `template` using `vars`.
///
/// # Errors
///
/// Returns an error if any `@NAME@` placeholder has no entry in `vars` or any key in `vars` is
/// unused by `template`.
pub fn render_sql(template: &str, vars: &BTreeMap<String, SqlValue>) -> anyhow::Result<String> {
    let re = regex::Regex::new(r"@([A-Z][A-Z0-9_]*)@").map_err(|e| anyhow::anyhow!("{e}"))?;
    let used: std::collections::BTreeSet<&str> = re
        .captures_iter(template)
        .filter_map(|c| c.get(1))
        .map(|m| m.as_str())
        .collect();
    if let Some(missing) = used.iter().find(|n| !vars.contains_key(**n)) {
        anyhow::bail!(
            "the template's placeholder @{missing}@ has no value (missing placeholder {missing})"
        );
    }
    if let Some(unused) = vars.keys().find(|k| !used.contains(k.as_str())) {
        anyhow::bail!("the value {unused} is not used by the template (unused value {unused})");
    }
    let quote = |p: &str| p.replace('\'', "''");
    Ok(re
        .replace_all(template, |c: &regex::Captures<'_>| match &vars[&c[1]] {
            SqlValue::Raw(v) => v.clone(),
            SqlValue::Path(p) => quote(p),
            SqlValue::Files(fs) => fs
                .iter()
                .map(|f| format!("'{}'", quote(f)))
                .collect::<Vec<_>>()
                .join(", "),
        })
        .into_owned())
}

fn tail(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines.get(start..).unwrap_or(&[]).join("\n")
}

/// Runs a [`SqlStep`] in process on DuckDB, skipping when all outputs exist and no `.running`
/// marker is present.
///
/// # Errors
///
/// Returns [`DataError::Fatal`] on missing templates, bad placeholders, or missing expected
/// outputs, and [`DataError::Retryable`] on DuckDB execution or I/O failures.
pub async fn run_sql_step(
    step: &SqlStep,
    templates: &[(&str, &str)],
) -> Result<SqlOutcome, DataError> {
    let span = obs::step_span(&step.name, &step.dir, step.memory.as_deref());
    let r = step_inner(step, templates, &span)
        .instrument(span.clone())
        .await;
    if let Err(e) = &r {
        span.record("failed", true);
        span.in_scope(|| tracing::warn!(error = %tail(&e.to_string(), 2), "sql step failed"));
    }
    r
}

#[expect(
    clippy::too_many_lines,
    reason = "single-flow step setup, DuckDB session, telemetry, and cleanup"
)]
async fn step_inner(
    step: &SqlStep,
    templates: &[(&str, &str)],
    span: &tracing::Span,
) -> Result<SqlOutcome, DataError> {
    let started = std::time::Instant::now();
    let done = |skipped| {
        if obs::active() {
            span.record("skipped", skipped);
            if skipped {
                metrics::counter!(
                    "pipeline_sql_steps_skipped_total",
                    "step" => obs::step_base(&step.name).to_string()
                )
                .increment(1);
            }
        }
        SqlOutcome {
            step: step.name.clone(),
            skipped,
            outputs: step.outputs.clone(),
            seconds: if skipped {
                0.0
            } else {
                started.elapsed().as_secs_f64()
            },
        }
    };

    let logs = step.dir.join("logs");
    let running = logs.join(format!("{}.running", step.name));
    if !step.outputs.is_empty()
        && step.outputs.iter().all(|o| step.dir.join(o).exists())
        && !running.exists()
    {
        return Ok(done(true));
    }

    for o in &step.outputs {
        let _ = std::fs::remove_file(step.dir.join(o));
    }
    std::fs::create_dir_all(&logs)
        .map_err(|e| DataError::Retryable(format!("{}: {e}", logs.display())))?;
    std::fs::write(&running, "")
        .map_err(|e| DataError::Retryable(format!("{}: {e}", running.display())))?;

    let mut cleanup = Cleanup {
        dir: step.dir.clone(),
        outputs: step.outputs.clone(),
        armed: true,
    };

    let text = match &step.sql {
        SqlSource::Template(name) => templates
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| *t)
            .ok_or_else(|| DataError::Fatal(format!("no SQL template {name}")))?,
        SqlSource::Inline(s) => s.as_str(),
    };
    let body = render_sql(text, &step.vars)
        .map_err(|e| DataError::Fatal(format!("{}: {e:#}", step.name)))?;

    let mut params = env_params(&step.env);
    if let Some(m) = &step.memory {
        params.insert("env_duckdb_memory".into(), Param::Text(m.clone()));
    }
    let temp = step
        .temp
        .as_ref()
        .map_or_else(|| step.dir.join("duckdb_tmp"), PathBuf::from);
    params.insert(
        "env_duckdb_temp".into(),
        Param::Text(temp.display().to_string()),
    );

    note_start(&step.dir);
    let cfg = DuckConfig {
        memory_limit: step.memory.clone(),
        temp_directory: Some(temp),
        dir: Some(step.dir.clone()),
        ..Default::default()
    };
    let (dir, outputs) = (step.dir.clone(), step.outputs.clone());
    let (in_span, name) = (span.clone(), step.name.clone());

    let result = run_session(cfg, move |s, cancelled| {
        let sampler = obs::Sampler::start(s.conn);
        let mut stmts = Vec::new();
        let r = if obs::profiling() {
            run_script_profiled(s.conn, &body, &params, Some(&mut stmts))
        } else {
            run_script(s.conn, &body, &params)
        };
        if let Some(sampler) = sampler {
            let (sampled_mem, sampled_spill) = sampler.finish(s.conn);
            let prof = obs::StepProfile::of(&stmts);
            let mem = sampled_mem.max(prof.peak_buffer_bytes);
            let spill = sampled_spill.max(prof.peak_temp_bytes);
            in_span.record("duckdb_memory_peak_bytes", mem);
            in_span.record("spill_peak_bytes", spill);
            in_span.record("cpu_time_s", prof.cpu_s);
            in_span.record("wall_s", prof.wall_s);
            in_span.record("parallelism", prof.parallelism());
            in_span.record("bytes_read", prof.bytes_read);
            in_span.record("bytes_written", prof.bytes_written);
            in_span.record("rows_scanned", prof.rows_scanned);
            if r.is_ok() {
                in_span.record("rows_out", obs::parquet_rows(s.conn, &outputs));
                if obs::profile_operators() {
                    in_span.in_scope(|| {
                        for st in &stmts {
                            for o in &st.operators {
                                tracing::info!(
                                    name: "operator",
                                    operator = %o.name,
                                    seconds = o.seconds,
                                    rows = o.rows,
                                    statement = st.statement as u64
                                );
                            }
                        }
                    });
                }
            }
            obs::step_metrics(&name, started.elapsed().as_secs_f64(), mem, spill, &prof);
        }
        if r.is_err() || cancelled.load(Ordering::SeqCst) {
            for o in &outputs {
                let _ = std::fs::remove_file(dir.join(o));
            }
        }
        r
    })
    .await;

    let log = match &result {
        Ok(l) => l.text.clone(),
        Err(e) => e.to_string(),
    };
    let _ = std::fs::write(logs.join(format!("{}.log", step.name)), log);

    result.map_err(|e| {
        let msg = format!("SQL step {}: {e}", step.name);
        if matches!(e, DuckError::BadParam(_)) {
            DataError::Fatal(msg)
        } else {
            DataError::Retryable(msg)
        }
    })?;

    if let Some(missing) = step.outputs.iter().find(|o| !step.dir.join(o).exists()) {
        return Err(DataError::Fatal(format!(
            "SQL step {} finished without writing {missing}",
            step.name
        )));
    }

    cleanup.armed = false;
    let _ = std::fs::remove_file(&running);
    if obs::active() {
        span.record("seconds", started.elapsed().as_secs_f64());
    }
    Ok(done(false))
}

struct Cleanup {
    dir: PathBuf,
    outputs: Vec<String>,
    armed: bool,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        if self.armed {
            for o in &self.outputs {
                let _ = std::fs::remove_file(self.dir.join(o));
            }
        }
    }
}

/// Executes a read-only SQL query in `dir` and returns all rows as JSON object maps.
///
/// # Errors
///
/// Returns [`DataError::Retryable`] if DuckDB fails to execute the query.
pub async fn query_json(
    dir: &Path,
    sql: &str,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, DataError> {
    let sql = sql.to_string();
    run_session(DuckConfig::in_dir(dir), move |s, _| {
        query_json_rows(s.conn, &sql)
    })
    .await
    .map_err(|e| DataError::Retryable(e.to_string()))
}

/// Extracts and deserializes column `name` from a JSON row returned by [`query_json`].
///
/// # Errors
///
/// Returns [`DataError::Fatal`] if the column is missing or cannot be deserialized into `T`.
pub fn field<T: serde::de::DeserializeOwned>(
    row: &serde_json::Map<String, serde_json::Value>,
    name: &str,
) -> Result<T, DataError> {
    let v = row
        .get(name)
        .cloned()
        .ok_or_else(|| DataError::Fatal(format!("the query result has no {name} column")))?;
    serde_json::from_value(v).map_err(|e| DataError::Fatal(format!("column {name}: {e}")))
}

/// Input for [`QueryJson`] and [`QueryJsonTask`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryJsonInput {
    /// Working directory where relative paths in `sql` resolve.
    pub dir: PathBuf,
    /// SQL query to execute.
    pub sql: String,
}

/// Jackdaw task that executes an inline [`SqlStep`].
///
/// # Errors
///
/// Returns a [`TaskError`] if the SQL step fails.
#[task(name = "jwl.run_sql_step")]
pub async fn run_sql_step_task(step: SqlStep) -> Result<SqlOutcome, TaskError> {
    run_sql_step(&step, &[])
        .await
        .map_err(DataError::into_task_error)
}

/// Jackdaw task that executes [`query_json`].
///
/// # Errors
///
/// Returns a [`TaskError`] if the query fails.
#[task(name = "jwl.query_json")]
pub async fn query_json_task(
    input: QueryJsonInput,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, TaskError> {
    query_json(&input.dir, &input.sql)
        .await
        .map_err(DataError::into_task_error)
}

/// Durable Jackdaw workflow for executing a single inline [`SqlStep`].
#[workflow(namespace = "jwl", name = "run-sql-step", version = "1.0.0")]
pub struct RunSqlStep;

/// Durable Jackdaw workflow that runs a sequence of [`SqlStep`]s as child [`RunSqlStep`]
/// workflows.
#[workflow(namespace = "jwl", name = "run-sql-pipeline", version = "1.0.0")]
pub struct RunSqlPipeline;

/// Durable Jackdaw workflow that executes a DuckDB query and returns JSON rows.
#[workflow(namespace = "jwl", name = "query-json", version = "1.0.0")]
pub struct QueryJson;

mod workflow_impls {
    #![allow(missing_docs)]
    use super::{
        QueryJson, QueryJsonInput, QueryJsonTask, RunSqlPipeline, RunSqlStep, RunSqlStepTask,
        SqlOutcome, SqlStep,
    };
    use jackdaw::prelude::*;

    #[workflow_methods]
    impl RunSqlStep {
        /// Runs [`RunSqlStepTask`] with `step`.
        #[run]
        pub async fn run(
            ctx: &mut WorkflowContext<Self>,
            step: SqlStep,
        ) -> WorkflowResult<SqlOutcome> {
            ctx.call(RunSqlStepTask, &step).await
        }
    }

    #[workflow_methods]
    impl RunSqlPipeline {
        /// Runs each [`SqlStep`] in `steps` sequentially via [`RunSqlStep`].
        #[run]
        pub async fn run(
            ctx: &mut WorkflowContext<Self>,
            steps: Vec<SqlStep>,
        ) -> WorkflowResult<Vec<SqlOutcome>> {
            let mut out = Vec::with_capacity(steps.len());
            for step in &steps {
                out.push(ctx.run(RunSqlStep::run, step).await?);
            }
            Ok(out)
        }
    }

    #[workflow_methods]
    impl QueryJson {
        /// Runs [`QueryJsonTask`] with `input`.
        #[run]
        pub async fn run(
            ctx: &mut WorkflowContext<Self>,
            input: QueryJsonInput,
        ) -> WorkflowResult<Vec<serde_json::Map<String, serde_json::Value>>> {
            ctx.call(QueryJsonTask, &input).await
        }
    }
}

/// Registers all `data` tasks and workflows onto `builder`.
#[must_use]
pub fn register(builder: EngineBuilder) -> EngineBuilder {
    builder
        .task(RunSqlStepTask)
        .task(QueryJsonTask)
        .workflow::<RunSqlStep>()
        .workflow::<RunSqlPipeline>()
        .workflow::<QueryJson>()
}

/// Embeds a directory of `.sql` templates and generates `TEMPLATES`, `template(name)`,
/// `run_sql_step(&step)`, and a Jackdaw `#[task]` (`run_sql`).
#[macro_export]
macro_rules! sql_steps {
    ($($name:literal),+ $(,)?) => {
        $crate::sql_steps!(in "../../sql"; $($name),+);
    };
    (in $dir:literal; $($name:literal),+ $(,)?) => {
        $crate::sql_steps!(in $dir, task "run_sql"; $($name),+);
    };
    (in $dir:literal, task $task:literal; $($name:literal),+ $(,)?) => {
        pub use $crate::data::sql::{SqlOutcome, SqlSource, SqlStep, SqlValue, render_sql};

        /// The SQL templates embedded into the binary.
        pub const TEMPLATES: &[(&str, &str)] = &[
            $(($name, include_str!(concat!($dir, "/", $name, ".sql")))),+
        ];

        /// Returns the text of a built-in template by name.
        #[must_use]
        pub fn template(name: &str) -> Option<&'static str> {
            TEMPLATES.iter().find(|(n, _)| *n == name).map(|(_, t)| *t)
        }

        /// Runs one SQL step in process on DuckDB using [`TEMPLATES`].
        ///
        /// # Errors
        ///
        /// Returns [`DataError`]($crate::data::sql::DataError) if the step fails.
        pub async fn run_sql_step(
            step: &SqlStep,
        ) -> Result<SqlOutcome, $crate::data::sql::DataError> {
            $crate::data::sql::run_sql_step(step, TEMPLATES).await
        }

        /// Jackdaw task that executes [`run_sql_step`].
        ///
        /// # Errors
        ///
        /// Returns a [`TaskError`](::jackdaw::prelude::TaskError) if the SQL step fails.
        #[::jackdaw::prelude::task(name = $task)]
        pub async fn run_sql(
            step: SqlStep,
        ) -> Result<SqlOutcome, ::jackdaw::prelude::TaskError> {
            run_sql_step(&step)
                .await
                .map_err($crate::data::sql::DataError::into_task_error)
        }
    };
}
