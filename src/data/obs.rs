//! Observability for DuckDB SQL steps: `sql_step` tracing spans, periodic memory/spill sampling,
//! and statement-level JSON profiling.
//!
//! When telemetry is inactive ([`active`] is `false`), no spans, sampler threads, or profiling
//! queries are created.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use duckdb::Connection;

static ACTIVE: AtomicBool = AtomicBool::new(false);
static PROFILE: AtomicBool = AtomicBool::new(false);
static SAMPLERS: AtomicUsize = AtomicUsize::new(0);
static STEP_RUNS: AtomicUsize = AtomicUsize::new(0);
static STARTED_AT: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
static FIRST_BATCH: std::sync::Once = std::sync::Once::new();

/// How often DuckDB's memory and spill are sampled while a step runs.
pub const SAMPLE_EVERY: Duration = Duration::from_millis(250);

/// Maximum number of slowest operators recorded as span events per statement.
pub const TOP_OPERATORS: usize = 5;

/// Returns whether telemetry is active in this process.
#[must_use]
pub fn active() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

/// Enables or disables DuckDB step observability.
pub fn set_active(on: bool) {
    if on {
        STARTED_AT.get_or_init(std::time::Instant::now);
    }
    ACTIVE.store(on, Ordering::Relaxed);
}

/// Returns elapsed seconds since telemetry was activated, or `0.0` if inactive.
#[must_use]
pub fn uptime_seconds() -> f64 {
    STARTED_AT.get().map_or(0.0, |t| t.elapsed().as_secs_f64())
}

/// Records the time-to-first-batch gauge once when telemetry is active.
pub fn note_first_batch() {
    if active() {
        FIRST_BATCH.call_once(|| {
            metrics::gauge!("pipeline_time_to_first_batch_seconds").set(uptime_seconds());
        });
    }
}

/// Returns whether statement-level JSON profiling is enabled.
#[must_use]
pub fn profiling() -> bool {
    active()
}

/// Returns whether slowest-operator span events should be emitted for this step.
#[must_use]
pub fn profile_operators() -> bool {
    if !(active()
        && (PROFILE.load(Ordering::Relaxed)
            || std::env::var("PIPELINE_SQL_PROFILE").is_ok_and(|v| !v.is_empty() && v != "0")))
    {
        return false;
    }
    let every: usize = std::env::var("PIPELINE_SQL_PROFILE_EVERY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1);
    STEP_RUNS
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(every)
}

/// Overrides operator-level profiling programmatically.
pub fn set_profile(on: bool) {
    PROFILE.store(on, Ordering::Relaxed);
}

/// Returns the total number of memory samplers started in this process.
#[must_use]
pub fn samplers_started() -> usize {
    SAMPLERS.load(Ordering::Relaxed)
}

/// Strips a trailing 15-hex-digit batch/cell suffix from a step name if present.
#[must_use]
pub fn step_base(name: &str) -> &str {
    let b = name.as_bytes();
    for i in 0..b.len() {
        if b.get(i) == Some(&b'_')
            && b.len() >= i + 16
            && b.get(i + 1..i + 16)
                .is_some_and(|slice| slice.iter().all(u8::is_ascii_hexdigit))
            && b.get(i + 16).is_none_or(|c| *c == b'_')
        {
            return name.get(..i).unwrap_or(name);
        }
    }
    name
}

/// Samples current DuckDB memory usage and temporary spill bytes.
#[must_use]
pub fn sample(conn: &Connection) -> (u64, u64) {
    let one = |sql: &str| {
        conn.query_row(sql, [], |r| r.get::<_, i64>(0))
            .map_or(0, |n| u64::try_from(n).unwrap_or(0))
    };
    let mem = one("SELECT coalesce(sum(memory_usage_bytes), 0)::BIGINT FROM duckdb_memory()");
    let spill = one(
        "SELECT greatest(coalesce((SELECT sum(size) FROM duckdb_temporary_files()), 0), coalesce((SELECT sum(temporary_storage_bytes) FROM duckdb_memory()), 0))::BIGINT",
    );
    (mem, spill)
}

/// Background sampler polling DuckDB memory and spill on a cloned connection.
pub struct Sampler {
    stop: Arc<AtomicBool>,
    mem: Arc<AtomicU64>,
    spill: Arc<AtomicU64>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Sampler {
    /// Starts sampling `conn` if telemetry is active and `conn` can be cloned.
    #[must_use]
    pub fn start(conn: &Connection) -> Option<Self> {
        if !active() {
            return None;
        }
        let own = conn.try_clone().ok()?;
        SAMPLERS.fetch_add(1, Ordering::Relaxed);
        let stop = Arc::new(AtomicBool::new(false));
        let mem = Arc::new(AtomicU64::new(0));
        let spill = Arc::new(AtomicU64::new(0));
        let (stop_flag, mem_peak, spill_peak) =
            (Arc::clone(&stop), Arc::clone(&mem), Arc::clone(&spill));
        let thread = std::thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                let (cur_mem, cur_spill) = sample(&own);
                mem_peak.fetch_max(cur_mem, Ordering::Relaxed);
                spill_peak.fetch_max(cur_spill, Ordering::Relaxed);
                let until = std::time::Instant::now() + SAMPLE_EVERY;
                while !stop_flag.load(Ordering::Relaxed) && std::time::Instant::now() < until {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        });
        Some(Self {
            stop,
            mem,
            spill,
            thread: Some(thread),
        })
    }

    /// Stops the sampler thread and returns peak `(memory_bytes, spill_bytes)`.
    pub fn finish(mut self, conn: &Connection) -> (u64, u64) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        let (a, b) = sample(conn);
        (
            self.mem.load(Ordering::Relaxed).max(a),
            self.spill.load(Ordering::Relaxed).max(b),
        )
    }
}

/// Counts total rows across the `.parquet` files listed in `outputs`.
#[must_use]
pub fn parquet_rows(conn: &Connection, outputs: &[String]) -> u64 {
    outputs
        .iter()
        .filter(|o| o.ends_with(".parquet"))
        .map(|o| {
            conn.query_row(
                &format!(
                    "SELECT coalesce(sum(num_rows), 0)::BIGINT FROM parquet_file_metadata('{}')",
                    o.replace('\'', "''")
                ),
                [],
                |r| r.get::<_, i64>(0),
            )
            .map_or(0, |n| u64::try_from(n).unwrap_or(0))
        })
        .sum()
}

/// A physical operator entry from DuckDB's JSON query profile.
#[derive(Clone, Debug, PartialEq)]
pub struct Operator {
    /// Operator type name (e.g. `HASH_JOIN`).
    pub name: String,
    /// Execution time in seconds.
    pub seconds: f64,
    /// Intermediate rows processed.
    pub rows: u64,
}

/// Extracts the slowest [`TOP_OPERATORS`] operators from a DuckDB JSON profile.
#[must_use]
pub fn top_operators(profile: &serde_json::Value) -> Vec<Operator> {
    fn walk(n: &serde_json::Value, out: &mut Vec<Operator>) {
        if let (Some(t), Some(s)) = (
            n.get("type").and_then(|t| t.as_str()),
            n.get("timing").and_then(serde_json::Value::as_f64),
        ) {
            out.push(Operator {
                name: t.to_string(),
                seconds: s,
                rows: n
                    .get("intermediate_rows")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            });
        }
        for c in n
            .get("children")
            .and_then(|c| c.as_array())
            .into_iter()
            .flatten()
        {
            walk(c, out);
        }
    }
    let mut out = Vec::new();
    for root in profile
        .get("operator")
        .and_then(|o| o.as_array())
        .into_iter()
        .flatten()
    {
        walk(root, &mut out);
    }
    out.sort_by(|a, b| b.seconds.total_cmp(&a.seconds));
    out.truncate(TOP_OPERATORS);
    out
}

/// Parsed DuckDB JSON profile for a single SQL statement.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StmtProfile {
    /// Zero-based statement index within the script.
    pub statement: usize,
    /// Summed CPU time across all threads in seconds.
    pub cpu_s: f64,
    /// Wall-clock duration in seconds.
    pub wall_s: f64,
    /// Peak buffer manager memory in bytes.
    pub peak_buffer_bytes: u64,
    /// Peak temporary spill directory size in bytes.
    pub peak_temp_bytes: u64,
    /// Total bytes read.
    pub bytes_read: u64,
    /// Total bytes written.
    pub bytes_written: u64,
    /// Total rows scanned.
    pub rows_scanned: u64,
    /// Slowest operators in the statement.
    pub operators: Vec<Operator>,
}

/// Parses a DuckDB JSON profile into a [`StmtProfile`], returning `None` if `query` is absent.
#[must_use]
pub fn parse_profile(statement: usize, json: &serde_json::Value) -> Option<StmtProfile> {
    let q = json.get("query")?;
    let f = |v: Option<&serde_json::Value>| v.and_then(serde_json::Value::as_f64).unwrap_or(0.0);
    let n = |v: Option<&serde_json::Value>| v.and_then(serde_json::Value::as_u64).unwrap_or(0);
    Some(StmtProfile {
        statement,
        cpu_s: f(q.get("cpu_time")),
        wall_s: f(q.get("total_time")),
        peak_buffer_bytes: n(json.pointer("/system/peak_buffer_memory")),
        peak_temp_bytes: n(json.pointer("/system/peak_temp_dir_size")),
        bytes_read: n(json.pointer("/io/total_bytes_read")),
        bytes_written: n(json.pointer("/io/total_bytes_written")),
        rows_scanned: n(q.get("total_rows_scanned")),
        operators: top_operators(json),
    })
}

/// Aggregated profile metrics across all statements in a SQL step.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StepProfile {
    /// Total CPU seconds.
    pub cpu_s: f64,
    /// Total wall seconds.
    pub wall_s: f64,
    /// Maximum peak buffer memory across statements.
    pub peak_buffer_bytes: u64,
    /// Maximum peak spill size across statements.
    pub peak_temp_bytes: u64,
    /// Total bytes read.
    pub bytes_read: u64,
    /// Total bytes written.
    pub bytes_written: u64,
    /// Total rows scanned.
    pub rows_scanned: u64,
}

impl StepProfile {
    /// Aggregates a slice of [`StmtProfile`] values into a [`StepProfile`].
    #[must_use]
    pub fn of(stmts: &[StmtProfile]) -> Self {
        stmts.iter().fold(Self::default(), |a, s| Self {
            cpu_s: a.cpu_s + s.cpu_s,
            wall_s: a.wall_s + s.wall_s,
            peak_buffer_bytes: a.peak_buffer_bytes.max(s.peak_buffer_bytes),
            peak_temp_bytes: a.peak_temp_bytes.max(s.peak_temp_bytes),
            bytes_read: a.bytes_read + s.bytes_read,
            bytes_written: a.bytes_written + s.bytes_written,
            rows_scanned: a.rows_scanned + s.rows_scanned,
        })
    }

    /// Returns effective parallelism (`cpu_s / wall_s`), or `0.0` if `wall_s <= 0.0`.
    #[must_use]
    pub fn parallelism(&self) -> f64 {
        if self.wall_s > 0.0 {
            self.cpu_s / self.wall_s
        } else {
            0.0
        }
    }
}

/// Creates a `sql_step` tracing span, or [`tracing::Span::none`] when telemetry is inactive.
#[must_use]
pub fn step_span(step: &str, dir: &std::path::Path, memory: Option<&str>) -> tracing::Span {
    if !active() {
        return tracing::Span::none();
    }
    tracing::info_span!(
        "sql_step",
        step = step_base(step),
        step_name = step,
        dir = %dir.display(),
        memory_limit = memory.unwrap_or("default"),
        skipped = tracing::field::Empty,
        seconds = tracing::field::Empty,
        rows_out = tracing::field::Empty,
        duckdb_memory_peak_bytes = tracing::field::Empty,
        spill_peak_bytes = tracing::field::Empty,
        failed = tracing::field::Empty,
        cpu_time_s = tracing::field::Empty,
        wall_s = tracing::field::Empty,
        parallelism = tracing::field::Empty,
        bytes_read = tracing::field::Empty,
        bytes_written = tracing::field::Empty,
        rows_scanned = tracing::field::Empty,
    )
}

/// Emits step-level DuckDB metrics.
#[expect(
    clippy::cast_precision_loss,
    reason = "metric gauges/histograms represent byte counts as f64"
)]
pub fn step_metrics(step: &str, seconds: f64, mem: u64, spill: u64, profile: &StepProfile) {
    let s = step_base(step).to_string();
    if profile.wall_s > 0.0 {
        metrics::histogram!("pipeline_sql_step_cpu_seconds", "step" => s.clone())
            .record(profile.cpu_s);
        metrics::histogram!("pipeline_sql_step_parallelism", "step" => s.clone())
            .record(profile.parallelism());
    }
    metrics::counter!("pipeline_sql_step_bytes_read_total", "step" => s.clone())
        .increment(profile.bytes_read);
    metrics::counter!("pipeline_sql_step_bytes_written_total", "step" => s.clone())
        .increment(profile.bytes_written);
    metrics::histogram!("pipeline_sql_step_duration_seconds", "step" => s.clone()).record(seconds);
    metrics::histogram!("pipeline_sql_step_memory_peak_bytes", "step" => s.clone())
        .record(mem as f64);
    metrics::histogram!("pipeline_sql_step_spill_peak_bytes", "step" => s).record(spill as f64);
}
