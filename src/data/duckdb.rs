//! In-process DuckDB connection management, script execution, Arrow streaming, and
//! cancellation-safe async sessions.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub use duckdb;
pub use duckdb::arrow;
use duckdb::{Connection, InterruptHandle};

/// Errors returned by DuckDB operations in this module.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DuckError {
    /// Opening or configuring the connection failed.
    #[error("cannot open DuckDB: {0}")]
    Open(String),
    /// Installing or loading a DuckDB extension failed.
    #[error(
        "DuckDB extension {name} could not be installed or loaded: {message} (INSTALL needs the network once; extensions are cached in {cache})"
    )]
    Extension {
        /// Extension name.
        name: String,
        /// Underlying DuckDB error message.
        message: String,
        /// Extension cache directory path.
        cache: String,
    },
    /// A SQL statement failed.
    #[error("{message}\n  in statement: {statement}")]
    Sql {
        /// Truncated SQL statement that failed.
        statement: String,
        /// Underlying DuckDB error message.
        message: String,
    },
    /// A parameter name was not a valid SQL identifier.
    #[error("bad parameter name {0:?}: letters, digits and underscores only")]
    BadParam(String),
    /// The execution was interrupted or cancelled.
    #[error("DuckDB was interrupted")]
    Cancelled,
    /// A filesystem or I/O error occurred.
    #[error("{0}")]
    Io(String),
}

/// Convenience `Result` alias for [`DuckError`].
pub type DuckResult<T> = Result<T, DuckError>;

/// DuckDB extensions commonly used by data workflows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Extension {
    /// DuckDB `spatial` extension.
    Spatial,
    /// DuckDB `httpfs` extension.
    Httpfs,
    /// DuckDB `h3` community extension.
    H3,
}

impl Extension {
    /// Returns the extension name as used in `INSTALL`/`LOAD`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Spatial => "spatial",
            Self::Httpfs => "httpfs",
            Self::H3 => "h3",
        }
    }

    fn install_sql(self) -> String {
        match self {
            Self::H3 => "INSTALL h3 FROM community; LOAD h3;".into(),
            Self::Spatial | Self::Httpfs => format!("INSTALL {n}; LOAD {n};", n = self.name()),
        }
    }
}

/// Configuration for opening an in-memory DuckDB connection.
#[derive(Clone, Debug, Default)]
pub struct DuckConfig {
    /// Worker thread count (`SET threads`).
    pub threads: Option<usize>,
    /// Memory ceiling (e.g. `"8GB"`).
    pub memory_limit: Option<String>,
    /// Spill directory (`SET temp_directory`, created if missing).
    pub temp_directory: Option<PathBuf>,
    /// Directory where DuckDB caches downloaded extensions.
    pub extension_directory: Option<PathBuf>,
    /// Custom extension repository URL or local directory.
    pub extension_repository: Option<String>,
    /// Extensions to install and load immediately after opening.
    pub extensions: Vec<Extension>,
    /// Working directory for relative SQL paths (honored by [`run_session`]).
    pub dir: Option<PathBuf>,
}

impl DuckConfig {
    /// Creates a [`DuckConfig`] with [`DuckConfig::dir`] set to `dir`.
    #[must_use]
    pub fn in_dir(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: Some(dir.into()),
            ..Default::default()
        }
    }
}

/// Returns the extension cache directory path as a display string.
#[must_use]
pub fn extension_cache(cfg: &DuckConfig) -> String {
    cfg.extension_directory
        .clone()
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var("HOME").unwrap_or_default())
                .join(".duckdb")
                .join("extensions")
        })
        .display()
        .to_string()
}

static STARTS: Mutex<BTreeMap<PathBuf, usize>> = Mutex::new(BTreeMap::new());
static INSTALL: Mutex<()> = Mutex::new(());

/// Records a session start in `dir` (for diagnostics and testing).
pub fn note_start(dir: &Path) {
    *STARTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(dir.to_path_buf())
        .or_insert(0) += 1;
}

/// Returns how many times [`note_start`] was called for `dir`.
#[must_use]
pub fn starts_in(dir: &Path) -> usize {
    STARTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(dir)
        .copied()
        .unwrap_or(0)
}

fn sql_err(statement: &str, e: &duckdb::Error) -> DuckError {
    let message = e.to_string();
    if message.contains("INTERRUPT") {
        DuckError::Cancelled
    } else {
        DuckError::Sql {
            statement: shorten(statement),
            message,
        }
    }
}

fn shorten(sql: &str) -> String {
    let one: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if one.chars().count() > 300 {
        format!("{}...", one.chars().take(300).collect::<String>())
    } else {
        one
    }
}

/// Wraps `text` in SQL single quotes, doubling any embedded single quotes.
#[must_use]
pub fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Opens an in-memory DuckDB connection configured according to `cfg`.
///
/// # Errors
///
/// Returns [`DuckError::Open`], [`DuckError::Extension`], [`DuckError::Io`], or [`DuckError::Sql`]
/// if opening or configuring the connection fails.
pub fn connect(cfg: &DuckConfig) -> DuckResult<Connection> {
    let mut c = duckdb::Config::default();
    if let Some(dir) = &cfg.extension_directory {
        c = c
            .with("extension_directory", dir.display().to_string())
            .map_err(|e| DuckError::Open(e.to_string()))?;
    }
    let conn =
        Connection::open_in_memory_with_flags(c).map_err(|e| DuckError::Open(e.to_string()))?;
    if let Some(n) = cfg.threads {
        conn.execute_batch(&format!("SET threads = {n};"))
            .map_err(|e| sql_err("SET threads", &e))?;
    }
    if let Some(m) = &cfg.memory_limit {
        conn.execute_batch(&format!("SET memory_limit = {};", quote(m)))
            .map_err(|e| sql_err("SET memory_limit", &e))?;
    }
    if let Some(t) = &cfg.temp_directory {
        std::fs::create_dir_all(t).map_err(|e| DuckError::Io(format!("{}: {e}", t.display())))?;
        conn.execute_batch(&format!(
            "SET temp_directory = {};",
            quote(&t.display().to_string())
        ))
        .map_err(|e| sql_err("SET temp_directory", &e))?;
    }
    if let Some(repo) = &cfg.extension_repository {
        conn.execute_batch(&format!(
            "SET custom_extension_repository = {};",
            quote(repo)
        ))
        .map_err(|e| sql_err("SET custom_extension_repository", &e))?;
    }
    for ext in &cfg.extensions {
        let _guard = INSTALL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        conn.execute_batch(&ext.install_sql())
            .map_err(|e| DuckError::Extension {
                name: ext.name().into(),
                message: e.to_string(),
                cache: extension_cache(cfg),
            })?;
    }
    Ok(conn)
}

/// Returns the version string of the linked DuckDB library (for example, `"v2.0.0"`).
///
/// # Errors
///
/// Returns [`DuckError::Open`] or [`DuckError::Sql`] if querying the version fails.
pub fn library_version() -> DuckResult<String> {
    let conn = Connection::open_in_memory().map_err(|e| DuckError::Open(e.to_string()))?;
    conn.version().map_err(|e| sql_err("version", &e))
}

/// A parameter value bound into a DuckDB session via `SET VARIABLE`.
#[derive(Clone, Debug, PartialEq)]
pub enum Param {
    /// Text string.
    Text(String),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating-point number.
    Double(f64),
    /// Boolean value.
    Bool(bool),
}

impl From<&str> for Param {
    fn from(v: &str) -> Self {
        Self::Text(v.to_string())
    }
}
impl From<String> for Param {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}
impl From<i64> for Param {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}
impl From<f64> for Param {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}
impl From<bool> for Param {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

/// Named parameters bound as DuckDB session variables (`getvariable('name')`).
pub type Params = BTreeMap<String, Param>;

fn is_ident(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Binds each `(name, value)` pair in `params` via `SET VARIABLE`.
///
/// # Errors
///
/// Returns [`DuckError::BadParam`] if any key is not a valid identifier, or [`DuckError::Sql`]
/// if binding fails.
pub fn bind_params(conn: &Connection, params: &Params) -> DuckResult<()> {
    for (name, value) in params {
        if !is_ident(name) {
            return Err(DuckError::BadParam(name.clone()));
        }
        let stmt = format!("SET VARIABLE {name} = ?");
        let r = match value {
            Param::Text(v) => conn.execute(&stmt, [v]),
            Param::Int(v) => conn.execute(&stmt, [v]),
            Param::Double(v) => conn.execute(&stmt, [v]),
            Param::Bool(v) => conn.execute(&stmt, [v]),
        };
        r.map_err(|e| sql_err(&stmt, &e))?;
    }
    Ok(())
}

/// Splits a SQL script into non-empty statements on `;` outside quotes, comments, and `$$`
/// blocks.
#[must_use]
pub fn split_statements(sql: &str) -> Vec<String> {
    let b = sql.as_bytes();
    let (mut out, mut start, mut i) = (Vec::new(), 0, 0);
    let flush = |from: usize, to: usize, out: &mut Vec<String>| {
        let piece = sql.get(from..to).unwrap_or_default().trim();
        let has_code = piece.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_code {
            out.push(piece.to_string());
        }
    };
    while let Some(&byte) = b.get(i) {
        match byte {
            b'\'' | b'"' => {
                let q = byte;
                i += 1;
                while let Some(&inner) = b.get(i) {
                    if inner == q {
                        if b.get(i + 1) == Some(&q) {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    i += 1;
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while b.get(i).is_some_and(|&c| c != b'\n') {
                    i += 1;
                }
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b.get(i) == Some(&b'*') && b.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                i += 1;
            }
            b'$' if b.get(i + 1) == Some(&b'$') => {
                i += 2;
                while i + 1 < b.len() && !(b.get(i) == Some(&b'$') && b.get(i + 1) == Some(&b'$')) {
                    i += 1;
                }
                i += 1;
            }
            b';' => {
                flush(start, i, &mut out);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    flush(start, b.len().min(sql.len()), &mut out);
    out
}

/// Output captured from running a SQL script.
#[derive(Clone, Debug, Default)]
pub struct ScriptLog {
    /// Number of statements executed.
    pub statements: usize,
    /// Pretty-formatted result tables from row-returning statements.
    pub text: String,
    /// Stringified rows (up to [`LOG_ROWS`]) of the last row-returning statement.
    pub last_rows: Vec<Vec<String>>,
}

/// Maximum number of rows per statement formatted into [`ScriptLog`].
pub const LOG_ROWS: usize = 200;

/// Executes `sql` statement-by-statement on `conn` after binding `params`.
///
/// # Errors
///
/// Returns [`DuckError`] on parameter binding or statement execution failure.
pub fn run_script(conn: &Connection, sql: &str, params: &Params) -> DuckResult<ScriptLog> {
    run_script_profiled(conn, sql, params, None)
}

/// Executes `sql` statement-by-statement on `conn`, optionally collecting statement profiles.
///
/// # Errors
///
/// Returns [`DuckError`] on parameter binding, statement execution, or formatting failure.
pub fn run_script_profiled(
    conn: &Connection,
    sql: &str,
    params: &Params,
    mut profile: Option<&mut Vec<super::obs::StmtProfile>>,
) -> DuckResult<ScriptLog> {
    bind_params(conn, params)?;
    let mut log = ScriptLog::default();
    let prof_file = std::env::temp_dir().join(format!(
        "duckdb-profile-{}-{:p}.json",
        std::process::id(),
        conn
    ));
    if profile.is_some() {
        conn.execute_batch("SET enable_profiling = 'json'; SET profiling_coverage = 'ALL'")
            .map_err(|e| sql_err("SET enable_profiling", &e))?;
    }
    for stmt in split_statements(sql) {
        if profile.is_some() {
            let _ = std::fs::remove_file(&prof_file);
            conn.execute_batch(&format!(
                "SET profiling_output = {}",
                quote(&prof_file.display().to_string())
            ))
            .map_err(|e| sql_err("SET profiling_output", &e))?;
        }
        let mut prepared = conn.prepare(&stmt).map_err(|e| sql_err(&stmt, &e))?;
        let batches = prepared.stream_arrow([]).map_err(|e| sql_err(&stmt, &e))?;
        let mut kept = Vec::new();
        let mut rows = 0usize;
        let head = stmt.trim_start().to_ascii_lowercase();
        let prints = !(head.starts_with("copy")
            || head.starts_with("set ")
            || head.starts_with("create")
            || head.starts_with("insert")
            || head.starts_with("drop"));
        for b in batches {
            let b = b.map_err(|e| sql_err(&stmt, &e))?;
            if rows < LOG_ROWS {
                rows += b.num_rows();
                kept.push(b);
            }
        }
        if prints && kept.iter().any(|b| b.num_columns() > 0 && b.num_rows() > 0) {
            let table = arrow::util::pretty::pretty_format_batches(&kept)
                .map_err(|e| DuckError::Io(e.to_string()))?;
            log.text.push_str(&table.to_string());
            log.text.push('\n');
            log.last_rows = kept
                .iter()
                .flat_map(|b| {
                    (0..b.num_rows()).map(move |r| {
                        (0..b.num_columns())
                            .map(|c| {
                                arrow::util::display::array_value_to_string(b.column(c), r)
                                    .unwrap_or_default()
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
        }
        if let Some(out) = profile.as_deref_mut()
            && let Some(json) = std::fs::read(&prof_file)
                .ok()
                .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        {
            out.extend(super::obs::parse_profile(log.statements, &json));
        }
        log.statements += 1;
    }
    if profile.is_some() {
        let _ = std::fs::remove_file(&prof_file);
        let _ = conn.execute_batch("PRAGMA disable_profiling");
    }
    Ok(log)
}

/// Reads and executes a `.sql` file via [`run_script`].
///
/// # Errors
///
/// Returns [`DuckError::Io`] if the file cannot be read, or any [`DuckError`] from [`run_script`].
pub fn run_script_file(conn: &Connection, path: &Path, params: &Params) -> DuckResult<ScriptLog> {
    let sql = std::fs::read_to_string(path)
        .map_err(|e| DuckError::Io(format!("{}: {e}", path.display())))?;
    run_script(conn, &sql, params)
}

/// Streams Arrow [`arrow::record_batch::RecordBatch`]es for `sql` to `each` without collecting.
///
/// # Errors
///
/// Returns [`DuckError`] if preparing, executing, or handling a batch fails.
pub fn stream_batches(
    conn: &Connection,
    sql: &str,
    mut each: impl FnMut(arrow::record_batch::RecordBatch) -> DuckResult<()>,
) -> DuckResult<usize> {
    let mut stmt = conn.prepare(sql).map_err(|e| sql_err(sql, &e))?;
    let batches = stmt.stream_arrow([]).map_err(|e| sql_err(sql, &e))?;
    let mut rows = 0;
    for b in batches {
        let b = b.map_err(|e| sql_err(sql, &e))?;
        rows += b.num_rows();
        each(b)?;
    }
    Ok(rows)
}

/// Executes `COPY (<select>) TO '<path>' (FORMAT parquet, COMPRESSION <compression>)`.
///
/// # Errors
///
/// Returns [`DuckError::Io`] for invalid compression names, or [`DuckError::Sql`] on query
/// failure (removing any partial output file).
pub fn copy_to_parquet(
    conn: &Connection,
    select: &str,
    path: &Path,
    compression: &str,
) -> DuckResult<usize> {
    if !compression.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(DuckError::Io(format!("bad compression {compression:?}")));
    }
    let sql = format!(
        "COPY ({select}) TO {} (FORMAT parquet, COMPRESSION {compression})",
        quote(&path.display().to_string())
    );
    let n = conn
        .query_row(&sql, [], |r| r.get::<_, i64>(0))
        .map_err(|e| {
            let _ = std::fs::remove_file(path);
            sql_err(&sql, &e)
        })?;
    Ok(usize::try_from(n).unwrap_or(0))
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "duckdb::types::ValueRef is #[non_exhaustive]"
)]
fn json_of(v: duckdb::types::ValueRef<'_>) -> serde_json::Value {
    use duckdb::types::ValueRef as V;
    use serde_json::Value as J;
    match v {
        V::Null => J::Null,
        V::Boolean(b) => J::Bool(b),
        V::TinyInt(n) => J::from(n),
        V::SmallInt(n) => J::from(n),
        V::Int(n) => J::from(n),
        V::BigInt(n) => J::from(n),
        V::UTinyInt(n) => J::from(n),
        V::USmallInt(n) => J::from(n),
        V::UInt(n) => J::from(n),
        V::UBigInt(n) => J::from(n),
        V::HugeInt(n) => i64::try_from(n).map_or_else(|_| J::String(n.to_string()), J::from),
        V::UHugeInt(n) => u64::try_from(n).map_or_else(|_| J::String(n.to_string()), J::from),
        V::Float(f) => serde_json::Number::from_f64(f64::from(f)).map_or(J::Null, J::Number),
        V::Double(f) => serde_json::Number::from_f64(f).map_or(J::Null, J::Number),
        V::Text(t) => J::String(String::from_utf8_lossy(t).into_owned()),
        other => J::String(format!("{other:?}")),
    }
}

/// Executes `sql` and returns all rows as JSON object maps.
///
/// # Errors
///
/// Returns [`DuckError::Sql`] if preparing or reading rows fails.
pub fn query_json_rows(
    conn: &Connection,
    sql: &str,
) -> DuckResult<Vec<serde_json::Map<String, serde_json::Value>>> {
    let mut stmt = conn.prepare(sql).map_err(|e| sql_err(sql, &e))?;
    let mut rows = stmt.query([]).map_err(|e| sql_err(sql, &e))?;
    let names: Vec<String> = rows
        .as_ref()
        .map(duckdb::Statement::column_names)
        .unwrap_or_default();
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| sql_err(sql, &e))? {
        let mut m = serde_json::Map::new();
        for (i, n) in names.iter().enumerate() {
            m.insert(
                n.clone(),
                json_of(row.get_ref(i).map_err(|e| sql_err(sql, &e))?),
            );
        }
        out.push(m);
    }
    Ok(out)
}

/// Active DuckDB connection and interrupt handle passed to [`run_session`] closures.
pub struct Session<'a> {
    /// Open DuckDB connection.
    pub conn: &'a Connection,
    /// Handle used to interrupt an in-flight query.
    pub interrupt: Arc<InterruptHandle>,
}

#[cfg(target_os = "linux")]
mod cwd {
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard};

    static CWD_FALLBACK: Mutex<()> = Mutex::new(());

    pub struct Private {
        restore: Option<PathBuf>,
        _guard: Option<MutexGuard<'static, ()>>,
    }

    impl Drop for Private {
        fn drop(&mut self) {
            if let Some(p) = &self.restore {
                let _ = std::env::set_current_dir(p);
            }
        }
    }

    pub fn enter(dir: &Path) -> Result<Private, String> {
        // SAFETY: unshare(2) with CLONE_FS only detaches the calling thread's filesystem attributes.
        if unsafe { libc::unshare(libc::CLONE_FS) } == 0 {
            std::env::set_current_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
            return Ok(Private {
                restore: None,
                _guard: None,
            });
        }
        let err = std::io::Error::last_os_error();
        if matches!(err.raw_os_error(), Some(libc::ENOSYS | libc::EPERM)) {
            let guard = CWD_FALLBACK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let before = std::env::current_dir().ok();
            std::env::set_current_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
            return Ok(Private {
                restore: before,
                _guard: Some(guard),
            });
        }
        Err(format!("unshare: {err}"))
    }
}

#[cfg(not(target_os = "linux"))]
mod cwd {
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard};

    static CWD: Mutex<()> = Mutex::new(());

    pub struct Private(Option<PathBuf>, #[allow(dead_code)] MutexGuard<'static, ()>);

    impl Drop for Private {
        fn drop(&mut self) {
            if let Some(p) = &self.0 {
                let _ = std::env::set_current_dir(p);
            }
        }
    }

    pub fn enter(dir: &Path) -> Result<Private, String> {
        let guard = CWD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = std::env::current_dir().ok();
        std::env::set_current_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        Ok(Private(before, guard))
    }
}

fn session_thread<T: Send>(
    cfg: &DuckConfig,
    on_open: impl FnOnce(Arc<InterruptHandle>) + Send,
    f: impl FnOnce(&Session<'_>) -> DuckResult<T> + Send,
) -> DuckResult<T> {
    std::thread::scope(|s| {
        s.spawn(|| {
            let _cwd = match &cfg.dir {
                Some(d) => Some(cwd::enter(d).map_err(DuckError::Io)?),
                None => None,
            };
            let conn = connect(cfg)?;
            let interrupt = conn.interrupt_handle();
            on_open(Arc::clone(&interrupt));
            f(&Session {
                conn: &conn,
                interrupt,
            })
        })
        .join()
        .unwrap_or_else(|_| Err(DuckError::Io("the DuckDB thread panicked".into())))
    })
}

/// Runs `f` synchronously on a dedicated thread with a connection configured from `cfg`.
///
/// # Errors
///
/// Returns [`DuckError`] if opening the connection or running `f` fails.
pub fn run_session_blocking<T: Send>(
    cfg: &DuckConfig,
    f: impl FnOnce(&Session<'_>) -> DuckResult<T> + Send,
) -> DuckResult<T> {
    session_thread(cfg, |_| {}, f)
}

struct StopOnDrop {
    handle: Arc<Mutex<Option<Arc<InterruptHandle>>>>,
    cancelled: Arc<AtomicBool>,
    done: bool,
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        if !self.done {
            self.cancelled.store(true, Ordering::SeqCst);
            if let Some(h) = self
                .handle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
            {
                h.interrupt();
            }
        }
    }
}

/// Runs `f` on a dedicated thread without blocking the Tokio runtime, interrupting the DuckDB
/// connection if the returned future is dropped.
///
/// # Errors
///
/// Returns [`DuckError`] if opening the connection, running `f`, or joining the session thread
/// fails.
pub async fn run_session<T: Send + 'static>(
    cfg: DuckConfig,
    f: impl FnOnce(&Session<'_>, &AtomicBool) -> DuckResult<T> + Send + 'static,
) -> DuckResult<T> {
    let handle = Arc::new(Mutex::new(None));
    let cancelled = Arc::new(AtomicBool::new(false));
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let (handle, cancelled) = (Arc::clone(&handle), Arc::clone(&cancelled));
        std::thread::spawn(move || {
            let flag = Arc::clone(&cancelled);
            let slot = Arc::clone(&handle);
            let r = session_thread(
                &cfg,
                move |h| {
                    *slot
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&h));
                    if flag.load(Ordering::SeqCst) {
                        h.interrupt();
                    }
                },
                |s| f(s, &cancelled),
            );
            let _ = tx.send(r);
        });
    }
    let mut stop = StopOnDrop {
        handle,
        cancelled,
        done: false,
    };
    let r = rx
        .await
        .unwrap_or_else(|_| Err(DuckError::Io("the DuckDB thread vanished".into())));
    stop.done = true;
    r
}
