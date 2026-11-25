//! Core download implementation and Jackdaw tasks/workflows for `fetch`.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use gosh_dl::protocol::ExpectedChecksum;
use gosh_dl::{DownloadEngine, DownloadEvent, DownloadOptions, EngineConfig};
use jackdaw::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors returned by [`fetch_file`] and [`FetchFile`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum FetchError {
    /// The caller provided an invalid [`FetchSpec`] (for example, no source URIs).
    #[error("invalid fetch specification: {0}")]
    InvalidInput(String),
    /// A network or transient I/O error occurred; retrying may succeed.
    #[error("retryable fetch error: {0}")]
    Retryable(String),
    /// A permanent error occurred (such as a checksum mismatch or missing local file).
    #[error("fetch failed: {0}")]
    Fatal(String),
}

impl FetchError {
    /// Converts this error into a [`TaskError`], preserving retryability.
    #[must_use]
    pub fn into_task_error(self) -> TaskError {
        match self {
            Self::Retryable(msg) => TaskError::retryable(msg),
            Self::InvalidInput(msg) | Self::Fatal(msg) => TaskError::non_retryable(msg),
        }
    }
}

/// Specification for fetching a single file from one or more sources.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FetchSpec {
    /// Source URIs: `http(s)://` URLs (primary first, additional entries used as fallback/striped
    /// mirrors), or a single `file://` URI or local filesystem path.
    pub sources: Vec<String>,
    /// Destination file path.
    pub dest: PathBuf,
    /// Optional expected lowercase hex SHA-256 digest.
    #[serde(default)]
    pub sha256: Option<String>,
    /// Maximum time allowed for the transfer in seconds.
    #[serde(default = "default_timeout_s")]
    pub timeout_s: f64,
    /// Optional HTTP `User-Agent` header.
    #[serde(default)]
    pub user_agent: Option<String>,
    /// Whether to re-fetch even if `dest` already exists.
    #[serde(default)]
    pub overwrite: bool,
    /// Maximum concurrent HTTP connections for segmented downloads.
    #[serde(default)]
    pub max_connections: Option<usize>,
}

const fn default_timeout_s() -> f64 {
    300.0
}

impl FetchSpec {
    /// Creates a [`FetchSpec`] for a single source URI and destination path.
    #[must_use]
    pub fn new(source: impl Into<String>, dest: impl Into<PathBuf>) -> Self {
        Self {
            sources: vec![source.into()],
            dest: dest.into(),
            sha256: None,
            timeout_s: default_timeout_s(),
            user_agent: None,
            overwrite: false,
            max_connections: None,
        }
    }

    /// Adds mirror URLs to use alongside the primary source.
    #[must_use]
    pub fn with_mirrors(mut self, mirrors: impl IntoIterator<Item = String>) -> Self {
        self.sources.extend(mirrors);
        self
    }

    /// Sets an expected SHA-256 hex digest for post-download verification.
    #[must_use]
    pub fn with_sha256(mut self, sha256: impl Into<String>) -> Self {
        self.sha256 = Some(sha256.into());
        self
    }
}

/// Result of a completed [`FetchFile`] operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Fetched {
    /// Source URIs configured for the fetch.
    pub sources: Vec<String>,
    /// Destination path where the file now resides.
    pub path: PathBuf,
    /// File size in bytes.
    pub bytes: u64,
    /// `true` if `dest` already existed and was reused without transferring.
    pub skipped: bool,
    /// Elapsed wall-clock seconds for this operation.
    pub seconds: f64,
}

fn local_path(uri: &str) -> Option<PathBuf> {
    if let Some(rest) = uri.strip_prefix("file://") {
        return Some(PathBuf::from(rest));
    }
    if uri.starts_with("http://") || uri.starts_with("https://") {
        None
    } else {
        Some(PathBuf::from(uri))
    }
}

fn part_path(dest: &Path) -> PathBuf {
    let mut os = dest.as_os_str().to_os_string();
    os.push(".part");
    PathBuf::from(os)
}

async fn copy_local(src: &Path, dest: &Path) -> Result<u64, FetchError> {
    let part = part_path(dest);
    let bytes = tokio::fs::copy(src, &part)
        .await
        .map_err(|e| FetchError::Fatal(format!("copying {}: {e}", src.display())))?;
    if let Err(e) = tokio::fs::rename(&part, dest).await {
        let _ = tokio::fs::remove_file(&part).await;
        return Err(FetchError::Retryable(format!(
            "moving {} into {}: {e}",
            part.display(),
            dest.display()
        )));
    }
    Ok(bytes)
}

async fn download_http(
    spec: &FetchSpec,
    primary: &str,
    mirrors: &[String],
) -> Result<u64, FetchError> {
    let parent = spec
        .dest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = spec
        .dest
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| {
            FetchError::InvalidInput(format!(
                "destination has no file name: {}",
                spec.dest.display()
            ))
        })?;
    let part_name = format!("{file_name}.part");
    let part = parent.join(&part_name);

    let mut cfg = EngineConfig {
        download_dir: parent.to_path_buf(),
        ..Default::default()
    };
    if let Some(conns) = spec.max_connections {
        cfg.max_connections_per_download = conns.max(1);
    }
    if let Some(ua) = &spec.user_agent {
        cfg.user_agent.clone_from(ua);
    }

    let engine = DownloadEngine::new(cfg)
        .await
        .map_err(|e| FetchError::Fatal(format!("initializing download engine: {e}")))?;
    let mut events = engine.subscribe();

    let mut opts = DownloadOptions::new().save_dir(parent).filename(&part_name);
    opts.mirrors = mirrors.to_vec();
    opts.user_agent.clone_from(&spec.user_agent);
    opts.max_connections = spec.max_connections;
    if let Some(hash) = &spec.sha256 {
        opts.checksum = Some(ExpectedChecksum::sha256(hash));
    }

    let id = engine
        .add_http(primary, opts)
        .await
        .map_err(|e| FetchError::Retryable(format!("starting download {primary}: {e}")))?;

    let timeout = Duration::from_secs_f64(spec.timeout_s.max(1.0));
    let wait_res = tokio::time::timeout(timeout, async {
        loop {
            match events.recv().await {
                Ok(DownloadEvent::Completed { id: ev_id }) if ev_id == id => return Ok(()),
                Ok(DownloadEvent::Failed {
                    id: ev_id,
                    error,
                    retryable,
                }) if ev_id == id => {
                    return if retryable {
                        Err(FetchError::Retryable(error))
                    } else {
                        Err(FetchError::Fatal(error))
                    };
                }
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(FetchError::Retryable(
                        "download event stream closed unexpectedly".into(),
                    ));
                }
            }
        }
    })
    .await;

    let _ = engine.shutdown().await;

    match wait_res {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            let _ = tokio::fs::remove_file(&part).await;
            return Err(err);
        }
        Err(_) => {
            return Err(FetchError::Retryable(format!(
                "download timed out after {:.1}s: {primary}",
                spec.timeout_s
            )));
        }
    }

    tokio::fs::rename(&part, &spec.dest)
        .await
        .map_err(|e| FetchError::Retryable(format!("moving {} into place: {e}", part.display())))?;

    let meta = tokio::fs::metadata(&spec.dest)
        .await
        .map_err(|e| FetchError::Retryable(format!("stat {}: {e}", spec.dest.display())))?;
    Ok(meta.len())
}

/// Fetches a file according to `spec`, skipping if `spec.dest` already exists and
/// `spec.overwrite` is `false`.
///
/// # Errors
///
/// Returns [`FetchError::InvalidInput`] if `spec.sources` is empty or `spec.dest` has no file
/// name, [`FetchError::Retryable`] for transient transfer/I/O failures, and [`FetchError::Fatal`]
/// for checksum mismatches or missing local files.
pub async fn fetch_file(spec: &FetchSpec) -> Result<Fetched, FetchError> {
    let Some(primary) = spec.sources.first() else {
        return Err(FetchError::InvalidInput(
            "at least one source URI is required".into(),
        ));
    };

    if !spec.overwrite
        && let Ok(meta) = tokio::fs::metadata(&spec.dest).await
        && meta.is_file()
    {
        return Ok(Fetched {
            sources: spec.sources.clone(),
            path: spec.dest.clone(),
            bytes: meta.len(),
            skipped: true,
            seconds: 0.0,
        });
    }

    let started = Instant::now();
    if let Some(parent) = spec.dest.parent().filter(|p| !p.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| FetchError::Retryable(format!("creating {}: {e}", parent.display())))?;
    }

    let bytes = if let Some(src) = local_path(primary) {
        copy_local(&src, &spec.dest).await?
    } else {
        let mirrors = spec.sources.get(1..).unwrap_or(&[]);
        download_http(spec, primary, mirrors).await?
    };

    Ok(Fetched {
        sources: spec.sources.clone(),
        path: spec.dest.clone(),
        bytes,
        skipped: false,
        seconds: started.elapsed().as_secs_f64(),
    })
}

/// Jackdaw task that executes [`fetch_file`].
///
/// # Errors
///
/// Returns a [`TaskError`] (retryable or non-retryable) if fetching fails.
#[task(name = "jwl.fetch_file")]
pub async fn fetch_file_task(spec: FetchSpec) -> Result<Fetched, TaskError> {
    fetch_file(&spec).await.map_err(FetchError::into_task_error)
}

/// Durable Jackdaw workflow for fetching a single file from one or more sources.
#[workflow(namespace = "jwl", name = "fetch-file", version = "1.0.0")]
pub struct FetchFile;

/// Durable Jackdaw workflow that fetches a batch of files by composing [`FetchFile`] child
/// workflows.
#[workflow(namespace = "jwl", name = "fetch-batch", version = "1.0.0")]
pub struct FetchBatch;

mod workflow_impls {
    #![allow(missing_docs)]
    use super::{FetchBatch, FetchFile, FetchFileTask, FetchSpec, Fetched};
    use jackdaw::prelude::*;

    #[workflow_methods]
    impl FetchFile {
        /// Runs [`FetchFileTask`] with `spec`.
        #[run]
        pub async fn run(
            ctx: &mut WorkflowContext<Self>,
            spec: FetchSpec,
        ) -> WorkflowResult<Fetched> {
            ctx.call(FetchFileTask, &spec).await
        }
    }

    #[workflow_methods]
    impl FetchBatch {
        /// Fetches each [`FetchSpec`] in `specs` via [`FetchFile`] and returns all [`Fetched`]
        /// results in order.
        #[run]
        pub async fn run(
            ctx: &mut WorkflowContext<Self>,
            specs: Vec<FetchSpec>,
        ) -> WorkflowResult<Vec<Fetched>> {
            let mut out = Vec::with_capacity(specs.len());
            for spec in &specs {
                out.push(ctx.run(FetchFile::run, spec).await?);
            }
            Ok(out)
        }
    }
}

/// Registers all `fetch` tasks and workflows onto `builder`.
#[must_use]
pub fn register(builder: EngineBuilder) -> EngineBuilder {
    builder
        .task(FetchFileTask)
        .workflow::<FetchFile>()
        .workflow::<FetchBatch>()
}
