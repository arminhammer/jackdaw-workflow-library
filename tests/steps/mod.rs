//! Shared Cucumber `World` and step definitions for `jackdaw-workflow-library`.

pub mod fetch;

use std::path::PathBuf;

use cucumber::World;
use jackdaw_workflow_library::fetch::Fetched;
use tempfile::TempDir;

/// Isolated per-scenario test state.
#[derive(Default, World)]
pub struct LibraryWorld {
    /// Per-scenario temporary directory, cleaned up on drop.
    pub tmp: Option<TempDir>,
    /// Optional in-process HTTP server for offline `fetch` scenarios.
    pub http_server: Option<fetch::TestHttpServer>,
    /// URLs prepared by `Given` steps.
    pub last_urls: Vec<String>,
    /// Outcome of the last `FetchFile` workflow run.
    pub last_fetch: Option<Result<Fetched, String>>,
    /// Outcome of the last `FetchBatch` workflow run.
    pub last_batch: Option<Result<Vec<Fetched>, String>>,
}

impl std::fmt::Debug for LibraryWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LibraryWorld")
            .field("tmp", &self.tmp)
            .field("last_urls", &self.last_urls)
            .field("last_fetch", &self.last_fetch)
            .field("last_batch", &self.last_batch)
            .finish_non_exhaustive()
    }
}

impl LibraryWorld {
    /// Returns the scenario's temporary directory, creating it on first use.
    pub fn work_dir(&mut self) -> PathBuf {
        self.tmp
            .get_or_insert_with(|| tempfile::tempdir().expect("create tempdir"))
            .path()
            .to_path_buf()
    }
}
