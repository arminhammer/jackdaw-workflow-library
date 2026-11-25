//! Resumable, multi-source file download workflows backed by [`gosh_dl`].
//!
//! - [`FetchFile`]: downloads a single file from one or more HTTP(S) mirror URLs (or copies a
//!   `file://` / local path) atomically into place, skipping if the destination already exists.
//! - [`FetchBatch`]: downloads multiple [`FetchSpec`] entries by composing [`FetchFile`] as child
//!   workflows.

pub mod download;

pub use download::{
    FetchBatch, FetchError, FetchFile, FetchFileTask, FetchSpec, Fetched, fetch_file, register,
};
