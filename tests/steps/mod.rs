//! Shared Cucumber `World` and step definitions for `jackdaw-workflow-library`.

use cucumber::World;

/// Isolated per-scenario test state.
#[derive(Debug, Default, World)]
pub struct LibraryWorld;
