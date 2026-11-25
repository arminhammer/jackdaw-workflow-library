#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(feature = "fetch"), allow(rustdoc::broken_intra_doc_links))]
//! Reusable, composable workflows for the [Jackdaw](https://docs.rs/jackdaw-wf) durable workflow engine.
//!
//! | Feature | Module | What it provides |
//! |---|---|---|
//! | `fetch` | [`fetch`] | Multi-source, resumable HTTP(S) and local file download workflows backed by `gosh-dl`. |
//! | `full` | — | Enables all workflow categories. |

#[cfg(feature = "fetch")]
#[cfg_attr(docsrs, doc(cfg(feature = "fetch")))]
pub mod fetch;

#[cfg(feature = "fetch")]
use jackdaw::prelude::EngineBuilder;

/// Registers all enabled tasks and workflows from this library onto `builder`.
#[cfg(feature = "fetch")]
#[must_use]
pub fn register(builder: EngineBuilder) -> EngineBuilder {
    fetch::register(builder)
}
