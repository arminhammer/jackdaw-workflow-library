//! Cucumber BDD test runner for `jackdaw-workflow-library`.
//!
//! Run with:
//! `cargo test --features full --test bdd`
//!
//! Tag gates:
//! - `@todo`: skipped until implemented
//! - `@slow`: skipped unless `JWL_SLOW=1`
//! - `@network`: skipped unless `JWL_NETWORK=1`
//! - `@bundled`: skipped unless `duckdb-bundled` feature is active
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod steps;

use cucumber::World as _;

fn has_tag(tags: &[String], tag: &str) -> bool {
    tags.iter().any(|t| t.trim_start_matches('@') == tag)
}

fn enabled(var: &str) -> bool {
    std::env::var(var).is_ok_and(|v| v == "1")
}

fn skipped(tags: &[String]) -> bool {
    has_tag(tags, "todo")
        || (has_tag(tags, "slow") && !enabled("JWL_SLOW"))
        || (has_tag(tags, "network") && !enabled("JWL_NETWORK"))
}

#[tokio::main]
async fn main() {
    let concurrency = std::env::var("BDD_SCENARIO_CONCURRENCY")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(4);

    steps::LibraryWorld::cucumber()
        .max_concurrent_scenarios(concurrency)
        .fail_on_skipped()
        .filter_run_and_exit("tests/features", |feature, rule, scenario| {
            !(skipped(&feature.tags)
                || rule.is_some_and(|r| skipped(&r.tags))
                || skipped(&scenario.tags))
        })
        .await;
}
