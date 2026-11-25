//! BDD step definitions for `data` (DuckDB / SQL) workflows.
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value
)]

use std::collections::BTreeMap;

use cucumber::{given, then, when};
use jackdaw::testing::TestEngine;
use jackdaw_workflow_library::data::{
    QueryJson, QueryJsonInput, QueryJsonTask, RunSqlPipeline, RunSqlStep, RunSqlStepTask,
    SqlOutcome, SqlSource, SqlStep, SqlValue, render_sql,
};

use crate::steps::LibraryWorld;

async fn exec_sql_step(step: SqlStep) -> Result<SqlOutcome, String> {
    let engine = TestEngine::builder()
        .task(RunSqlStepTask)
        .workflow::<RunSqlStep>()
        .build()
        .map_err(|e| e.to_string())?;
    let handle = engine
        .execute(RunSqlStep::run, step)
        .await
        .map_err(|e| e.to_string())?;
    handle.await.map_err(|e| e.to_string())
}

async fn exec_sql_pipeline(steps: Vec<SqlStep>) -> Result<Vec<SqlOutcome>, String> {
    let engine = TestEngine::builder()
        .task(RunSqlStepTask)
        .workflow::<RunSqlStep>()
        .workflow::<RunSqlPipeline>()
        .build()
        .map_err(|e| e.to_string())?;
    let handle = engine
        .execute(RunSqlPipeline::run, steps)
        .await
        .map_err(|e| e.to_string())?;
    handle.await.map_err(|e| e.to_string())
}

async fn exec_query_json(
    input: QueryJsonInput,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, String> {
    let engine = TestEngine::builder()
        .task(QueryJsonTask)
        .workflow::<QueryJson>()
        .build()
        .map_err(|e| e.to_string())?;
    let handle = engine
        .execute(QueryJson::run, input)
        .await
        .map_err(|e| e.to_string())?;
    handle.await.map_err(|e| e.to_string())
}

#[given(regex = r#"^a SQL template "([^"]+)"$"#)]
fn given_sql_template(world: &mut LibraryWorld, template: String) {
    world.sql_template = Some(template);
}

#[given(regex = r#"^a stale running marker for SQL step "([^"]+)" and an existing "([^"]+)"$"#)]
async fn given_stale_marker(world: &mut LibraryWorld, step_name: String, file: String) {
    let dir = world.work_dir();
    let logs = dir.join("logs");
    tokio::fs::create_dir_all(&logs).await.expect("mkdir logs");
    tokio::fs::write(logs.join(format!("{step_name}.running")), b"")
        .await
        .expect("write running marker");
    tokio::fs::write(dir.join(file), b"corrupt-partial")
        .await
        .expect("write stale output");
}

#[when(
    regex = r#"^I render the SQL template with NUM=(\d+), FILE="([^"]+)", FILES=\["([^"]+)", "([^"]+)"\]$"#
)]
fn when_render_sql(world: &mut LibraryWorld, num: String, file: String, f1: String, f2: String) {
    let tpl = world.sql_template.as_deref().expect("template");
    let mut vars = BTreeMap::new();
    vars.insert("NUM".into(), SqlValue::Raw(num));
    vars.insert("FILE".into(), SqlValue::Path(file));
    vars.insert("FILES".into(), SqlValue::Files(vec![f1, f2]));
    world.rendered_sql = Some(render_sql(tpl, &vars).expect("render_sql"));
}

#[when(
    regex = r#"^I run the RunSqlStep workflow "([^"]+)" producing "([^"]+)" with inline SQL "([^"]+)"$"#
)]
async fn when_run_sql_step(world: &mut LibraryWorld, name: String, out: String, sql: String) {
    let step = SqlStep {
        dir: world.work_dir(),
        name,
        sql: SqlSource::Inline(sql),
        vars: BTreeMap::new(),
        env: BTreeMap::new(),
        outputs: vec![out],
        memory: None,
        temp: None,
    };
    world.last_sql_step = Some(exec_sql_step(step).await);
}

#[when(
    regex = r#"^I run the RunSqlPipeline workflow with two chained SQL steps producing "([^"]+)" and "([^"]+)"$"#
)]
async fn when_run_sql_pipeline(world: &mut LibraryWorld, out1: String, out2: String) {
    let dir = world.work_dir();
    let s1 = SqlStep {
        dir: dir.clone(),
        name: "stage1".into(),
        sql: SqlSource::Inline(format!(
            "COPY (SELECT 10 AS v) TO '{out1}' (FORMAT parquet)"
        )),
        vars: BTreeMap::new(),
        env: BTreeMap::new(),
        outputs: vec![out1.clone()],
        memory: None,
        temp: None,
    };
    let s2 = SqlStep {
        dir,
        name: "stage2".into(),
        sql: SqlSource::Inline(format!(
            "COPY (SELECT v * 2 AS v FROM read_parquet('{out1}')) TO '{out2}' (FORMAT parquet)"
        )),
        vars: BTreeMap::new(),
        env: BTreeMap::new(),
        outputs: vec![out2],
        memory: None,
        temp: None,
    };
    world.last_sql_pipeline = Some(exec_sql_pipeline(vec![s1, s2]).await);
}

#[when(regex = r#"^I run the QueryJson workflow with SQL "([^"]+)"$"#)]
async fn when_query_json(world: &mut LibraryWorld, sql: String) {
    let input = QueryJsonInput {
        dir: world.work_dir(),
        sql,
    };
    world.last_json_rows = Some(exec_query_json(input).await);
}

#[then(regex = r#"^the rendered SQL is "([^"]+)"$"#)]
fn then_rendered_sql(world: &mut LibraryWorld, expected: String) {
    assert_eq!(world.rendered_sql.as_deref(), Some(expected.as_str()));
}

#[then(regex = r#"^the SQL step "([^"]+)" succeeds with skipped (true|false)$"#)]
fn then_sql_step_ok(world: &mut LibraryWorld, name: String, skipped: String) {
    let expected_skipped = skipped == "true";
    let res = world
        .last_sql_step
        .as_ref()
        .expect("sql step result")
        .as_ref()
        .expect("sql step succeeded");
    assert_eq!(res.step, name);
    assert_eq!(res.skipped, expected_skipped);
}

#[then(regex = r#"^the file "([^"]+)" exists in the workspace$"#)]
fn then_file_exists(world: &mut LibraryWorld, rel: String) {
    assert!(world.work_dir().join(rel).is_file());
}

#[then(regex = r"^the SQL pipeline returns (\d+) outcomes with both skipped (true|false)$")]
fn then_pipeline_ok(world: &mut LibraryWorld, count: usize, skipped: String) {
    let expected_skipped = skipped == "true";
    let res = world
        .last_sql_pipeline
        .as_ref()
        .expect("pipeline result")
        .as_ref()
        .expect("pipeline succeeded");
    assert_eq!(res.len(), count);
    assert!(res.iter().all(|o| o.skipped == expected_skipped));
}

#[then(regex = r#"^the JSON query returns 1 row with answer (\d+) and msg "([^"]+)"$"#)]
fn then_json_query_ok(world: &mut LibraryWorld, answer: i64, msg: String) {
    let rows = world
        .last_json_rows
        .as_ref()
        .expect("json rows")
        .as_ref()
        .expect("query succeeded");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("answer").and_then(serde_json::Value::as_i64),
        Some(answer)
    );
    assert_eq!(
        rows[0].get("msg").and_then(serde_json::Value::as_str),
        Some(msg.as_str())
    );
}
