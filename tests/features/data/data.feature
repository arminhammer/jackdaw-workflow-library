Feature: In-process DuckDB SQL step and pipeline workflows
  As a workflow author
  I want to run templated DuckDB SQL steps and compose them into pipelines
  So that data processing workflows are durable, idempotent, and cancellable

  Scenario: Render an @NAME@ SQL template with Raw, Path, and Files values
    Given a SQL template "SELECT @NUM@ AS n FROM read_parquet('@FILE@') UNION ALL SELECT 1 FROM read_parquet([@FILES@])"
    When I render the SQL template with NUM=42, FILE="a'b.parquet", FILES=["x.parquet", "y.parquet"]
    Then the rendered SQL is "SELECT 42 AS n FROM read_parquet('a''b.parquet') UNION ALL SELECT 1 FROM read_parquet(['x.parquet', 'y.parquet'])"

  Scenario: Run a SQL step workflow and skip on re-run when outputs exist
    When I run the RunSqlStep workflow "step1" producing "out.parquet" with inline SQL "COPY (SELECT 1 AS id, 'ok' AS label) TO 'out.parquet' (FORMAT parquet)"
    Then the SQL step "step1" succeeds with skipped false
    And the file "out.parquet" exists in the workspace
    When I run the RunSqlStep workflow "step1" producing "out.parquet" with inline SQL "INVALID SQL THAT SHOULD NOT RUN"
    Then the SQL step "step1" succeeds with skipped true

  Scenario: Redo a SQL step when a stale .running marker is present
    Given a stale running marker for SQL step "step2" and an existing "out2.parquet"
    When I run the RunSqlStep workflow "step2" producing "out2.parquet" with inline SQL "COPY (SELECT 99 AS id) TO 'out2.parquet' (FORMAT parquet)"
    Then the SQL step "step2" succeeds with skipped false

  Scenario: Compose multiple SQL steps using RunSqlPipeline
    When I run the RunSqlPipeline workflow with two chained SQL steps producing "stage1.parquet" and "stage2.parquet"
    Then the SQL pipeline returns 2 outcomes with both skipped false
    And the file "stage2.parquet" exists in the workspace

  Scenario: Query DuckDB and return JSON rows via QueryJson
    When I run the QueryJson workflow with SQL "SELECT 7 AS answer, 'hi' AS msg"
    Then the JSON query returns 1 row with answer 7 and msg "hi"
