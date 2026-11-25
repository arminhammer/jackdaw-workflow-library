Feature: Resumable and multi-source file fetching
  As a workflow author
  I want to fetch files from HTTP(S) mirrors or local paths into a destination
  So that workflows can reliably acquire inputs with resume, deduplication, and checksum verification

  Scenario: Fetch a single HTTP file
    Given a local HTTP server serving "/hello.txt" with body "hello world"
    When I run the FetchFile workflow from that URL to "out/hello.txt"
    Then the fetch succeeds with 11 bytes and skipped is false
    And the file "out/hello.txt" contains "hello world"

  Scenario: Skip fetching when the destination already exists
    Given a local HTTP server serving "/cached.txt" with body "fresh"
    And an existing file "out/cached.txt" containing "already-here"
    When I run the FetchFile workflow from that URL to "out/cached.txt"
    Then the fetch succeeds with 12 bytes and skipped is true
    And the file "out/cached.txt" contains "already-here"

  Scenario: Fetch from a fallback mirror when the primary URL fails
    Given a local HTTP server returning 503 on "/primary.bin" and serving "/mirror.bin" with body "mirror-payload"
    When I run the FetchFile workflow with primary and mirror URLs to "out/mirror.bin"
    Then the fetch succeeds with 14 bytes and skipped is false
    And the file "out/mirror.bin" contains "mirror-payload"

  Scenario: Verify SHA-256 checksum on fetched file
    Given a local HTTP server serving "/verified.txt" with body "verified-data"
    When I run the FetchFile workflow from that URL to "out/verified.txt" with the wrong sha256
    Then the fetch fails with a checksum error

  Scenario: Copy a local file via file:// URI
    Given an existing file "src/local.txt" containing "local-bytes"
    When I run the FetchFile workflow from file URI "src/local.txt" to "out/local.txt"
    Then the fetch succeeds with 11 bytes and skipped is false
    And the file "out/local.txt" contains "local-bytes"

  Scenario: Fetch a batch of files using FetchBatch
    Given a local HTTP server serving "/a.txt" with body "alpha" and "/b.txt" with body "beta"
    When I run the FetchBatch workflow for those two URLs into "batch/"
    Then the batch returns 2 fetched files
    And the file "batch/a.txt" contains "alpha"
    And the file "batch/b.txt" contains "beta"
