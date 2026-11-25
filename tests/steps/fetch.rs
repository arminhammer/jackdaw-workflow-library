//! BDD step definitions for `fetch` workflows.
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value
)]

use std::collections::HashMap;
use std::sync::Arc;

use cucumber::{given, then, when};
use jackdaw::testing::TestEngine;
use jackdaw_workflow_library::fetch::{FetchBatch, FetchFile, FetchFileTask, FetchSpec, Fetched};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::steps::LibraryWorld;

/// Minimal in-process HTTP server for offline BDD tests.
pub struct TestHttpServer {
    pub base_url: String,
    _handle: tokio::task::JoinHandle<()>,
}

impl TestHttpServer {
    pub async fn start(routes: HashMap<String, (u16, Vec<u8>)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let routes = Arc::new(routes);
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let routes = Arc::clone(&routes);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let mut first_line = req.lines().next().unwrap_or_default().split_whitespace();
                    let method = first_line.next().unwrap_or("GET");
                    let path = first_line.next().unwrap_or("/");

                    let (status, body) = match routes.get(path) {
                        Some((code, data)) => (*code, data.as_slice()),
                        None => (404, b"not found".as_slice()),
                    };
                    let reason = match status {
                        200 => "OK",
                        503 => "Service Unavailable",
                        _ => "Not Found",
                    };
                    let header = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(header.as_bytes()).await;
                    if method != "HEAD" {
                        let _ = socket.write_all(body).await;
                    }
                    let _ = socket.shutdown().await;
                });
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            _handle: handle,
        }
    }
}

async fn run_fetch_file(spec: FetchSpec) -> Result<Fetched, String> {
    let engine = TestEngine::builder()
        .task(FetchFileTask)
        .workflow::<FetchFile>()
        .build()
        .map_err(|e| e.to_string())?;
    let handle = engine
        .execute(FetchFile::run, spec)
        .await
        .map_err(|e| e.to_string())?;
    handle.await.map_err(|e| e.to_string())
}

async fn run_fetch_batch(specs: Vec<FetchSpec>) -> Result<Vec<Fetched>, String> {
    let engine = TestEngine::builder()
        .task(FetchFileTask)
        .workflow::<FetchFile>()
        .workflow::<FetchBatch>()
        .build()
        .map_err(|e| e.to_string())?;
    let handle = engine
        .execute(FetchBatch::run, specs)
        .await
        .map_err(|e| e.to_string())?;
    handle.await.map_err(|e| e.to_string())
}

#[given(regex = r#"^a local HTTP server serving "([^"]+)" with body "([^"]+)"$"#)]
async fn given_http_file(world: &mut LibraryWorld, path: String, body: String) {
    let mut routes = HashMap::new();
    routes.insert(path.clone(), (200, body.into_bytes()));
    let srv = TestHttpServer::start(routes).await;
    world.last_urls = vec![format!("{}{path}", srv.base_url)];
    world.http_server = Some(srv);
}

#[given(
    regex = r#"^a local HTTP server returning 503 on "([^"]+)" and serving "([^"]+)" with body "([^"]+)"$"#
)]
async fn given_http_mirror(
    world: &mut LibraryWorld,
    primary: String,
    mirror: String,
    body: String,
) {
    let mut routes = HashMap::new();
    routes.insert(primary.clone(), (503, b"unavailable".to_vec()));
    routes.insert(mirror.clone(), (200, body.into_bytes()));
    let srv = TestHttpServer::start(routes).await;
    world.last_urls = vec![
        format!("{}{primary}", srv.base_url),
        format!("{}{mirror}", srv.base_url),
    ];
    world.http_server = Some(srv);
}

#[given(
    regex = r#"^a local HTTP server serving "([^"]+)" with body "([^"]+)" and "([^"]+)" with body "([^"]+)"$"#
)]
async fn given_http_two_files(
    world: &mut LibraryWorld,
    p1: String,
    b1: String,
    p2: String,
    b2: String,
) {
    let mut routes = HashMap::new();
    routes.insert(p1.clone(), (200, b1.into_bytes()));
    routes.insert(p2.clone(), (200, b2.into_bytes()));
    let srv = TestHttpServer::start(routes).await;
    world.last_urls = vec![
        format!("{}{p1}", srv.base_url),
        format!("{}{p2}", srv.base_url),
    ];
    world.http_server = Some(srv);
}

#[given(regex = r#"^an existing file "([^"]+)" containing "([^"]+)"$"#)]
async fn given_existing_file(world: &mut LibraryWorld, rel: String, content: String) {
    let path = world.work_dir().join(rel);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.expect("mkdir");
    }
    tokio::fs::write(&path, content.as_bytes())
        .await
        .expect("write");
}

#[when(regex = r#"^I run the FetchFile workflow from that URL to "([^"]+)"$"#)]
async fn when_fetch_single(world: &mut LibraryWorld, rel: String) {
    let url = world.last_urls[0].clone();
    let dest = world.work_dir().join(rel);
    world.last_fetch = Some(run_fetch_file(FetchSpec::new(url, dest)).await);
}

#[when(regex = r#"^I run the FetchFile workflow with primary and mirror URLs to "([^"]+)"$"#)]
async fn when_fetch_mirror(world: &mut LibraryWorld, rel: String) {
    let primary = world.last_urls[0].clone();
    let mirrors = world.last_urls[1..].to_vec();
    let dest = world.work_dir().join(rel);
    let spec = FetchSpec::new(primary, dest).with_mirrors(mirrors);
    world.last_fetch = Some(run_fetch_file(spec).await);
}

#[when(
    regex = r#"^I run the FetchFile workflow from that URL to "([^"]+)" with the wrong sha256$"#
)]
async fn when_fetch_bad_sha(world: &mut LibraryWorld, rel: String) {
    let url = world.last_urls[0].clone();
    let dest = world.work_dir().join(rel);
    let spec = FetchSpec::new(url, dest)
        .with_sha256("0000000000000000000000000000000000000000000000000000000000000000");
    world.last_fetch = Some(run_fetch_file(spec).await);
}

#[when(regex = r#"^I run the FetchFile workflow from file URI "([^"]+)" to "([^"]+)"$"#)]
async fn when_fetch_local(world: &mut LibraryWorld, src: String, dest: String) {
    let src_path = world.work_dir().join(src);
    let uri = format!("file://{}", src_path.display());
    let dest_path = world.work_dir().join(dest);
    world.last_fetch = Some(run_fetch_file(FetchSpec::new(uri, dest_path)).await);
}

#[when(regex = r#"^I run the FetchBatch workflow for those two URLs into "([^"]+)"$"#)]
async fn when_fetch_batch(world: &mut LibraryWorld, dir: String) {
    let dir_path = world.work_dir().join(dir);
    let specs: Vec<FetchSpec> = world
        .last_urls
        .iter()
        .map(|u| {
            let name = u.rsplit('/').next().unwrap_or("file");
            FetchSpec::new(u.clone(), dir_path.join(name))
        })
        .collect();
    world.last_batch = Some(run_fetch_batch(specs).await);
}

#[then(regex = r"^the fetch succeeds with (\d+) bytes and skipped is (true|false)$")]
fn then_fetch_ok(world: &mut LibraryWorld, bytes: u64, skipped: String) {
    let expected_skipped = skipped == "true";
    let res = world
        .last_fetch
        .as_ref()
        .expect("fetch result")
        .as_ref()
        .expect("fetch succeeded");
    assert_eq!(res.bytes, bytes);
    assert_eq!(res.skipped, expected_skipped);
}

#[then(regex = r"^the fetch fails with a checksum error$")]
fn then_fetch_checksum_err(world: &mut LibraryWorld) {
    let err = world
        .last_fetch
        .as_ref()
        .expect("fetch result")
        .as_ref()
        .expect_err("fetch should fail");
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("checksum") || lower.contains("hash") || lower.contains("mismatch"),
        "expected checksum error, got: {err}"
    );
}

#[then(regex = r"^the batch returns (\d+) fetched files$")]
fn then_batch_count(world: &mut LibraryWorld, n: usize) {
    let res = world
        .last_batch
        .as_ref()
        .expect("batch result")
        .as_ref()
        .expect("batch succeeded");
    assert_eq!(res.len(), n);
}

#[then(regex = r#"^the file "([^"]+)" contains "([^"]+)"$"#)]
async fn then_file_contains(world: &mut LibraryWorld, rel: String, expected: String) {
    let path = world.work_dir().join(rel);
    let got = tokio::fs::read_to_string(&path).await.expect("read");
    assert_eq!(got, expected);
}
