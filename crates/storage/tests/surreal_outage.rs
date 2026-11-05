//! SurrealDB while the server can't be reached. Starting fails with
//! `StorageError::Timeout` after `[database].timeout` instead of hanging. A call
//! made meanwhile waits, and goes through once the server is back: background
//! work relies on that, so none of its writes is dropped. (Clients' calls get the
//! limit in the engine.)
//!
//! Runs against the `ws://` server named by `APOLLO_TEST_SURREAL_URL` (with
//! optional `APOLLO_TEST_SURREAL_USER` / `APOLLO_TEST_SURREAL_PASS`), through a
//! proxy in the test that can hold or cut the traffic. Skipped without one,
//! unless `APOLLO_TEST_REQUIRE_SURREAL` is set. Uses a fresh database in the
//! `apollo_test` namespace and removes it afterwards.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use apollo_config::{Backend, DatabaseConfig, SurrealdbConfig};
use apollo_domain::{Input, Item, Task, Url};
use apollo_storage::StorageError;

/// The time limit the test runs with.
const LIMIT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq)]
enum Mode {
    /// Pass the traffic through.
    Forward,
    /// Hold it, keeping the connections open: a host that vanished without
    /// closing them.
    Hold,
    /// Close the connections and refuse new ones: a server that's down.
    Cut,
}

/// A TCP proxy to the server, whose traffic the test can hold or cut.
struct Proxy {
    addr: SocketAddr,
    mode: Arc<watch::Sender<Mode>>,
    refused: Arc<AtomicUsize>,
}

impl Proxy {
    async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mode = Arc::new(watch::channel(Mode::Forward).0);
        let refused = Arc::new(AtomicUsize::new(0));
        let (modes, refusals) = (mode.clone(), refused.clone());
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                if *modes.borrow() == Mode::Cut {
                    refusals.fetch_add(1, Ordering::SeqCst);
                    continue;
                }
                tokio::spawn(relay(client, upstream.clone(), modes.subscribe()));
            }
        });
        Self {
            addr,
            mode,
            refused,
        }
    }

    fn set(&self, mode: Mode) {
        self.mode.send_replace(mode);
    }

    /// Wait until the client has tried to reconnect and been refused, so the
    /// SDK is in its reconnect loop.
    async fn wait_refused(&self) {
        let start = self.refused.load(Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.refused.load(Ordering::SeqCst) == start {
            assert!(
                Instant::now() < deadline,
                "the client never tried to reconnect"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// Pass one connection through until either side closes or the mode is `Cut`.
async fn relay(client: TcpStream, upstream: String, mode: watch::Receiver<Mode>) {
    let Ok(server) = TcpStream::connect(&upstream).await else {
        return;
    };
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    tokio::select! {
        _ = pump(client_read, server_write, mode.clone()) => {}
        _ = pump(server_read, client_write, mode) => {}
    }
}

/// Copy `from` to `to`, holding the bytes while the mode is `Hold`.
async fn pump(mut from: OwnedReadHalf, mut to: OwnedWriteHalf, mut mode: watch::Receiver<Mode>) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        if !open(&mut mode).await {
            return;
        }
        tokio::select! {
            read = from.read(&mut buf) => {
                let n = match read {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                if !open(&mut mode).await || to.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
            // A change of mode: check it again.
            _ = mode.changed() => {}
        }
    }
}

/// Wait while the mode is `Hold`: true once it's `Forward`, false on `Cut`.
async fn open(mode: &mut watch::Receiver<Mode>) -> bool {
    loop {
        match *mode.borrow_and_update() {
            Mode::Forward => return true,
            Mode::Cut => return false,
            Mode::Hold => {}
        }
        if mode.changed().await.is_err() {
            return false;
        }
    }
}

/// `call`'s result and how long it took. Fails the test if `call` is still
/// waiting long after `within`.
async fn timed<T>(within: Duration, call: impl Future<Output = T>) -> (T, Duration) {
    let start = Instant::now();
    let out = tokio::time::timeout(within * 5, call)
        .await
        .expect("still waiting long after it should have finished");
    (out, start.elapsed())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn starting_times_out_and_calls_wait_out_an_outage() {
    let Ok(url) = std::env::var("APOLLO_TEST_SURREAL_URL") else {
        assert!(
            std::env::var_os("APOLLO_TEST_REQUIRE_SURREAL").is_none(),
            "APOLLO_TEST_REQUIRE_SURREAL is set but APOLLO_TEST_SURREAL_URL is not"
        );
        eprintln!("skipping: set APOLLO_TEST_SURREAL_URL to run against SurrealDB");
        return;
    };
    // The proxy passes plain TCP, so a TLS certificate wouldn't match it.
    let Some(upstream) = url.strip_prefix("ws://") else {
        eprintln!("skipping: the outage test needs a ws:// APOLLO_TEST_SURREAL_URL");
        return;
    };
    let upstream = upstream.split('/').next().unwrap().to_string();
    let user = std::env::var("APOLLO_TEST_SURREAL_USER").ok();
    let password = std::env::var("APOLLO_TEST_SURREAL_PASS").ok();
    let database = format!(
        "outage{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let proxy = Proxy::start(upstream).await;
    let config = DatabaseConfig {
        backend: Backend::Surrealdb,
        surrealdb: Some(SurrealdbConfig {
            url: format!("ws://{}", proxy.addr),
            namespace: "apollo_test".into(),
            database: database.clone(),
            user: user.clone(),
            password: password.clone(),
        }),
        timeout: LIMIT.as_secs() as u32,
        ..Default::default()
    };
    let outcome = tokio::spawn(async move {
        // A host that accepts the connection but never answers.
        proxy.set(Mode::Hold);
        let (out, took) = timed(LIMIT, apollo_storage::open(&config)).await;
        assert!(
            matches!(out, Err(StorageError::Timeout(LIMIT))),
            "starting against a silent server: expected a timeout, got {:?}",
            out.err()
        );
        assert!(
            took >= LIMIT && took < LIMIT * 3,
            "timed out after {took:?}, for a {LIMIT:?} limit"
        );

        proxy.set(Mode::Forward);
        let store = apollo_storage::open(&config).await.unwrap();
        assert!(store.get_task("absent").await.unwrap().is_none());

        // A server that's down: the SDK keeps trying to reconnect meanwhile,
        // and a call waits for it rather than failing.
        proxy.set(Mode::Cut);
        proxy.wait_refused().await;
        let task = Task::new(
            "outage-task",
            vec![Item::new(
                Input::Image(Url {
                    main: "http://x/a.jpg".into(),
                    fallback: None,
                }),
                vec!["nsfw".into()],
                None,
            )],
            None,
        );
        let write = tokio::spawn({
            let (store, task) = (store.clone(), task.clone());
            async move { store.create_task(&task).await }
        });
        tokio::time::sleep(LIMIT * 2).await;
        assert!(!write.is_finished(), "the write didn't wait for the server");
        // Once it's back the SDK reconnects, signs in again, and the write goes
        // through.
        proxy.set(Mode::Forward);
        let (out, _) = timed(Duration::from_secs(5), write).await;
        out.unwrap()
            .expect("the write went through once the server was back");
        let stored = store.get_task(&task.id).await.unwrap();
        assert_eq!(stored.map(|t| t.id), Some(task.id));
    })
    .await;

    // Always drop the throwaway database, straight to the server.
    let admin = surrealdb::engine::any::connect(url).await.unwrap();
    if let (Some(username), Some(password)) = (user, password) {
        admin
            .signin(surrealdb::opt::auth::Root { username, password })
            .await
            .unwrap();
    }
    admin.use_ns("apollo_test").await.unwrap();
    admin
        .query(format!("REMOVE DATABASE IF EXISTS `{database}`"))
        .await
        .unwrap()
        .check()
        .unwrap();

    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic.into_panic());
    }
}
