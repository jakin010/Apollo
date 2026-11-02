//! Test helpers for other crates' tests (the `test-util` feature): a throwaway
//! SurrealDB database, like the one the storage contract test uses.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;

use apollo_config::SurrealdbConfig;

use crate::{Storage, StorageError, SurrealStorage};

/// The namespace throwaway databases are created in.
pub const NAMESPACE: &str = "apollo_test";

/// A fresh, migrated database for one test, on the SurrealDB server named by
/// `APOLLO_TEST_SURREAL_URL` (signed in with `APOLLO_TEST_SURREAL_USER` /
/// `APOLLO_TEST_SURREAL_PASS` when set). It isn't removed on drop: call
/// [`SurrealScratch::remove`].
pub struct SurrealScratch {
    config: SurrealdbConfig,
    admin: Surreal<Any>,
}

impl SurrealScratch {
    /// `None` when `APOLLO_TEST_SURREAL_URL` isn't set, so the test can skip;
    /// a panic instead when `APOLLO_TEST_REQUIRE_SURREAL` is set.
    pub async fn create() -> Option<Self> {
        let scratch = Self::create_empty().await?;
        SurrealStorage::connect(&scratch.config)
            .await
            .expect("connecting to the test database")
            .migrate()
            .await
            .expect("migrating the test database");
        Some(scratch)
    }

    /// Like [`SurrealScratch::create`], without the schema: as a new
    /// database is before the first instance starts on it.
    pub async fn create_empty() -> Option<Self> {
        let Ok(url) = std::env::var("APOLLO_TEST_SURREAL_URL") else {
            assert!(
                std::env::var_os("APOLLO_TEST_REQUIRE_SURREAL").is_none(),
                "APOLLO_TEST_REQUIRE_SURREAL is set but APOLLO_TEST_SURREAL_URL is not"
            );
            eprintln!("skipping: set APOLLO_TEST_SURREAL_URL to run against SurrealDB");
            return None;
        };
        // The workspace links two rustls backends; a wss:// URL needs one picked.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config = SurrealdbConfig {
            url: url.clone(),
            namespace: NAMESPACE.into(),
            database: format!(
                "t{}_{nanos}_{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ),
            user: std::env::var("APOLLO_TEST_SURREAL_USER").ok(),
            password: std::env::var("APOLLO_TEST_SURREAL_PASS").ok(),
        };

        let admin = surrealdb::engine::any::connect(url)
            .await
            .expect("connecting to APOLLO_TEST_SURREAL_URL");
        if let (Some(username), Some(password)) = (&config.user, &config.password) {
            admin
                .signin(Root {
                    username: username.clone(),
                    password: password.clone(),
                })
                .await
                .expect("signing in to SurrealDB");
        }
        // Tests running in parallel define the namespace at the same moment,
        // which SurrealDB may refuse with a write conflict: try again.
        let define = format!(
            "DEFINE NAMESPACE IF NOT EXISTS {NAMESPACE}; USE NS {NAMESPACE}; \
             DEFINE DATABASE IF NOT EXISTS `{}`;",
            config.database
        );
        retry(|| async {
            admin.query(define.as_str()).await?.check()?;
            Ok(())
        })
        .await
        .expect("creating the test database");
        admin
            .use_ns(NAMESPACE)
            .use_db(config.database.clone())
            .await
            .expect("selecting the test database");
        Some(Self { config, admin })
    }

    /// The `[database.surrealdb]` section for this database.
    pub fn config(&self) -> SurrealdbConfig {
        self.config.clone()
    }

    /// A task's resume attempt count, which [`Storage`] doesn't read back.
    pub async fn attempts(&self, task_id: &str) -> u32 {
        let mut resp = self
            .admin
            .query("SELECT VALUE attempts FROM ONLY type::record('task', $tid)")
            .bind(("tid", task_id.to_string()))
            .await
            .expect("reading a task's attempts");
        let attempts: Option<i64> = resp.take(0).expect("reading a task's attempts");
        attempts.expect("the task row and its attempts field") as u32
    }

    /// Drop the database.
    pub async fn remove(&self) {
        self.admin
            .query(format!(
                "REMOVE DATABASE IF EXISTS `{}`",
                self.config.database
            ))
            .await
            .and_then(|resp| resp.check())
            .expect("removing the test database");
    }
}

/// `op`, tried up to five times, with a short pause between tries.
async fn retry<F, Fut>(mut op: F) -> Result<(), StorageError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), StorageError>>,
{
    let mut tries = 0u64;
    loop {
        match op().await {
            Ok(()) => return Ok(()),
            Err(e) if tries >= 4 => return Err(e),
            Err(_) => {
                tries += 1;
                // Spread the retries of tests that collided.
                let jitter = u64::from(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .subsec_nanos()
                        % 50,
                );
                tokio::time::sleep(Duration::from_millis(50 * tries + jitter)).await;
            }
        }
    }
}
