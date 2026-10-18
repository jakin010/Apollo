//! One storage contract, run against every backend.
//!
//! SQLite always runs (against a temp file). SurrealDB runs when
//! `APOLLO_TEST_SURREAL_URL` is set (e.g. `ws://127.0.0.1:8000`), with optional
//! `APOLLO_TEST_SURREAL_USER` / `APOLLO_TEST_SURREAL_PASS`; otherwise it is
//! skipped — unless `APOLLO_TEST_REQUIRE_SURREAL` is set (as in CI), which turns
//! the skip into a failure. It uses a fresh, uniquely named database in the
//! `apollo_test` namespace and removes that database afterwards, so it never
//! touches existing data.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use apollo_config::{SqliteConfig, SurrealdbConfig};
use apollo_domain::{
    Classification, Frame, Input, Item, ItemState, ModelOutput, ModelResult, ModelState,
    Prediction, Task, TaskState, Url,
};
use apollo_storage::{SqliteStorage, Storage, SurrealStorage};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn sample_task(id: &str) -> Task {
    Task {
        id: id.into(),
        state: TaskState::Queued,
        items: vec![Item {
            input: Input::Image(Url {
                main: "http://x/a.jpg".into(),
                fallback: Some("http://y/a.jpg".into()),
            }),
            models: vec!["general".into(), "nsfw".into()],
            pipeline: Some("moderation".into()),
            state: ItemState::Queued,
            results: BTreeMap::new(),
            error: None,
            retries: 0,
        }],
        owner: Some(format!("owner-of-{id}")),
    }
}

/// The full persistence contract the engine relies on.
async fn exercise(store: &dyn Storage) {
    store.migrate().await.unwrap();
    store.migrate().await.unwrap(); // idempotent

    // --- create + reconstruct
    store.create_task(&sample_task("t1")).await.unwrap();
    let t = store.get_task("t1").await.unwrap().unwrap();
    assert_eq!(t.state, TaskState::Queued);
    assert_eq!(t.items[0].pipeline.as_deref(), Some("moderation"));
    assert_eq!(t.items[0].results["general"].state, ModelState::Queued);
    assert_eq!(t.owner.as_deref(), Some("owner-of-t1"));
    assert!(store.get_task("missing").await.unwrap().is_none());
    // Ownerless tasks (submitted with auth disabled) round-trip as None.
    store
        .create_task(&Task {
            owner: None,
            ..sample_task("anon")
        })
        .await
        .unwrap();
    assert_eq!(store.get_task("anon").await.unwrap().unwrap().owner, None);

    // --- per-model results + frame checkpoints
    let out = ModelOutput::Classification(Classification {
        predictions: vec![Prediction {
            label: 7,
            score: 0.99,
        }],
    });
    store
        .upsert_model_result("t1", 0, "general", &ModelResult::done(out.clone()))
        .await
        .unwrap();
    store
        .upsert_model_result("t1", 0, "nsfw", &ModelResult::processing())
        .await
        .unwrap();
    for i in 0..3u32 {
        store
            .append_frame(
                "t1",
                0,
                "nsfw",
                &Frame {
                    timestamp: i as f64,
                    index: i,
                    classification: Classification::default(),
                },
            )
            .await
            .unwrap();
    }
    let frames = store.load_frames("t1", 0, "nsfw").await.unwrap();
    assert_eq!(
        frames.iter().map(|f| f.index).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    let t = store.get_task("t1").await.unwrap().unwrap();
    assert_eq!(t.items[0].results["general"].output, Some(out.clone()));
    assert_eq!(t.items[0].results["nsfw"].state, ModelState::Processing);

    // --- resume bookkeeping: attempts, and their release on graceful shutdown
    store
        .set_task_state("t1", TaskState::Processing)
        .await
        .unwrap();
    let incomplete = store.load_incomplete_tasks().await.unwrap();
    assert!(incomplete.iter().any(|t| t.id == "t1"));
    assert_eq!(store.increment_attempts("t1").await.unwrap(), 1);
    assert_eq!(store.increment_attempts("t1").await.unwrap(), 2);
    assert_eq!(store.release_resume_attempts().await.unwrap(), 1);
    assert_eq!(store.increment_attempts("t1").await.unwrap(), 2);
    store.set_item_retries("t1", 0, 2).await.unwrap();
    assert_eq!(
        store.get_task("t1").await.unwrap().unwrap().items[0].retries,
        2
    );

    // --- conditional cancel: active -> cancelled; finished stays finished
    store.create_task(&sample_task("t2")).await.unwrap();
    assert!(store.cancel_task_if_active("t2").await.unwrap());
    assert_eq!(
        store.get_task("t2").await.unwrap().unwrap().state,
        TaskState::Cancelled
    );
    assert!(!store.cancel_task_if_active("t2").await.unwrap());
    store
        .set_task_state("t1", TaskState::Completed)
        .await
        .unwrap();
    assert!(!store.cancel_task_if_active("t1").await.unwrap());
    assert_eq!(
        store.get_task("t1").await.unwrap().unwrap().state,
        TaskState::Completed
    );
    assert!(!store.cancel_task_if_active("missing").await.unwrap());

    // --- webhook delivery flag
    store
        .set_item_state("t1", 0, ItemState::Completed, None)
        .await
        .unwrap();
    let pending = store.items_pending_webhook().await.unwrap();
    assert!(
        pending
            .iter()
            .any(|p| p.task_id == "t1" && p.item_index == 0)
    );
    store.mark_webhook_delivered("t1", 0).await.unwrap();
    let pending = store.items_pending_webhook().await.unwrap();
    assert!(!pending.iter().any(|p| p.task_id == "t1"));

    // --- result cache + purge
    store.cache_store("h", "m", "main", &out).await.unwrap();
    store.url_cache_store("u", "m", "main", "h").await.unwrap();
    assert_eq!(
        store.cache_lookup("h", "m", "main", 0).await.unwrap(),
        Some(out.clone())
    );
    assert_eq!(
        store
            .url_cache_lookup("u", "m", "main", 0)
            .await
            .unwrap()
            .as_deref(),
        Some("h")
    );
    assert!(
        store
            .cache_lookup("h", "m", "main", now() + 3600)
            .await
            .unwrap()
            .is_none(),
        "entries older than the freshness bound are ignored"
    );
    assert_eq!(store.purge_cache_before(now() - 3600).await.unwrap(), 0);
    assert_eq!(store.purge_cache_before(now() + 10).await.unwrap(), 2);
    assert!(
        store
            .cache_lookup("h", "m", "main", 0)
            .await
            .unwrap()
            .is_none()
    );

    // --- retention: finished tasks (and their children) are purged
    store
        .set_task_state("anon", TaskState::Completed)
        .await
        .unwrap();
    assert_eq!(store.purge_finished_before(now() + 10).await.unwrap(), 3);
    assert!(store.get_task("t1").await.unwrap().is_none());
    assert!(store.load_frames("t1", 0, "nsfw").await.unwrap().is_empty());
}

#[tokio::test]
async fn sqlite_backend_contract() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteStorage::connect(&SqliteConfig {
        path: dir.path().join("t.db").to_string_lossy().into_owned(),
        ..Default::default()
    })
    .await
    .unwrap();
    exercise(&store).await;
}

#[tokio::test]
async fn surreal_backend_contract() {
    let Ok(url) = std::env::var("APOLLO_TEST_SURREAL_URL") else {
        assert!(
            std::env::var_os("APOLLO_TEST_REQUIRE_SURREAL").is_none(),
            "APOLLO_TEST_REQUIRE_SURREAL is set but APOLLO_TEST_SURREAL_URL is not"
        );
        eprintln!("skipping: set APOLLO_TEST_SURREAL_URL to run against SurrealDB");
        return;
    };
    let user = std::env::var("APOLLO_TEST_SURREAL_USER").ok();
    let password = std::env::var("APOLLO_TEST_SURREAL_PASS").ok();
    let database = format!(
        "t{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let cfg = SurrealdbConfig {
        url: url.clone(),
        namespace: "apollo_test".into(),
        database: database.clone(),
        user: user.clone(),
        password: password.clone(),
    };
    let store = SurrealStorage::connect(&cfg).await.unwrap();

    // Run the contract, then always drop the throwaway database.
    let outcome = tokio::spawn(async move { exercise(&store).await }).await;

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
