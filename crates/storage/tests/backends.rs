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
    Classification, Input, Item, ItemState, ModelOutput, ModelResult, ModelState, Prediction,
    Sample, Task, TaskState, Url,
};
use apollo_storage::{SqliteStorage, Storage, SurrealStorage, TaskQuery};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn sample_task(id: &str) -> Task {
    Task::new(
        id,
        vec![Item {
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
        Some(format!("owner-of-{id}")),
    )
}

fn query() -> TaskQuery {
    TaskQuery {
        owner: None,
        states: Vec::new(),
        created_after: None,
        created_before: None,
        limit: 50,
        after: None,
    }
}

/// The full persistence contract the engine relies on.
async fn exercise(store: &dyn Storage) {
    store.migrate().await.unwrap();
    store.migrate().await.unwrap(); // idempotent

    // --- create + reconstruct (metadata, idempotency key, timestamps)
    let mut t1 = sample_task("t1");
    t1.metadata.insert("record".into(), "42".into());
    t1.request_id = Some("req-1".into());
    store.create_task(&t1).await.unwrap();
    let t = store.get_task("t1").await.unwrap().unwrap();
    assert_eq!(t.state, TaskState::Queued);
    assert_eq!(t.metadata.get("record").map(String::as_str), Some("42"));
    assert_eq!(t.request_id.as_deref(), Some("req-1"));
    assert!(t.created_at > 0 && t.updated_at >= t.created_at);
    assert_eq!(t.finished_at, None);
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

    // --- idempotency key lookup is scoped to the owner
    assert_eq!(
        store
            .find_task_by_request(Some("owner-of-t1"), "req-1")
            .await
            .unwrap()
            .as_deref(),
        Some("t1")
    );
    assert!(
        store
            .find_task_by_request(Some("someone-else"), "req-1")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .find_task_by_request(None, "req-1")
            .await
            .unwrap()
            .is_none()
    );
    let mut anon2 = sample_task("anon2");
    anon2.owner = None;
    anon2.request_id = Some("req-1".into());
    store.create_task(&anon2).await.unwrap();
    assert_eq!(
        store
            .find_task_by_request(None, "req-1")
            .await
            .unwrap()
            .as_deref(),
        Some("anon2")
    );

    // --- per-model results + frame checkpoints
    let out = ModelOutput::Classification(Classification {
        predictions: vec![Prediction::new(7, "seven", 0.99)],
    });
    store
        .upsert_model_result("t1", 0, "general", &ModelResult::done(out.clone()))
        .await
        .unwrap();
    store
        .upsert_model_result("t1", 0, "nsfw", &ModelResult::processing())
        .await
        .unwrap();
    let first: Vec<Sample> = (0..3u32)
        .map(|i| Sample::frame(i, i as f64, Classification::default()))
        .collect();
    store.append_samples("t1", 0, "nsfw", &first).await.unwrap();
    // A second batch: replaces sample 2, adds 3 and 4.
    let batch: Vec<Sample> = (2..5u32)
        .map(|i| {
            Sample::frame(
                i,
                i as f64 + 0.5,
                Classification {
                    predictions: vec![Prediction::new(i, format!("label {i}"), 0.1)],
                },
            )
        })
        .collect();
    store.append_samples("t1", 0, "nsfw", &batch).await.unwrap();
    store.append_samples("t1", 0, "nsfw", &[]).await.unwrap();
    let samples = store.load_samples("t1", 0, "nsfw").await.unwrap();
    assert_eq!(
        samples.iter().map(|f| f.index).collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
    assert_eq!((samples[2].start, samples[2].end()), (2.5, 2.5));
    assert_eq!(samples[4].classification.predictions[0].label, 4);
    assert_eq!(samples[4].classification.predictions[0].name, "label 4");
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

    // --- counts by state (t1 processing, anon + anon2 queued)
    let mut counts = store.count_tasks_by_state().await.unwrap();
    counts.sort_by_key(|(s, _)| s.as_str());
    assert_eq!(
        counts,
        vec![(TaskState::Processing, 1), (TaskState::Queued, 2)]
    );

    // --- conditional cancel: active -> cancelled; finished stays finished
    store.create_task(&sample_task("t2")).await.unwrap();
    assert!(store.cancel_task_if_active("t2").await.unwrap());
    let t2 = store.get_task("t2").await.unwrap().unwrap();
    assert_eq!(t2.state, TaskState::Cancelled);
    assert!(t2.finished_at.is_some(), "a cancelled task is finished");
    assert!(!store.cancel_task_if_active("t2").await.unwrap());
    store
        .set_task_state("t1", TaskState::Completed)
        .await
        .unwrap();
    assert!(!store.cancel_task_if_active("t1").await.unwrap());
    let done = store.get_task("t1").await.unwrap().unwrap();
    assert_eq!(done.state, TaskState::Completed);
    let finished = done.finished_at.expect("a completed task is finished");
    // A later write keeps the first finish time.
    store
        .set_task_state("t1", TaskState::Completed)
        .await
        .unwrap();
    assert_eq!(
        store.get_task("t1").await.unwrap().unwrap().finished_at,
        Some(finished)
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

    // --- listing: newest first, paged, filtered, scoped to an owner
    for i in 1..=5 {
        let mut t = sample_task(&format!("p{i}"));
        t.owner = Some("lister".into());
        store.create_task(&t).await.unwrap();
    }
    store.set_task_state("p2", TaskState::Failed).await.unwrap();
    let mine = TaskQuery {
        owner: Some(Some("lister".into())),
        limit: 2,
        ..query()
    };
    let mut seen = Vec::new();
    let mut page = store.list_tasks(&mine).await.unwrap();
    loop {
        assert!(page.tasks.len() <= 2);
        seen.extend(page.tasks.iter().map(|t| t.id.clone()));
        let Some(next) = page.next.take() else { break };
        page = store
            .list_tasks(&TaskQuery {
                after: Some(next),
                ..mine.clone()
            })
            .await
            .unwrap();
    }
    assert_eq!(
        seen,
        vec!["p5", "p4", "p3", "p2", "p1"],
        "newest first, no gaps"
    );
    let failed = store
        .list_tasks(&TaskQuery {
            states: vec![TaskState::Failed],
            ..mine.clone()
        })
        .await
        .unwrap();
    assert_eq!(
        failed
            .tasks
            .iter()
            .map(|t| t.id.as_str())
            .collect::<Vec<_>>(),
        vec!["p2"]
    );
    assert!(failed.next.is_none());
    let anonymous = store
        .list_tasks(&TaskQuery {
            owner: Some(None),
            ..query()
        })
        .await
        .unwrap();
    let mut ids: Vec<&str> = anonymous.tasks.iter().map(|t| t.id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, vec!["anon", "anon2"]);
    let everyone = store.list_tasks(&query()).await.unwrap();
    assert!(everyone.tasks.len() >= 9);
    assert!(
        store
            .list_tasks(&TaskQuery {
                created_after: Some(now() + 3600),
                ..query()
            })
            .await
            .unwrap()
            .tasks
            .is_empty()
    );

    // --- retention: finished tasks (and their children) are purged
    store
        .set_task_state("anon", TaskState::Completed)
        .await
        .unwrap();
    // t1, t2, anon and p2 are finished.
    assert_eq!(store.purge_finished_before(now() + 10).await.unwrap(), 4);
    assert!(store.get_task("t1").await.unwrap().is_none());
    assert!(
        store
            .load_samples("t1", 0, "nsfw")
            .await
            .unwrap()
            .is_empty()
    );
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
