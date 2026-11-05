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
    Revocation, Sample, Task, TaskError, TaskState, Url,
};
use apollo_storage::{
    ClaimMode, Delivery, Fence, Held, Lease, PendingWebhook, Renewal, SqliteStorage, Storage,
    StorageError, SurrealStorage, TaskCursor, TaskQuery,
};
use std::time::Duration;

/// A finishing write's delivery for planted rows: pending, any receiver.
const OPEN: Delivery<'static> = Delivery::Open { target: None };

/// A finishing write's delivery claimed by `n1/p1`, for `t://x`.
const CLAIM: Delivery<'static> = Delivery::Claim {
    holder: "n1/p1",
    target: "t://x",
    ttl: Duration::from_secs(30),
};

/// Real task ids are hyphenated UUIDs; one test task uses that shape.
const UUID_ID: &str = "3f2a8c1e-9b4d-4e1a-8f00-0123456789ab";

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
    store.create_task(&t1, None, None).await.unwrap();
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
        .create_task(
            &Task {
                owner: None,
                ..sample_task("anon")
            },
            None,
            None,
        )
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
    store.create_task(&anon2, None, None).await.unwrap();
    assert_eq!(
        store
            .find_task_by_request(None, "req-1")
            .await
            .unwrap()
            .as_deref(),
        Some("anon2")
    );

    // --- steps marker on a model with no result row yet: starts processing
    assert_eq!(store.steps_completed("t1", 0, "general").await.unwrap(), 0);
    store
        .set_steps_completed("t1", 0, "general", 5)
        .await
        .unwrap();
    assert_eq!(store.steps_completed("t1", 0, "general").await.unwrap(), 5);
    let t = store.get_task("t1").await.unwrap().unwrap();
    assert_eq!(t.items[0].results["general"].state, ModelState::Processing);

    // --- per-model results + frame checkpoints
    let out = ModelOutput::Classification(Classification {
        predictions: vec![Prediction::new(7, "seven", 0.99)],
    });
    store
        .upsert_model_result(
            "t1",
            0,
            "general",
            &ModelResult::done(out.clone()),
            Fence::Unchecked,
        )
        .await
        .unwrap();
    // A result write keeps the steps marker, and a marker write keeps the result.
    assert_eq!(store.steps_completed("t1", 0, "general").await.unwrap(), 5);
    store
        .set_steps_completed("t1", 0, "general", 6)
        .await
        .unwrap();
    assert_eq!(store.steps_completed("t1", 0, "general").await.unwrap(), 6);
    let t = store.get_task("t1").await.unwrap().unwrap();
    assert_eq!(t.items[0].results["general"].state, ModelState::Done);
    assert_eq!(t.items[0].results["general"].output, Some(out.clone()));
    store
        .upsert_model_result(
            "t1",
            0,
            "nsfw",
            &ModelResult::processing(),
            Fence::Unchecked,
        )
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

    // --- resume bookkeeping: attempts
    store
        .set_task_state("t1", TaskState::Processing, Fence::Unchecked)
        .await
        .unwrap();
    let incomplete = store.load_incomplete_tasks().await.unwrap();
    assert!(incomplete.iter().any(|t| t.id == "t1"));
    assert_eq!(store.increment_attempts("t1").await.unwrap(), 1);
    assert_eq!(store.increment_attempts("t1").await.unwrap(), 2);
    store
        .set_item_retries("t1", 0, 2, Fence::Unchecked)
        .await
        .unwrap();
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
    store
        .create_task(&sample_task(UUID_ID), None, None)
        .await
        .unwrap();
    assert!(store.cancel_task_if_active(UUID_ID).await.unwrap());
    let t2 = store.get_task(UUID_ID).await.unwrap().unwrap();
    assert_eq!(t2.state, TaskState::Cancelled);
    assert!(t2.finished_at.is_some(), "a cancelled task is finished");
    assert!(!store.cancel_task_if_active(UUID_ID).await.unwrap());
    store
        .set_task_state("t1", TaskState::Completed, Fence::Unchecked)
        .await
        .unwrap();
    assert!(!store.cancel_task_if_active("t1").await.unwrap());
    let done = store.get_task("t1").await.unwrap().unwrap();
    assert_eq!(done.state, TaskState::Completed);
    let finished = done.finished_at.expect("a completed task is finished");
    // The first finish wins: a later write, finishing it again, otherwise or
    // reopening it, changes nothing, and the first finish time stays. Times
    // are whole seconds, so wait past the next one for this to be able to fail.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    for state in [
        TaskState::Completed,
        TaskState::Failed,
        TaskState::Processing,
    ] {
        assert!(
            !store
                .set_task_state("t1", state, Fence::Unchecked)
                .await
                .unwrap(),
            "{state:?} over a finished task"
        );
    }
    let kept = store.get_task("t1").await.unwrap().unwrap();
    assert_eq!(
        (kept.state, kept.finished_at),
        (TaskState::Completed, Some(finished))
    );
    assert!(!store.cancel_task_if_active("missing").await.unwrap());
    assert!(
        !store
            .set_task_state("missing", TaskState::Failed, Fence::Unchecked)
            .await
            .unwrap()
    );

    // A second task with a stored id is refused, and the first is kept.
    match store.create_task(&sample_task("t1"), None, None).await {
        Err(StorageError::TaskExists(id)) => assert_eq!(id, "t1"),
        other => panic!("a duplicate create: {other:?}"),
    }
    assert_eq!(store.get_task("t1").await.unwrap().unwrap(), kept);

    // --- webhook delivery flag
    store
        .set_item_state("t1", 0, ItemState::Completed, None, Fence::Unchecked, CLAIM)
        .await
        .unwrap();
    // Only t1's item is terminal; the others (anon, anon2, the UUID task) are
    // still queued and must not be listed.
    let pending = store.items_pending_webhook().await.unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|p| (p.task_id.as_str(), p.item_index))
            .collect::<Vec<_>>(),
        [("t1", 0)]
    );
    assert!(
        store
            .mark_webhook_delivered("t1", 0, "n1/p1")
            .await
            .unwrap()
    );
    let pending = store.items_pending_webhook().await.unwrap();
    assert!(!pending.iter().any(|p| p.task_id == "t1"));
    // A finished item keeps its state.
    for state in [ItemState::Processing, ItemState::Failed] {
        assert!(
            !store
                .set_item_state("t1", 0, state, None, Fence::Unchecked, OPEN)
                .await
                .unwrap(),
            "{state:?} over a finished item"
        );
    }
    assert_eq!(
        store.get_task("t1").await.unwrap().unwrap().items[0].state,
        ItemState::Completed
    );
    assert!(
        !store
            .set_item_state("t1", 7, ItemState::Failed, None, Fence::Unchecked, OPEN)
            .await
            .unwrap(),
        "no such item"
    );
    // Failed and cancelled items are pending too; queued and processing ones are
    // not. Compared as a set: the query has no defined order.
    // A kind other than the fallback's, so storing the message alone would fail.
    let err = TaskError::fetch("fetch failed");
    store
        .set_item_state(
            "anon",
            0,
            ItemState::Failed,
            Some(&err),
            Fence::Unchecked,
            OPEN,
        )
        .await
        .unwrap();
    store
        .set_item_state(
            "anon2",
            0,
            ItemState::Cancelled,
            None,
            Fence::Unchecked,
            OPEN,
        )
        .await
        .unwrap();
    store
        .set_item_state(
            UUID_ID,
            0,
            ItemState::Processing,
            None,
            Fence::Unchecked,
            OPEN,
        )
        .await
        .unwrap();
    let mut pending: Vec<(String, usize)> = store
        .items_pending_webhook()
        .await
        .unwrap()
        .into_iter()
        .map(|p| (p.task_id, p.item_index))
        .collect();
    pending.sort();
    assert_eq!(pending, vec![("anon".into(), 0), ("anon2".into(), 0)]);
    let anon = store.get_task("anon").await.unwrap().unwrap();
    assert_eq!(anon.items[0].error, Some(err));

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
    assert!(
        store
            .url_cache_lookup("u", "m", "main", now() + 3600)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .cache_lookup("h", "m", "other", 0)
            .await
            .unwrap()
            .is_none(),
        "another revision misses"
    );
    // Storing again replaces the entry.
    let newer = ModelOutput::Classification(Classification {
        predictions: vec![Prediction::new(8, "eight", 0.5)],
    });
    store.cache_store("h", "m", "main", &newer).await.unwrap();
    assert_eq!(
        store.cache_lookup("h", "m", "main", 0).await.unwrap(),
        Some(newer)
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
        store.create_task(&t, None, None).await.unwrap();
    }
    store
        .set_task_state("p2", TaskState::Failed, Fence::Unchecked)
        .await
        .unwrap();
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
        .set_task_state("anon", TaskState::Completed, Fence::Unchecked)
        .await
        .unwrap();
    // t1, the UUID task, anon and p2 are finished.
    assert_eq!(store.purge_finished_before(now() + 10).await.unwrap(), 4);
    assert!(store.get_task("t1").await.unwrap().is_none());
    assert!(store.get_task(UUID_ID).await.unwrap().is_none());
    assert!(
        store
            .load_samples("t1", 0, "nsfw")
            .await
            .unwrap()
            .is_empty()
    );
    // No children were left behind: the id can be used again from scratch.
    store
        .create_task(&sample_task("t1"), None, None)
        .await
        .unwrap();
    let again = store.get_task("t1").await.unwrap().unwrap();
    assert_eq!(again.items.len(), 1);
    assert_eq!(again.items[0].results["general"].state, ModelState::Queued);
    assert_eq!(store.steps_completed("t1", 0, "general").await.unwrap(), 0);
    assert!(
        store.get_task("p1").await.unwrap().is_some(),
        "active tasks stay"
    );
    assert_eq!(store.purge_finished_before(now() + 10).await.unwrap(), 0);

    // --- failing an item: the item and the listed models, in one write
    store
        .create_task(&sample_task("f1"), None, None)
        .await
        .unwrap();
    let err = TaskError::fetch("unreachable");
    let general = ["general".to_string()];
    assert!(
        store
            .set_item_failed("f1", 0, &err, &general, Fence::Unchecked, OPEN)
            .await
            .unwrap()
    );
    let f1 = store.get_task("f1").await.unwrap().unwrap();
    assert_eq!(f1.items[0].state, ItemState::Failed);
    assert_eq!(f1.items[0].error.as_ref(), Some(&err));
    assert_eq!(f1.items[0].results["general"].state, ModelState::Failed);
    assert_eq!(f1.items[0].results["general"].error.as_ref(), Some(&err));
    assert_ne!(f1.items[0].results["nsfw"].state, ModelState::Failed);
    // Once finished, nothing more is written.
    let nsfw = ["nsfw".to_string()];
    assert!(
        !store
            .set_item_failed("f1", 0, &err, &nsfw, Fence::Unchecked, OPEN)
            .await
            .unwrap()
    );
    assert_eq!(store.get_task("f1").await.unwrap().unwrap(), f1);
    assert!(
        !store
            .set_item_failed("f1", 7, &err, &nsfw, Fence::Unchecked, OPEN)
            .await
            .unwrap()
    );

    // --- a retried item goes on, and finishes
    store
        .create_task(&sample_task("r1"), None, None)
        .await
        .unwrap();
    for state in [
        ItemState::Processing,
        ItemState::Retrying,
        ItemState::Processing,
        ItemState::Retrying,
        ItemState::Completed,
    ] {
        assert!(
            store
                .set_item_state("r1", 0, state, None, Fence::Unchecked, OPEN)
                .await
                .unwrap(),
            "{state:?}"
        );
    }

    // --- two writers storing one id at the same moment: one stores it, the
    // other is told it exists
    for round in 0..5 {
        let id = format!("dup{round}");
        let task = sample_task(&id);
        let (first, second) = tokio::join!(
            store.create_task(&task, None, None),
            store.create_task(&task, None, None)
        );
        let outcome = |r: Result<(), StorageError>| match r {
            Ok(()) => "stored",
            Err(StorageError::TaskExists(_)) => "exists",
            Err(e) => panic!("{id}: {e}"),
        };
        let mut outcomes = [outcome(first), outcome(second)];
        outcomes.sort();
        assert_eq!(outcomes, ["exists", "stored"], "{id}");
    }
}

/// A lease for `token`, held by node `node`, for `ttl`.
fn lease<'a>(token: &'a str, node: &'a str, ttl: Duration) -> Lease<'a> {
    Lease {
        token,
        node,
        process: "p",
        ttl,
    }
}

/// How claimable `id` is for `node`, per mode: (Force, Open, Expired), each
/// `None` (not claimable) or `Some(abandoned pin)`.
async fn claimable(
    store: &dyn Storage,
    id: &str,
    node: &str,
    grace: Duration,
) -> [Option<Option<String>>; 3] {
    let mut out = [None, None, None];
    for (i, mode) in [ClaimMode::Force, ClaimMode::Open, ClaimMode::Expired]
        .into_iter()
        .enumerate()
    {
        let rows = store
            .claimable_tasks(mode, node, &[], grace, None, 100)
            .await
            .unwrap();
        out[i] = rows
            .into_iter()
            .find(|c| c.id == id)
            .map(|c| c.abandoned_pin);
    }
    out
}

/// The lease contract: claims on the database clock, fences, renewals,
/// releases, attempts, pins.
async fn exercise_leases(store: &dyn Storage) {
    const SHORT: Duration = Duration::from_millis(300);
    const PAST_SHORT: Duration = Duration::from_millis(450);
    const LONG: Duration = Duration::from_secs(60);
    let none = Duration::ZERO;
    let held = |id: &str, token: &str| Held {
        task_id: id.into(),
        token: token.into(),
    };

    // A claimed task: claimable by nobody while its lease lives, except by
    // force; its runner's writes need its token.
    store
        .create_task(&sample_task("L1"), Some(&lease("a1", "A", LONG)), None)
        .await
        .unwrap();
    assert_eq!(
        claimable(store, "L1", "B", none).await,
        [Some(None), None, None]
    );
    assert_eq!(
        store
            .claim_task("L1", &lease("b1", "B", LONG), ClaimMode::Expired, none)
            .await
            .unwrap(),
        None,
        "a live lease isn't taken over"
    );
    assert!(
        !store
            .set_task_state("L1", TaskState::Processing, Fence::Claim("b1"))
            .await
            .unwrap(),
        "fenced out"
    );
    assert!(
        store
            .set_task_state("L1", TaskState::Processing, Fence::Claim("a1"))
            .await
            .unwrap()
    );
    let err = TaskError::fetch("x");
    let general = ["general".to_string()];
    let done = ModelResult::done(ModelOutput::Classification(Classification::default()));
    for fence in [Fence::Claim("b1"), Fence::Claim("a1")] {
        let ours = fence == Fence::Claim("a1");
        assert_eq!(
            store
                .set_item_state("L1", 0, ItemState::Processing, None, fence, OPEN)
                .await
                .unwrap(),
            ours,
            "{fence:?}"
        );
        assert_eq!(
            store
                .upsert_model_result("L1", 0, "general", &done, fence)
                .await
                .unwrap(),
            ours,
            "{fence:?}"
        );
        assert_eq!(
            store.set_item_retries("L1", 0, 1, fence).await.unwrap(),
            ours,
            "{fence:?}"
        );
    }
    assert!(
        !store
            .set_item_failed("L1", 0, &err, &general, Fence::Claim("b1"), OPEN)
            .await
            .unwrap()
    );
    let l1 = store.get_task("L1").await.unwrap().unwrap();
    assert_eq!(l1.items[0].state, ItemState::Processing);
    assert_eq!(l1.items[0].results["general"].state, ModelState::Done);
    assert_eq!(l1.items[0].retries, 1);

    // Renewing: by id and token only, and not a change to the task. Times
    // are whole seconds: wait for the next one, so a touched `updated_at`
    // would show.
    let second = now();
    while now() == second {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        store
            .renew_claims(
                &[held("L1", "a1"), held("L1", "b1"), held("nope", "a1")],
                LONG
            )
            .await
            .unwrap(),
        [
            Renewal::Held(TaskState::Processing),
            Renewal::Lost,
            Renewal::Lost
        ]
    );
    let renewed = store.get_task("L1").await.unwrap().unwrap();
    assert_eq!(renewed.updated_at, l1.updated_at);
    assert_eq!(renewed, l1);

    // An expired lease: taken over, counting an attempt, and the old runner
    // is fenced out.
    store
        .create_task(&sample_task("L2"), Some(&lease("a2", "A", SHORT)), None)
        .await
        .unwrap();
    assert_eq!(
        claimable(store, "L2", "B", none).await,
        [Some(None), None, None]
    );
    tokio::time::sleep(PAST_SHORT).await;
    assert_eq!(
        claimable(store, "L2", "B", none).await,
        [Some(None), None, Some(None)]
    );
    assert_eq!(
        store
            .claim_task("L2", &lease("b2", "B", LONG), ClaimMode::Open, none)
            .await
            .unwrap(),
        None,
        "an expired claim isn't open"
    );
    assert_eq!(
        store
            .claim_task("L2", &lease("b2", "B", LONG), ClaimMode::Expired, none)
            .await
            .unwrap(),
        Some(1),
        "a takeover of a lapsed claim counts an attempt"
    );
    assert_eq!(
        store
            .renew_claims(&[held("L2", "a2"), held("L2", "b2")], LONG)
            .await
            .unwrap(),
        [Renewal::Lost, Renewal::Held(TaskState::Queued)]
    );
    assert!(
        !store
            .set_task_state("L2", TaskState::Failed, Fence::Claim("a2"))
            .await
            .unwrap()
    );

    // Released: open at once, and the next claim counts nothing.
    assert_eq!(
        store
            .release_claims(&[held("L2", "b2"), held("L2", "a2")])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        claimable(store, "L2", "C", none).await,
        [Some(None), Some(None), Some(None)]
    );
    assert_eq!(
        store
            .claim_task("L2", &lease("c2", "C", LONG), ClaimMode::Open, none)
            .await
            .unwrap(),
        Some(1),
        "no attempt for a handed-back claim"
    );
    // Force takes even a live lease, and counts the claim it replaces.
    assert_eq!(
        store
            .claim_task("L2", &lease("d2", "D", LONG), ClaimMode::Force, none)
            .await
            .unwrap(),
        Some(2)
    );

    // Never claimed (stored before leases): open, and no attempt.
    store
        .create_task(&sample_task("L3"), None, None)
        .await
        .unwrap();
    assert_eq!(
        claimable(store, "L3", "B", none).await,
        [Some(None), Some(None), Some(None)]
    );
    assert_eq!(
        store
            .claim_task("L3", &lease("b3", "B", LONG), ClaimMode::Open, none)
            .await
            .unwrap(),
        Some(0)
    );

    // Pinned to node A: others leave it until A's lease ended more than the
    // grace ago, then may only fail it; A itself may run it.
    store
        .create_task(
            &sample_task("L4"),
            Some(&lease("a4", "A", SHORT)),
            Some("A"),
        )
        .await
        .unwrap();
    tokio::time::sleep(PAST_SHORT).await;
    assert_eq!(
        claimable(store, "L4", "B", LONG).await,
        [Some(Some("A".into())), None, None],
        "within the grace"
    );
    assert_eq!(
        claimable(store, "L4", "A", LONG).await,
        [Some(None), None, Some(None)],
        "its own node"
    );
    assert_eq!(
        store
            .claim_task("L4", &lease("b4", "B", LONG), ClaimMode::Expired, LONG)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        claimable(store, "L4", "B", none).await,
        [Some(Some("A".into())), None, Some(Some("A".into()))],
        "past the grace"
    );
    let row = |rows: Vec<apollo_storage::Claimable>, id: &str| {
        rows.into_iter().find(|c| c.id == id).expect("listed")
    };
    let own = row(
        store
            .claimable_tasks(ClaimMode::Force, "A", &[], LONG, None, 100)
            .await
            .unwrap(),
        "L4",
    );
    assert_eq!(
        (own.pinned_here, own.stale),
        (true, false),
        "its own, within the grace"
    );
    let other = row(
        store
            .claimable_tasks(ClaimMode::Force, "B", &[], none, None, 100)
            .await
            .unwrap(),
        "L4",
    );
    assert_eq!(
        (other.pinned_here, other.stale),
        (false, true),
        "another's, past the grace"
    );
    assert!(
        store
            .claim_task("L4", &lease("b4", "B", LONG), ClaimMode::Expired, none)
            .await
            .unwrap()
            .is_some()
    );

    // Pinned and cancelled: finishing it reads no input, so the pin doesn't
    // hold it back, and it isn't reported abandoned.
    store
        .create_task(
            &sample_task("L7"),
            Some(&lease("a7", "A", SHORT)),
            Some("A"),
        )
        .await
        .unwrap();
    assert!(store.cancel_task_if_active("L7").await.unwrap());
    tokio::time::sleep(PAST_SHORT).await;
    assert_eq!(
        claimable(store, "L7", "B", LONG).await,
        [Some(None), None, Some(None)]
    );
    assert!(
        store
            .claim_task("L7", &lease("b7", "B", LONG), ClaimMode::Expired, LONG)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .set_item_state(
                "L7",
                0,
                ItemState::Cancelled,
                None,
                Fence::Claim("b7"),
                OPEN
            )
            .await
            .unwrap()
    );

    // Stored before leases: stale (nobody has held it since).
    store
        .create_task(&sample_task("L8"), None, None)
        .await
        .unwrap();
    let legacy = row(
        store
            .claimable_tasks(ClaimMode::Force, "B", &[], LONG, None, 100)
            .await
            .unwrap(),
        "L8",
    );
    assert_eq!((legacy.pinned_here, legacy.stale), (false, true));

    // A cancelled task is claimable while it has unfinished items (to finish
    // them), and not once they are.
    store
        .create_task(&sample_task("L5"), Some(&lease("a5", "A", SHORT)), None)
        .await
        .unwrap();
    assert!(store.cancel_task_if_active("L5").await.unwrap());
    tokio::time::sleep(PAST_SHORT).await;
    assert_eq!(
        claimable(store, "L5", "B", none).await,
        [Some(None), None, Some(None)]
    );
    let rows = store
        .claimable_tasks(ClaimMode::Expired, "B", &[], none, None, 100)
        .await
        .unwrap();
    assert_eq!(
        rows.iter().find(|c| c.id == "L5").map(|c| c.state),
        Some(TaskState::Cancelled)
    );
    assert!(
        store
            .set_item_state(
                "L5",
                0,
                ItemState::Cancelled,
                None,
                Fence::Claim("a5"),
                OPEN
            )
            .await
            .unwrap()
    );
    assert_eq!(claimable(store, "L5", "B", none).await, [None, None, None]);
    assert_eq!(
        store
            .claim_task("L5", &lease("b5", "B", LONG), ClaimMode::Expired, none)
            .await
            .unwrap(),
        None
    );

    // Skipped ids, oldest first, and paging.
    let all = store
        .claimable_tasks(ClaimMode::Force, "B", &[], none, None, 100)
        .await
        .unwrap();
    let ids: Vec<&str> = all.iter().map(|c| c.id.as_str()).collect();
    assert!(ids.windows(2).all(|w| {
        let (a, b) = (
            &all[ids.iter().position(|x| x == &w[0]).unwrap()],
            &all[ids.iter().position(|x| x == &w[1]).unwrap()],
        );
        (a.created_at, &a.id) < (b.created_at, &b.id)
    }));
    let skipped = store
        .claimable_tasks(ClaimMode::Force, "B", &["L1".to_string()], none, None, 100)
        .await
        .unwrap();
    assert!(!skipped.iter().any(|c| c.id == "L1"));
    assert_eq!(skipped.len() + 1, all.len());
    let first = store
        .claimable_tasks(ClaimMode::Force, "B", &[], none, None, 1)
        .await
        .unwrap();
    let cursor = TaskCursor {
        created_at: first[0].created_at,
        id: first[0].id.clone(),
    };
    let rest = store
        .claimable_tasks(ClaimMode::Force, "B", &[], none, Some(&cursor), 100)
        .await
        .unwrap();
    assert_eq!(
        std::iter::once(first[0].id.clone())
            .chain(rest.iter().map(|c| c.id.clone()))
            .collect::<Vec<_>>(),
        all.iter().map(|c| c.id.clone()).collect::<Vec<_>>()
    );

    // Racing claimers: exactly one wins.
    store
        .create_task(&sample_task("L6"), Some(&lease("a6", "A", SHORT)), None)
        .await
        .unwrap();
    tokio::time::sleep(PAST_SHORT).await;
    let tokens: Vec<String> = (0..6).map(|i| format!("r{i}")).collect();
    let claims = futures_lite_join(store, &tokens).await;
    assert_eq!(
        claims.iter().filter(|c| c.is_some()).count(),
        1,
        "{claims:?}"
    );
}

/// Whether `items` has `task_id`'s item 0.
fn has(items: &[PendingWebhook], task_id: &str) -> bool {
    items
        .iter()
        .any(|p| p.task_id == task_id && p.item_index == 0)
}

/// Finished-webhook delivery claims.
async fn exercise_webhook_claims(store: &dyn Storage) {
    let ttl = Duration::from_secs(30);
    // Settle what the other exercises left pending (a Force claim takes any).
    for p in store
        .claim_pending_webhooks("sweep", None, ClaimMode::Force, ttl, 1000)
        .await
        .unwrap()
    {
        assert!(
            store
                .mark_webhook_delivered(&p.task_id, p.item_index, "sweep")
                .await
                .unwrap()
        );
    }
    assert!(store.items_pending_webhook().await.unwrap().is_empty());

    let finish = |id: &'static str, delivery: Delivery<'static>| async move {
        store
            .create_task(&sample_task(id), None, None)
            .await
            .unwrap();
        assert!(
            store
                .set_item_state(
                    id,
                    0,
                    ItemState::Completed,
                    None,
                    Fence::Unchecked,
                    delivery
                )
                .await
                .unwrap()
        );
    };

    // (1) A finishing write's claim: listed as pending, but others take it
    // only by force.
    finish("W1", CLAIM).await;
    assert!(has(&store.items_pending_webhook().await.unwrap(), "W1"));
    for mode in [ClaimMode::Open, ClaimMode::Expired] {
        let got = store
            .claim_pending_webhooks("n2/p2", Some("t://x"), mode, ttl, 1000)
            .await
            .unwrap();
        assert!(!has(&got, "W1"), "{mode:?} took a live claim");
    }
    let got = store
        .claim_pending_webhooks("n2/p2", Some("t://x"), ClaimMode::Force, ttl, 1000)
        .await
        .unwrap();
    assert!(has(&got, "W1"));

    // (2) A lapsed claim: Expired takes it, Open doesn't.
    let short = Delivery::Claim {
        holder: "n1/p1",
        target: "t://x",
        ttl: Duration::from_millis(50),
    };
    finish("W2", short).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let got = store
        .claim_pending_webhooks("n2/p2", Some("t://x"), ClaimMode::Open, ttl, 1000)
        .await
        .unwrap();
    assert!(!has(&got, "W2"), "Open took a lapsed claim");
    let got = store
        .claim_pending_webhooks("n2/p2", Some("t://x"), ClaimMode::Expired, ttl, 1000)
        .await
        .unwrap();
    assert!(has(&got, "W2"), "Expired left a lapsed claim");

    // (3) Only for its receiver, or for any; a row for any receiver goes to
    // every one.
    finish("W3", short).await;
    finish("W3b", OPEN).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let got = store
        .claim_pending_webhooks("n2/p2", Some("t://y"), ClaimMode::Expired, ttl, 1000)
        .await
        .unwrap();
    assert!(!has(&got, "W3"), "another receiver's event");
    assert!(has(&got, "W3b"), "an event for any receiver");
    let got = store
        .claim_pending_webhooks("n2/p2", None, ClaimMode::Expired, ttl, 1000)
        .await
        .unwrap();
    assert!(has(&got, "W3"), "a claim for any receiver");

    // (4) Only the holder marks it delivered.
    finish("W4", CLAIM).await;
    assert!(
        !store
            .mark_webhook_delivered("W4", 0, "n2/p2")
            .await
            .unwrap()
    );
    assert!(has(&store.items_pending_webhook().await.unwrap(), "W4"));
    assert!(
        store
            .mark_webhook_delivered("W4", 0, "n1/p1")
            .await
            .unwrap()
    );
    assert!(!has(&store.items_pending_webhook().await.unwrap(), "W4"));
    assert!(
        !store
            .mark_webhook_delivered("W4", 0, "n1/p1")
            .await
            .unwrap(),
        "marked twice"
    );

    // (5) A claim handed back is anyone's.
    finish("W5", CLAIM).await;
    assert!(!store.release_webhook_claim("W5", 0, "n2/p2").await.unwrap());
    assert!(store.release_webhook_claim("W5", 0, "n1/p1").await.unwrap());
    let got = store
        .claim_pending_webhooks("n2/p2", Some("t://x"), ClaimMode::Open, ttl, 1000)
        .await
        .unwrap();
    assert!(has(&got, "W5"), "a handed-back claim");

    // (6) Renewing says whether the claim is still held.
    finish("W6", CLAIM).await;
    let w6 = [PendingWebhook {
        task_id: "W6".into(),
        item_index: 0,
    }];
    assert_eq!(
        store.renew_webhook_claims("n1/p1", &w6, ttl).await.unwrap(),
        [true]
    );
    let got = store
        .claim_pending_webhooks("n2/p2", Some("t://x"), ClaimMode::Force, ttl, 1000)
        .await
        .unwrap();
    assert!(has(&got, "W6"));
    assert_eq!(
        store.renew_webhook_claims("n1/p1", &w6, ttl).await.unwrap(),
        [false]
    );
    assert_eq!(
        store.renew_webhook_claims("n2/p2", &w6, ttl).await.unwrap(),
        [true]
    );

    // (7) Webhooks off: nothing is kept to send.
    finish("W7", Delivery::Off).await;
    assert!(!has(&store.items_pending_webhook().await.unwrap(), "W7"));
    let got = store
        .claim_pending_webhooks("n2/p2", None, ClaimMode::Force, ttl, 1000)
        .await
        .unwrap();
    assert!(!has(&got, "W7"));

    // (8) A write that doesn't finish the item lists nothing pending, and the
    // finishing write's delivery is what counts.
    store
        .create_task(&sample_task("W8"), None, None)
        .await
        .unwrap();
    assert!(
        store
            .set_item_state(
                "W8",
                0,
                ItemState::Processing,
                None,
                Fence::Unchecked,
                CLAIM
            )
            .await
            .unwrap()
    );
    assert!(!has(&store.items_pending_webhook().await.unwrap(), "W8"));
    assert!(
        store
            .set_item_state(
                "W8",
                0,
                ItemState::Failed,
                None,
                Fence::Unchecked,
                Delivery::Open {
                    target: Some("t://x")
                }
            )
            .await
            .unwrap()
    );
    let got = store
        .claim_pending_webhooks("n2/p2", Some("t://x"), ClaimMode::Open, ttl, 1000)
        .await
        .unwrap();
    assert!(
        has(&got, "W8"),
        "unclaimed: the earlier write claimed nothing"
    );

    // set_item_failed records it too.
    store
        .create_task(&sample_task("W8b"), None, None)
        .await
        .unwrap();
    let err = TaskError::fetch("fetch failed");
    assert!(
        store
            .set_item_failed("W8b", 0, &err, &[], Fence::Unchecked, CLAIM)
            .await
            .unwrap()
    );
    let got = store
        .claim_pending_webhooks("n2/p2", Some("t://x"), ClaimMode::Expired, ttl, 1000)
        .await
        .unwrap();
    assert!(!has(&got, "W8b"), "a failed item's live claim");
    assert!(
        store
            .mark_webhook_delivered("W8b", 0, "n1/p1")
            .await
            .unwrap()
    );

    // (10) At most `limit` at a time.
    finish("W10a", OPEN).await;
    finish("W10b", OPEN).await;
    let got = store
        .claim_pending_webhooks("n3/p3", None, ClaimMode::Open, ttl, 1)
        .await
        .unwrap();
    assert_eq!(got.len(), 1, "{got:?}");
    let more = store
        .claim_pending_webhooks("n3/p3", None, ClaimMode::Open, ttl, 1000)
        .await
        .unwrap();
    assert!(
        has(&more, "W10a") != has(&got, "W10a"),
        "the other one, next"
    );

    // (9) Concurrent claimers: at most one gets it (a conflict is not a claim).
    finish("W9", OPEN).await;
    let (x, y, z) = tokio::join!(
        store.claim_pending_webhooks("c1", None, ClaimMode::Open, ttl, 1000),
        store.claim_pending_webhooks("c2", None, ClaimMode::Open, ttl, 1000),
        store.claim_pending_webhooks("c3", None, ClaimMode::Open, ttl, 1000),
    );
    let winners = [x, y, z]
        .into_iter()
        .filter(|r| r.as_ref().is_ok_and(|got| has(got, "W9")))
        .count();
    assert!(winners <= 1, "{winners} claimers got W9");
}

/// The shared public key and token revocations.
async fn exercise_auth(store: &dyn Storage) {
    assert_eq!(store.auth_key().await.unwrap(), None, "a fresh database");
    // Instances starting together on a new database: one key wins, and every
    // one of them is told the same.
    let keys: Vec<String> = (0..8).map(|i| format!("k4.public.key{i}")).collect();
    let (a, b, c, d) = tokio::join!(
        store.publish_auth_key(&keys[0], "n0"),
        store.publish_auth_key(&keys[1], "n1"),
        store.publish_auth_key(&keys[2], "n2"),
        store.publish_auth_key(&keys[3], "n3"),
    );
    let (e, f, g, h) = tokio::join!(
        store.publish_auth_key(&keys[4], "n4"),
        store.publish_auth_key(&keys[5], "n5"),
        store.publish_auth_key(&keys[6], "n6"),
        store.publish_auth_key(&keys[7], "n7"),
    );
    let told: Vec<_> = [a, b, c, d, e, f, g, h]
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    let stored = store.auth_key().await.unwrap().unwrap();
    assert!(told.iter().all(|k| *k == stored), "{told:?} vs {stored:?}");
    assert!(keys.contains(&stored.public_key));
    let i = keys.iter().position(|k| *k == stored.public_key).unwrap();
    assert_eq!(stored.node.as_deref(), Some(format!("n{i}").as_str()));

    // Publishing again never replaces it; replacing does.
    let other = store
        .publish_auth_key("k4.public.other", "x")
        .await
        .unwrap();
    assert_eq!(other, stored);
    store
        .replace_auth_key("k4.public.rotated", "r")
        .await
        .unwrap();
    assert_eq!(
        store.auth_key().await.unwrap(),
        Some(apollo_storage::SharedKey {
            public_key: "k4.public.rotated".into(),
            node: Some("r".into()),
        })
    );

    // Revocations: once each, however often revoked.
    assert!(store.revocations().await.unwrap().is_empty());
    let id = Revocation::Id("jti-1".into());
    let subject = Revocation::Subject("bob".into());
    for _ in 0..2 {
        store.add_revocation(&id).await.unwrap();
        store.add_revocation(&subject).await.unwrap();
    }
    let mut listed = store.revocations().await.unwrap();
    listed.sort_by_key(|r| r.to_string());
    assert_eq!(listed, [id.clone(), subject.clone()]);
    // An id and a subject with the same value are two entries.
    store
        .add_revocation(&Revocation::Subject("jti-1".into()))
        .await
        .unwrap();
    assert_eq!(store.revocations().await.unwrap().len(), 3);
    assert!(store.remove_revocation(&id).await.unwrap());
    assert!(!store.remove_revocation(&id).await.unwrap());
    let left = store.revocations().await.unwrap();
    assert_eq!(left.len(), 2);
    assert!(!left.contains(&id));
}

/// Each token tries to claim L6 at the same moment.
async fn futures_lite_join(store: &dyn Storage, tokens: &[String]) -> Vec<Option<u32>> {
    let leases: Vec<Lease<'_>> = tokens
        .iter()
        .map(|t| lease(t, "R", Duration::from_secs(60)))
        .collect();
    let mut claims = Vec::new();
    for chunk in leases.chunks(3) {
        let (x, y, z) = tokio::join!(
            store.claim_task("L6", &chunk[0], ClaimMode::Expired, Duration::ZERO),
            store.claim_task("L6", &chunk[1], ClaimMode::Expired, Duration::ZERO),
            store.claim_task("L6", &chunk[2], ClaimMode::Expired, Duration::ZERO),
        );
        for r in [x, y, z] {
            // A conflicting claim may fail outright: it didn't take the task.
            claims.push(r.ok().flatten());
        }
    }
    claims
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
    exercise_leases(&store).await;
    exercise_webhook_claims(&store).await;
    exercise_auth(&store).await;
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
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let store = SurrealStorage::connect(&cfg).await.unwrap();
    let (raw_url, raw_user, raw_password, raw_db) = (
        url.clone(),
        user.clone(),
        password.clone(),
        database.clone(),
    );

    // Run the contract, then always drop the throwaway database.
    let outcome = tokio::spawn(async move {
        exercise(&store).await;
        exercise_leases(&store).await;
        exercise_webhook_claims(&store).await;
        exercise_auth(&store).await;
        // A revocation row of the wrong shape (written by hand) is left out,
        // not a failure of every read.
        let raw = surrealdb::engine::any::connect(raw_url).await.unwrap();
        if let (Some(username), Some(password)) = (raw_user, raw_password) {
            raw.signin(surrealdb::opt::auth::Root { username, password })
                .await
                .unwrap();
        }
        raw.use_ns("apollo_test").use_db(raw_db).await.unwrap();
        raw.query("CREATE revocation:['id', 'bad'] CONTENT { kind: 'id', value: 42 }")
            .await
            .unwrap()
            .check()
            .unwrap();
        assert_eq!(store.revocations().await.unwrap().len(), 2);
    })
    .await;

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
