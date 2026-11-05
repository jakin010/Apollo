//! Engine-level tests against a real SQLite store. No model is ever loaded: these
//! cover lifecycle paths (recovery, cancellation, cleanup) that run without
//! inference.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use apollo_config::{Config, DatabaseConfig, SqliteConfig};
use apollo_domain::{
    ErrorKind, Input, Item, ItemState, MediaKind, ModelResult, ModelState, Task, TaskState,
};
use apollo_storage::{Fence, Storage, TaskQuery};

use crate::{Engine, EngineError, Run, Submission};

/// What a planted terminal write records about the finished webhook:
/// pending, for any receiver, nobody's to deliver.
const PLANTED: apollo_storage::Delivery<'static> = apollo_storage::Delivery::Open { target: None };

struct Fixture {
    engine: Engine,
    storage: Arc<dyn Storage>,
    _dir: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    fixture_with("").await
}

/// A fixture whose config also has the `models` / `pipelines` TOML given.
/// Models are never loaded, so their repos need not exist.
async fn fixture_with(toml: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(&dir, toml);
    let storage = apollo_storage::open(&config.database).await.unwrap();
    let engine = Engine::new(Arc::new(config), storage.clone(), None);
    Fixture {
        engine,
        storage,
        _dir: dir,
    }
}

/// `toml` with the database and cache directory in `dir`.
fn config_in(dir: &tempfile::TempDir, toml: &str) -> Config {
    let mut config = apollo_config::load::from_str(toml).unwrap();
    config.validate().unwrap();
    config.database = DatabaseConfig {
        sqlite: Some(SqliteConfig {
            path: dir.path().join("apollo.db").to_string_lossy().into_owned(),
            ..Default::default()
        }),
        ..Default::default()
    };
    config.app.cache_dir = Some(dir.path().to_string_lossy().into_owned());
    config
}

fn task(id: &str, state: TaskState, item_state: ItemState, upload: Option<PathBuf>) -> Task {
    let input = match upload {
        Some(path) => Input::Bytes {
            path,
            kind: MediaKind::Image,
        },
        None => image(),
    };
    let mut item = Item::new(input, vec!["m".into()], None);
    item.state = item_state;
    let mut task = Task::new(id, vec![item], None);
    task.state = state;
    task
}

fn image() -> Input {
    Input::Image(apollo_domain::Url {
        main: "https://example.com/a.png".into(),
        fallback: None,
    })
}

fn staged_upload(engine: &Engine, name: &str) -> PathBuf {
    let path = engine.ensure_upload_dir().unwrap().join(name);
    std::fs::write(&path, b"bytes").unwrap();
    path
}

#[tokio::test]
async fn poison_task_is_failed_reported_and_cleaned_up() {
    let f = fixture().await;
    let upload = staged_upload(&f.engine, "poison.bin");
    f.storage
        .create_task(
            &task(
                "t",
                TaskState::Processing,
                ItemState::Processing,
                Some(upload.clone()),
            ),
            Some(&crashed_claim()),
            None,
        )
        .await
        .unwrap();
    // Three crashes already happened; this recovery is the fourth attempt.
    for _ in 0..3 {
        f.storage.increment_attempts("t").await.unwrap();
    }

    assert_eq!(f.engine.recover().await.unwrap(), 0);

    let t = f.storage.get_task("t").await.unwrap().unwrap();
    assert_eq!(t.state, TaskState::Failed);
    assert_eq!(t.items[0].state, ItemState::Failed);
    assert_eq!(t.items[0].error.as_ref().unwrap().kind, ErrorKind::Internal);
    assert_eq!(t.items[0].results["m"].state, ModelState::Failed);
    assert!(!upload.exists(), "poison task's upload should be removed");
}

#[tokio::test]
async fn cancel_never_overwrites_a_finished_task() {
    let f = fixture().await;
    f.storage
        .create_task(
            &task("q", TaskState::Queued, ItemState::Queued, None),
            None,
            None,
        )
        .await
        .unwrap();
    f.storage
        .create_task(
            &task("c", TaskState::Completed, ItemState::Completed, None),
            None,
            None,
        )
        .await
        .unwrap();

    f.engine.cancel("q").await.unwrap();
    f.engine.cancel("c").await.unwrap();
    let q = f.storage.get_task("q").await.unwrap().unwrap();
    let c = f.storage.get_task("c").await.unwrap().unwrap();
    assert_eq!(q.state, TaskState::Cancelled);
    assert_eq!(c.state, TaskState::Completed);

    assert!(matches!(
        f.engine.cancel("missing").await,
        Err(EngineError::UnknownTask(_))
    ));
}

#[tokio::test]
async fn cancelled_task_upload_is_removed_once_its_item_stops() {
    let f = fixture().await;
    let upload = staged_upload(&f.engine, "cancel.bin");
    f.storage
        .create_task(
            &task(
                "t",
                TaskState::Processing,
                ItemState::Processing,
                Some(upload.clone()),
            ),
            None,
            None,
        )
        .await
        .unwrap();

    f.engine.cancel("t").await.unwrap();
    // The item is still running: its upload must survive.
    f.engine.rollup_task_state("t").await;
    assert!(upload.exists());

    // The item reaches its cancellation checkpoint (what cancel_item persists).
    f.storage
        .set_item_state(
            "t",
            0,
            ItemState::Cancelled,
            None,
            Fence::Unchecked,
            PLANTED,
        )
        .await
        .unwrap();
    f.engine.rollup_task_state("t").await;
    assert!(
        !upload.exists(),
        "cancelled task's upload should be removed"
    );
    let t = f.storage.get_task("t").await.unwrap().unwrap();
    assert_eq!(t.state, TaskState::Cancelled);
}

/// The claim of a run that crashed: never handed back.
fn crashed_claim() -> apollo_storage::Lease<'static> {
    apollo_storage::Lease {
        token: "crashed",
        node: "gone",
        process: "p",
        ttl: Duration::from_secs(60),
    }
}

#[tokio::test]
async fn shutdown_hands_back_only_its_own_claims() {
    let f = fixture().await;
    f.storage
        .create_task(
            &task("t", TaskState::Processing, ItemState::Processing, None),
            Some(&crashed_claim()),
            None,
        )
        .await
        .unwrap();
    // And one this engine runs.
    let mine = apollo_storage::Lease {
        token: "mine",
        node: f.engine.node(),
        ..crashed_claim()
    };
    f.storage
        .create_task(
            &task("m", TaskState::Processing, ItemState::Processing, None),
            Some(&mine),
            None,
        )
        .await
        .unwrap();
    let _run = f
        .engine
        .register("m", "mine".into(), Instant::now())
        .unwrap();

    f.engine.shutdown().await;

    // Its own was handed back: taking it counts no attempt.
    let later = apollo_storage::Lease {
        token: "later",
        ..crashed_claim()
    };
    assert_eq!(
        f.storage
            .claim_task(
                "m",
                &later,
                apollo_storage::ClaimMode::Force,
                Duration::ZERO
            )
            .await
            .unwrap(),
        Some(0)
    );

    // Not this engine's claim: still a crash's, so taking it over counts one.
    let next = apollo_storage::Lease {
        token: "next",
        ..crashed_claim()
    };
    let attempts = f
        .storage
        .claim_task("t", &next, apollo_storage::ClaimMode::Force, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(attempts, Some(1));
}

#[tokio::test]
async fn shutdown_hands_back_claims_of_stopped_runs() {
    let f = fixture().await;
    let mine = apollo_storage::Lease {
        token: "stopped",
        node: f.engine.node(),
        ..crashed_claim()
    };
    f.storage
        .create_task(
            &task("s", TaskState::Processing, ItemState::Processing, None),
            Some(&mine),
            None,
        )
        .await
        .unwrap();
    // A run stopped for want of a renewal, which has ended, its claim not
    // handed back yet.
    f.engine
        .inner
        .unreleased
        .lock()
        .unwrap()
        .push(apollo_storage::Held {
            task_id: "s".into(),
            token: "stopped".into(),
        });

    f.engine.shutdown().await;

    let later = apollo_storage::Lease {
        token: "later",
        ..crashed_claim()
    };
    assert_eq!(
        f.storage
            .claim_task(
                "s",
                &later,
                apollo_storage::ClaimMode::Force,
                Duration::ZERO
            )
            .await
            .unwrap(),
        Some(0),
        "handed back at shutdown: taking it counts no attempt"
    );
}

#[tokio::test]
async fn upload_dir_is_private() {
    let f = fixture().await;
    let dir = f.engine.ensure_upload_dir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }
    assert!(dir.ends_with("uploads"));
}

#[tokio::test]
async fn status_reports_tasks_queue_and_models() {
    let f = fixture().await;
    f.storage
        .create_task(
            &task("q", TaskState::Queued, ItemState::Queued, None),
            None,
            None,
        )
        .await
        .unwrap();
    f.storage
        .create_task(
            &task("c", TaskState::Completed, ItemState::Completed, None),
            None,
            None,
        )
        .await
        .unwrap();
    f.storage
        .create_task(
            &task("c2", TaskState::Completed, ItemState::Completed, None),
            None,
            None,
        )
        .await
        .unwrap();

    let s = f.engine.status().await.unwrap();
    assert_eq!(
        s.tasks,
        crate::TaskCounts {
            queued: 1,
            completed: 2,
            ..Default::default()
        }
    );
    assert_eq!(
        (s.items_in_flight, s.slots_busy, s.slots_waiting),
        (0, 0, 0)
    );
    assert!(s.slots_total >= 1);
    assert_eq!(s.webhooks_pending, None, "no webhook configured");
    assert!(s.models.is_empty(), "fixture configures no models");
}

#[tokio::test]
async fn a_repeated_request_id_returns_the_existing_task() {
    let f = fixture().await;
    let mut earlier = task("earlier", TaskState::Completed, ItemState::Completed, None);
    earlier.request_id = Some("order-7".into());
    earlier.owner = Some("alice".into());
    f.storage.create_task(&earlier, None, None).await.unwrap();

    // The lookup comes first, so even a submission that would not validate
    // (no such model) gets the earlier task back.
    let submission = |rid: &str| Submission {
        input: image(),
        run: Run::Models(vec!["nope".into()]),
        metadata: Default::default(),
        request_id: Some(rid.into()),
    };
    let same = f
        .engine
        .submit(submission("order-7"), Some("alice".into()))
        .await
        .unwrap();
    assert_eq!(
        same,
        crate::Submitted {
            id: "earlier".into(),
            existing: true
        }
    );
    // Another caller's key, or a new key, is a new submission.
    for (rid, owner) in [("order-7", Some("bob")), ("order-8", Some("alice"))] {
        let err = f
            .engine
            .submit(submission(rid), owner.map(str::to_string))
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::UnknownModel(_)), "{err}");
    }
}

const PIPELINE: &str = r#"
    [models.nsfw]
    architecture = "vit"
    repo = "test/nsfw"
    [models.category]
    architecture = "siglip"
    repo = "test/siglip"
    labels = ["food", "sports"]
    early_exit = { labels = [1], threshold = 0.7 }
    video_strategy = "s"
    [models.general]
    architecture = "vit"
    repo = "test/general"
    enabled = false

    [strategies.s]
    [[strategies.s.sampling]]
    step = 1
    method = "uniform"
    count = 4

    [pipelines.moderation]
    steps = [
        { model = "nsfw", order = 1, stop_if = { labels = [1], threshold = 0.85 } },
        { model = ["category"], order = 2 },
    ]
"#;

#[tokio::test]
async fn submissions_are_checked_against_models_and_pipelines() {
    let f = fixture_with(PIPELINE).await;
    let check = |run: Run| f.engine.check_submission(&image(), &run);

    check(Run::Pipeline("moderation".into())).unwrap();
    check(Run::Models(vec!["nsfw".into(), "category".into()])).unwrap();
    assert!(matches!(
        check(Run::Pipeline("nope".into())),
        Err(EngineError::Config(_))
    ));
    assert!(matches!(
        check(Run::Models(vec!["nsfw".into(), "nsfw".into()])),
        Err(EngineError::Incompatible(_))
    ));
    assert!(matches!(
        check(Run::Models(vec!["general".into()])),
        Err(EngineError::Incompatible(_))
    ));
}

#[tokio::test]
async fn the_catalog_lists_models_labels_and_pipelines() {
    let f = fixture_with(PIPELINE).await;
    let catalog = f.engine.catalog().await.unwrap();

    let names: Vec<&str> = catalog.models.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["category", "general", "nsfw"]);
    let category = &catalog.models[0];
    assert_eq!(category.accepts, [MediaKind::Image, MediaKind::Video]);
    let labels: Vec<(u32, &str)> = category
        .labels
        .iter()
        .map(|l| (l.id, l.name.as_str()))
        .collect();
    assert_eq!(labels, [(0, "food"), (1, "sports")]);
    assert_eq!(category.trigger.as_ref().unwrap().labels, [1]);
    assert!(!category.loaded);
    // A vit model that was never downloaded: no labels, and nothing fetched.
    let nsfw = &catalog.models[2];
    assert!(nsfw.labels.is_empty());
    assert_eq!(nsfw.accepts, [MediaKind::Image]);
    assert!(!catalog.models[1].enabled);

    let p = &catalog.pipelines[0];
    assert_eq!(p.name, "moderation");
    assert_eq!(p.steps[0].models, ["nsfw"]);
    let gate = p.steps[0].stop_if.as_ref().unwrap();
    assert_eq!(gate.model, "nsfw");
    assert_eq!(gate.trigger.labels, [1]);
    assert_eq!(p.steps[1].models, ["category"]);
    assert!(p.steps[1].stop_if.is_none());
}

#[tokio::test]
async fn a_watch_follows_a_task_until_it_finishes() {
    let f = fixture().await;
    // Writes through the engine's storage are announced.
    let storage = f.engine.inner.storage.clone();
    storage
        .create_task(
            &task("w", TaskState::Queued, ItemState::Queued, None),
            None,
            None,
        )
        .await
        .unwrap();

    let mut watch = f.engine.watch("w");
    assert_eq!(
        watch.next().await.unwrap().unwrap().state,
        TaskState::Queued
    );

    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        storage
            .set_item_state(
                "w",
                0,
                ItemState::Processing,
                None,
                Fence::Unchecked,
                PLANTED,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let done = ModelResult::skipped();
        storage
            .upsert_model_result("w", 0, "m", &done, Fence::Unchecked)
            .await
            .unwrap();
        storage
            .set_item_state(
                "w",
                0,
                ItemState::Completed,
                None,
                Fence::Unchecked,
                PLANTED,
            )
            .await
            .unwrap();
        storage
            .set_task_state("w", TaskState::Completed, Fence::Unchecked)
            .await
            .unwrap();
    });

    // Each write is announced: the watch keeps up well within the time it
    // would take to read the task again on its own.
    let mut states = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(t) = watch.next().await.unwrap() {
            states.push(t.state);
        }
    })
    .await
    .expect("announced changes arrive at once");
    writer.await.unwrap();
    assert_eq!(states.last(), Some(&TaskState::Completed));
    assert!(states.len() >= 2, "{states:?}");
    assert!(watch.next().await.unwrap().is_none());
}

#[tokio::test]
async fn waiting_returns_the_finished_task_or_the_latest_state() {
    let f = fixture().await;
    let storage = f.engine.inner.storage.clone();
    for id in ["slow", "fast"] {
        storage
            .create_task(
                &task(id, TaskState::Processing, ItemState::Processing, None),
                None,
                None,
            )
            .await
            .unwrap();
    }

    // Nothing happens to "slow": the wait ends with its current state.
    let started = std::time::Instant::now();
    let slow = f
        .engine
        .wait_for("slow", Duration::from_millis(100))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(slow.state, TaskState::Processing);
    assert!(started.elapsed() >= Duration::from_millis(100));

    // "fast" finishes well within its wait, which returns at once.
    let s = storage.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        s.set_task_state("fast", TaskState::Failed, Fence::Unchecked)
            .await
            .unwrap();
    });
    let started = std::time::Instant::now();
    let fast = f
        .engine
        .wait_for("fast", Duration::from_secs(10))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fast.state, TaskState::Failed);
    // Announced, so well before a watch would read it again anyway.
    assert!(started.elapsed() < Duration::from_secs(1));

    assert!(
        f.engine
            .wait_for("missing", Duration::from_millis(10))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_reload_keeps_unchanged_models_and_replaces_changed_ones() {
    let f = fixture_with(PIPELINE).await;
    let before = f.engine.live();
    let handle = |live: &crate::live::Live, m: &str| live.registry.get(m).unwrap();

    // nsfw: only its timeout and admission limit change (read per use) -> kept.
    // category: its labels change (baked into the loaded model) -> reloaded.
    // general: enabled now -> added. A new strategy and pipeline, too.
    let changed = PIPELINE
        .replace(
            "repo = \"test/nsfw\"",
            "repo = \"test/nsfw\"\n    timeout = 99\n    max_pending = 3",
        )
        .replace(
            "labels = [\"food\", \"sports\"]",
            "labels = [\"food\", \"sports\", \"art\"]",
        )
        .replace("enabled = false", "enabled = true");
    let report = f
        .engine
        .reload(config_in(&f._dir, &changed), None)
        .await
        .unwrap();
    assert_eq!(report.generation, 2);
    assert_eq!(report.models.kept, ["nsfw"]);
    assert_eq!(report.models.reloaded, ["category"]);
    assert_eq!(report.models.added, ["general"]);
    assert!(report.models.removed.is_empty());
    assert!(report.restart_required.is_empty());

    let after = f.engine.live();
    assert!(Arc::ptr_eq(
        &handle(&before, "nsfw"),
        &handle(&after, "nsfw")
    ));
    assert!(!Arc::ptr_eq(
        &handle(&before, "category"),
        &handle(&after, "category")
    ));
    assert_eq!(after.config.models["nsfw"].timeout, 99);
    assert_eq!(after.config.models["nsfw"].max_pending, 3);
    assert_eq!(f.engine.config_generation().0, 2);

    // The new labels are listed at once.
    let catalog = f.engine.catalog().await.unwrap();
    let category = catalog
        .models
        .iter()
        .find(|m| m.name == "category")
        .unwrap();
    assert_eq!(category.labels.len(), 3);

    // Removing a model (and the pipeline that used it).
    let removed = changed
        .split("[models.general]")
        .next()
        .unwrap()
        .to_string()
        + "\n[strategies.s]\n[[strategies.s.sampling]]\nstep = 1\nmethod = \"uniform\"\ncount = 4\n";
    let report = f
        .engine
        .reload(config_in(&f._dir, &removed), None)
        .await
        .unwrap();
    assert_eq!(report.models.removed, ["general"]);
    assert_eq!(report.models.kept, ["category", "nsfw"]);
    assert!(f.engine.live().registry.get("general").is_none());
    assert!(matches!(
        f.engine
            .check_submission(&image(), &Run::Pipeline("moderation".into())),
        Err(EngineError::Config(_))
    ));
}

#[tokio::test]
async fn a_task_keeps_the_config_it_started_with() {
    let f = fixture_with(PIPELINE).await;
    let pinned = f.engine.pinned();
    let without_nsfw = PIPELINE
        .replace(
            "[models.nsfw]\n    architecture = \"vit\"\n    repo = \"test/nsfw\"\n",
            "",
        )
        .replace(
            "{ model = \"nsfw\", order = 1, stop_if = { labels = [1], threshold = 0.85 } },\n",
            "",
        )
        .replace("order = 2", "order = 1");
    f.engine
        .reload(config_in(&f._dir, &without_nsfw), None)
        .await
        .unwrap();
    // The current config lost nsfw; a handle pinned before still has it.
    assert!(f.engine.live().registry.get("nsfw").is_none());
    assert!(pinned.live().registry.get("nsfw").is_some());
    assert_eq!(pinned.config_generation().0, 1);
}

#[tokio::test]
async fn restart_only_changes_are_reported_and_invalid_configs_rejected() {
    let f = fixture_with(PIPELINE).await;
    let mut config = config_in(&f._dir, PIPELINE);
    config.app.port += 1;
    config.app.max_retries += 5;
    let report = f.engine.reload(config, None).await.unwrap();
    assert_eq!(report.restart_required, ["app.port"]);
    let defaults = apollo_config::load::from_str("").unwrap().app;
    let live = f.engine.config();
    assert_eq!(live.app.port, defaults.port);
    assert_eq!(live.app.max_retries, defaults.max_retries + 5);

    // An invalid config (unknown strategy) changes nothing.
    let bad = PIPELINE.replace("video_strategy = \"s\"", "video_strategy = \"nope\"");
    let bad: Config = apollo_config::load::from_str(&bad).unwrap();
    let err = f.engine.reload(bad, None).await.unwrap_err();
    assert!(matches!(err, EngineError::Config(_)), "{err}");
    assert_eq!(f.engine.config_generation().0, 2);
    // So does one without models.
    let empty: Config = apollo_config::load::from_str("").unwrap();
    assert!(f.engine.reload(empty, None).await.is_err());
    assert_eq!(f.engine.config_generation().0, 2);
}

#[tokio::test]
async fn metrics_render_in_the_prometheus_format() {
    let f = fixture_with(PIPELINE).await;
    // A rejected submission is counted.
    let bad = Submission {
        input: image(),
        run: Run::Models(vec!["nope".into()]),
        metadata: Default::default(),
        request_id: None,
    };
    assert!(f.engine.submit(bad, None).await.is_err());
    let text = f.engine.metrics_text().await.unwrap();
    for line in [
        "# TYPE apollo_build_info gauge",
        "apollo_submissions_total{outcome=\"invalid\"} 1",
        "apollo_config_generation 1",
        "apollo_model_loaded{model=\"nsfw\"} 0",
        "apollo_tasks{state=\"queued\"} 0",
        "# TYPE apollo_model_batch_seconds histogram",
        "apollo_fetch_seconds_bucket{le=\"+Inf\"} 0",
    ] {
        assert!(
            text.lines().any(|l| l == line),
            "missing {line:?} in\n{text}"
        );
    }
    // Every sample line is `name{labels} value` with a numeric value.
    for line in text.lines().filter(|l| !l.starts_with('#')) {
        let value = line.rsplit(' ').next().unwrap();
        assert!(
            value.parse::<f64>().is_ok() || value == "+Inf",
            "bad sample {line:?}"
        );
    }
}

#[tokio::test]
async fn a_model_loads_the_files_read_when_the_config_was_applied() {
    let files = tempfile::tempdir().unwrap();
    let list = files.path().join("blocklist.txt");
    std::fs::write(&list, format!("{} 1\n", "ab".repeat(32))).unwrap();
    let toml = format!(
        "[models.blocklist]\narchitecture = \"pdq\"\nhash_list = \"{}\"\ncategories = {{ 1 = \"known\" }}\n",
        list.display()
    );
    let f = fixture_with(&toml).await;

    // The file changes on disk, without a reload: the model still loads the
    // list that was validated.
    std::fs::write(&list, "not a hash list\n").unwrap();
    f.engine.load_model("blocklist").await.unwrap();

    // A reload reads the file again, and rejects it.
    let mut again = apollo_config::load::from_str(&toml).unwrap();
    again.database = f.engine.config().database.clone();
    again.app.cache_dir = f.engine.config().app.cache_dir.clone();
    let err = f.engine.reload(again, None).await.unwrap_err();
    assert!(err.to_string().contains("blocklist.txt`: line 1"), "{err}");
}

#[tokio::test]
async fn text_and_image_models_only_take_their_own_input() {
    let f = fixture_with(
        r#"
        [limits]
        max_text = "10b"
        [models.toxicity]
        architecture = "bert"
        repo = "test/toxic-bert"
        [models.nsfw]
        architecture = "vit"
        repo = "test/nsfw"
        "#,
    )
    .await;
    let text = Input::Text("hello".into());
    let check = |input: &Input, model: &str| {
        f.engine
            .check_submission(input, &Run::Models(vec![model.into()]))
    };
    check(&text, "toxicity").unwrap();
    for (input, model, needle) in [
        (
            &text,
            "nsfw",
            "is an image model and does not take text input",
        ),
        (
            &image(),
            "toxicity",
            "is a text model and does not take image input",
        ),
        (
            &Input::Text("a much longer text".into()),
            "toxicity",
            "over the 10-byte limit",
        ),
    ] {
        let err = check(input, model).unwrap_err().to_string();
        assert!(err.contains(needle), "{err}");
    }

    let catalog = f.engine.catalog().await.unwrap();
    let toxicity = catalog
        .models
        .iter()
        .find(|m| m.name == "toxicity")
        .unwrap();
    assert_eq!(toxicity.accepts, [MediaKind::Text]);
}

const OCR_PIPELINE: &str = r#"
    [models.ocr]
    architecture = "paddleocr_vl"
    repo = "test/ocr"
    video_strategy = "s"
    [models.toxicity]
    architecture = "bert"
    repo = "test/toxic-bert"

    [strategies.s]
    [[strategies.s.sampling]]
    step = 1
    method = "uniform"
    count = 4

    [pipelines.read_and_check]
    steps = [
        { model = "ocr", order = 1 },
        { model = "toxicity", order = 2, input = "ocr" },
    ]
"#;

#[tokio::test]
async fn a_pipeline_classifies_the_text_an_earlier_step_reads() {
    let f = fixture_with(OCR_PIPELINE).await;
    let check = |input: &Input, run: Run| f.engine.check_submission(input, &run);
    let video = Input::Video(apollo_domain::Url {
        main: "https://example.com/a.mp4".into(),
        fallback: None,
    });
    // The text model takes the text the OCR model reads, so the pipeline
    // takes what the OCR model takes: images and video.
    let pipeline = || Run::Pipeline("read_and_check".into());
    check(&image(), pipeline()).unwrap();
    check(&video, pipeline()).unwrap();
    check(&image(), Run::Models(vec!["ocr".into()])).unwrap();
    for (input, run, needle) in [
        (
            Input::Text("hi".into()),
            pipeline(),
            "is an image model and does not take text input",
        ),
        (
            image(),
            Run::Models(vec!["toxicity".into()]),
            "is a text model and does not take image input",
        ),
    ] {
        let err = check(&input, run).unwrap_err().to_string();
        assert!(err.contains(needle), "{err}");
    }

    let catalog = f.engine.catalog().await.unwrap();
    let ocr = catalog.models.iter().find(|m| m.name == "ocr").unwrap();
    assert_eq!(ocr.output, apollo_domain::OutputKind::Text);
    assert_eq!(ocr.accepts, [MediaKind::Image, MediaKind::Video]);
    assert!(ocr.labels.is_empty());
    let steps = &catalog.pipelines[0].steps;
    assert_eq!(steps[0].input, None);
    assert_eq!(steps[1].input.as_deref(), Some("ocr"));
}

const LIMITED: &str = r#"
    [app]
    max_pending = 4
    [models.ocr]
    architecture = "vit"
    repo = "test/ocr"
    max_pending = 2
    [models.nsfw]
    architecture = "vit"
    repo = "test/nsfw"
"#;

#[tokio::test]
async fn a_model_limit_refuses_only_that_models_work() {
    let f = fixture_with(LIMITED).await;
    let labels = |names: &[&str]| names.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let reason = |r: Result<crate::Reservation, EngineError>| match r {
        Err(EngineError::Overloaded { reason, .. }) => Some(reason),
        Err(e) => panic!("unexpected error {e}"),
        Ok(_) => None,
    };
    let ocr = Run::Models(labels(&["ocr"]));

    // Two OCR inputs fill its limit; the third is refused, nsfw still fits.
    let first = f.engine.reserve(&labels(&["ocr"])).unwrap();
    let _second = f.engine.reserve(&labels(&["ocr"])).unwrap();
    assert_eq!(
        reason(f.engine.reserve(&labels(&["ocr"]))),
        Some(crate::Overload::Model)
    );
    assert!(matches!(
        f.engine.check_submission(&image(), &ocr),
        Err(EngineError::Overloaded {
            reason: crate::Overload::Model,
            ..
        })
    ));
    let nsfw = f.engine.reserve(&labels(&["nsfw"])).unwrap();
    // An input running both models counts toward both.
    assert_eq!(
        reason(f.engine.reserve(&labels(&["nsfw", "ocr"]))),
        Some(crate::Overload::Model)
    );

    let status = f.engine.status().await.unwrap();
    let admitted = |label: &str| {
        status
            .models
            .iter()
            .find(|m| m.label == label)
            .unwrap()
            .admitted
    };
    assert_eq!(
        (status.items_in_flight, admitted("ocr"), admitted("nsfw")),
        (3, 2, 1)
    );

    // A finished input gives its place back.
    drop(first);
    f.engine.check_submission(&image(), &ocr).unwrap();
    let _third = f.engine.reserve(&labels(&["ocr"])).unwrap();

    // [app].max_pending (4) still caps the total, whatever the model.
    let _fourth = f.engine.reserve(&labels(&["nsfw"])).unwrap();
    assert_eq!(
        reason(f.engine.reserve(&labels(&["nsfw"]))),
        Some(crate::Overload::Queue)
    );

    drop(nsfw);
    let text = f.engine.metrics_text().await.unwrap();
    assert!(
        text.contains("apollo_submissions_overloaded_total{reason=\"model\"} 1"),
        "{text}"
    );
    assert!(
        text.contains("apollo_model_items_admitted{model=\"ocr\"} 2"),
        "{text}"
    );
}

/// An engine on `LIMITED` whose task writes wait for `release`.
async fn held_engine(
    fail: bool,
) -> (
    Engine,
    Arc<dyn Storage>,
    Arc<tokio::sync::Semaphore>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(&dir, LIMITED);
    let storage = apollo_storage::open(&config.database).await.unwrap();
    let (held, release) = crate::testing::HeldCreate::new(storage.clone(), fail);
    let engine = Engine::new(Arc::new(config), Arc::new(held), None);
    (engine, storage, release, dir)
}

/// A submission that fails its fetch at once (local files are refused), so
/// its task finishes without a model or the network.
fn local_file_submission() -> Submission {
    Submission {
        input: Input::Image(apollo_domain::Url {
            main: "file:///nonexistent/apollo-test.png".into(),
            fallback: None,
        }),
        run: Run::Models(vec!["nsfw".into()]),
        metadata: Default::default(),
        request_id: None,
    }
}

async fn in_flight_drops_to_zero(engine: &Engine) {
    for _ in 0..200 {
        if engine.status().await.unwrap().items_in_flight == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("in_flight never returned to 0");
}

#[tokio::test]
async fn a_submission_dropped_while_storing_still_runs_its_task() {
    let (engine, storage, release, _dir) = held_engine(false).await;

    // The client gives up while the task is being written.
    let gave_up = tokio::time::timeout(
        Duration::from_millis(100),
        engine.submit(local_file_submission(), None),
    )
    .await;
    assert!(
        gave_up.is_err(),
        "the write is held, so the submit can't finish"
    );
    assert_eq!(engine.status().await.unwrap().items_in_flight, 1);

    // The write then finishes, the task runs to its end, and the reservation
    // comes back.
    release.add_permits(1);
    in_flight_drops_to_zero(&engine).await;
    let tasks = storage.load_incomplete_tasks().await.unwrap();
    assert!(tasks.is_empty(), "nothing is left unrun: {tasks:?}");
    let all = storage
        .list_tasks(&TaskQuery {
            owner: None,
            states: Vec::new(),
            created_after: None,
            created_before: None,
            limit: 10,
            after: None,
        })
        .await
        .unwrap();
    assert_eq!(all.tasks.len(), 1);
    assert_eq!(
        all.tasks[0].state,
        TaskState::Failed,
        "local files are refused"
    );
}

#[tokio::test]
async fn a_task_that_cant_be_stored_gives_its_reservation_back() {
    let (engine, _storage, release, _dir) = held_engine(true).await;
    release.add_permits(1);
    let err = engine
        .submit(local_file_submission(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Storage(_)), "{err}");
    assert_eq!(engine.status().await.unwrap().items_in_flight, 0);

    // Also when the caller went away before the write failed.
    let gave_up = tokio::time::timeout(
        Duration::from_millis(100),
        engine.submit(local_file_submission(), None),
    )
    .await;
    assert!(gave_up.is_err());
    release.add_permits(1);
    in_flight_drops_to_zero(&engine).await;
}

#[tokio::test]
async fn a_cancelled_item_waiting_for_a_slot_gives_back_its_place() {
    let f = fixture_with(
        r#"
        [app]
        max_concurrent = 1
        [models.nsfw]
        architecture = "vit"
        repo = "test/nsfw"
        max_pending = 1
        "#,
    )
    .await;
    // The only slot is taken, so the submitted item waits for it.
    let held = f.engine.inner.gate.clone().acquire(0).await;
    let task = f
        .engine
        .submit(local_file_submission(), None)
        .await
        .unwrap();
    for _ in 0..200 {
        if f.engine.status().await.unwrap().slots_waiting == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        f.engine.status().await.unwrap().slots_waiting,
        1,
        "the item is waiting for the slot"
    );
    assert!(matches!(
        f.engine
            .check_submission(&image(), &Run::Models(vec!["nsfw".into()])),
        Err(EngineError::Overloaded { .. })
    ));

    // Cancelling frees its place at once, though no slot has come free.
    f.engine.cancel(&task.id).await.unwrap();
    in_flight_drops_to_zero(&f.engine).await;
    let status = f.engine.status().await.unwrap();
    assert_eq!((status.slots_busy, status.slots_waiting), (1, 0));
    f.engine
        .check_submission(&image(), &Run::Models(vec!["nsfw".into()]))
        .unwrap();
    let stored = f.storage.get_task(&task.id).await.unwrap().unwrap();
    assert_eq!(stored.state, TaskState::Cancelled);
    assert_eq!(stored.items[0].state, ItemState::Cancelled);

    drop(held);
    assert_eq!(f.engine.status().await.unwrap().slots_busy, 0);
}

#[tokio::test]
async fn a_repeat_waits_for_a_dropped_submission_to_be_stored() {
    let (engine, storage, release, _dir) = held_engine(false).await;
    let keyed = || Submission {
        request_id: Some("r".into()),
        ..local_file_submission()
    };

    // The first caller gives up while its task is being stored.
    let gave_up =
        tokio::time::timeout(Duration::from_millis(100), engine.submit(keyed(), None)).await;
    assert!(gave_up.is_err());

    // A repeat, and the server's lookup, wait for that store rather than
    // treating the repeat as new.
    let repeat = tokio::spawn({
        let engine = engine.clone();
        async move { engine.submit(keyed(), None).await }
    });
    let lookup = tokio::spawn({
        let engine = engine.clone();
        async move { engine.find_request_settled(None, "r").await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!repeat.is_finished() && !lookup.is_finished());
    // Another key doesn't wait for that store.
    let other = tokio::time::timeout(
        Duration::from_millis(500),
        engine.find_request_settled(None, "other"),
    )
    .await
    .expect("an unrelated key isn't blocked");
    assert_eq!(other.unwrap(), None);

    release.add_permits(1); // only the first store; the repeat stores nothing
    // Bounded, so a broken lock fails here instead of hanging on the held store.
    let repeat = tokio::time::timeout(Duration::from_secs(10), repeat)
        .await
        .expect("the repeat finished")
        .unwrap()
        .unwrap();
    assert!(repeat.existing, "{repeat:?}");
    let stored = storage.find_task_by_request(None, "r").await.unwrap();
    assert_eq!(stored.as_deref(), Some(repeat.id.as_str()));
    assert_eq!(lookup.await.unwrap().unwrap(), stored);
    in_flight_drops_to_zero(&engine).await;
}

#[tokio::test]
async fn a_cancel_landing_with_a_slot_wins() {
    let f = fixture_with("[app]\nmax_concurrent = 1\n").await;
    let cancel = std::sync::atomic::AtomicBool::new(false);
    for _ in 0..50 {
        let held = f.engine.inner.gate.clone().acquire(0).await;
        let mut wait = Box::pin(f.engine.slot_or_cancel(0, &cancel));
        assert!(futures::poll!(wait.as_mut()).is_pending());
        // The cancel and the free slot both arrive before the wait runs again.
        cancel.store(true, std::sync::atomic::Ordering::SeqCst);
        f.engine.inner.cancelled.notify_waiters();
        drop(held);
        assert!(
            wait.await.is_none(),
            "a cancelled item never takes the slot"
        );
        assert_eq!(
            f.engine.inner.gate.snapshot(),
            (1, 0, 0),
            "and it isn't lost"
        );
        cancel.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn a_cancel_ends_a_retry_backoff_at_once() {
    let f = fixture().await;
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sleeping = tokio::spawn({
        let engine = f.engine.clone();
        let cancel = cancel.clone();
        async move {
            engine
                .sleep_or_cancel(Duration::from_secs(30), &cancel)
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.store(true, std::sync::atomic::Ordering::SeqCst);
    f.engine.inner.cancelled.notify_waiters();
    let cancelled = tokio::time::timeout(Duration::from_secs(2), sleeping)
        .await
        .expect("the backoff ended on cancel")
        .unwrap();
    assert!(cancelled);
    // Uncancelled, it sleeps the whole time.
    let plain = std::sync::atomic::AtomicBool::new(false);
    assert!(
        !f.engine
            .sleep_or_cancel(Duration::from_millis(20), &plain)
            .await
    );
}

const NSFW_ONLY: &str = r#"
    [models.nsfw]
    architecture = "vit"
    repo = "test/nsfw"
"#;

/// Run `task` as resumed work (the way `recover` starts it), to its end: the
/// run claims it first, if it still may (otherwise its claim is one nobody
/// stored, and its writes are fenced out).
async fn run_resumed(engine: &Engine, task: Task) {
    let token = uuid::Uuid::new_v4().to_string();
    let sent = Instant::now();
    let _ = engine
        .inner
        .storage
        .claim_task(
            &task.id,
            &engine.lease(&token),
            apollo_storage::ClaimMode::Force,
            Duration::ZERO,
        )
        .await
        .unwrap();
    let run = engine.register(&task.id, token, sent).unwrap();
    let reservations = task
        .items
        .iter()
        .map(|item| engine.reserve_resumed(&item.models))
        .collect();
    engine.run_task(task, run, reservations).await;
}

#[tokio::test]
async fn a_late_runner_leaves_a_finished_task_as_it_ended() {
    let f = fixture_with(NSFW_ONLY).await;
    let late = |id: &str| {
        let mut t = task(id, TaskState::Processing, ItemState::Queued, None);
        t.items[0].models = vec!["nsfw".into()];
        t.items[0].input = local_file_submission().input;
        t
    };
    // Each finished after this runner loaded it: one cancelled (elsewhere,
    // so this engine has no signal for it), one completed.
    for id in ["cancelled", "completed"] {
        f.storage.create_task(&late(id), None, None).await.unwrap();
    }
    assert!(f.storage.cancel_task_if_active("cancelled").await.unwrap());
    assert!(
        f.storage
            .set_task_state("completed", TaskState::Completed, Fence::Unchecked)
            .await
            .unwrap()
    );

    for id in ["cancelled", "completed"] {
        run_resumed(&f.engine, late(id)).await;
    }
    let cancelled = f.storage.get_task("cancelled").await.unwrap().unwrap();
    assert_eq!(
        cancelled.state,
        TaskState::Cancelled,
        "the start write lost"
    );
    assert!(cancelled.finished_at.is_some());
    // Its item is finished, as cancelled, without being run (its refused
    // input would have failed it).
    assert_eq!(cancelled.items[0].state, ItemState::Cancelled);
    let completed = f.storage.get_task("completed").await.unwrap().unwrap();
    assert_eq!(completed.state, TaskState::Completed);
    assert_eq!(completed.items[0].state, ItemState::Queued, "nothing ran");
    assert_eq!(f.engine.status().await.unwrap().items_in_flight, 0);
}

#[tokio::test]
async fn a_late_runner_leaves_a_finished_item_alone() {
    let f = fixture_with(NSFW_ONLY).await;
    // This runner's copy says queued; meanwhile another runner finished it.
    let mut late = Task::new(
        "t",
        vec![Item::new(image(), vec!["nsfw".into()], None)],
        None,
    );
    late.items[0].input = local_file_submission().input;
    f.storage.create_task(&late, None, None).await.unwrap();
    assert!(
        f.storage
            .set_item_state(
                "t",
                0,
                ItemState::Completed,
                None,
                Fence::Unchecked,
                PLANTED
            )
            .await
            .unwrap()
    );

    run_resumed(&f.engine, late).await;
    let stored = f.storage.get_task("t").await.unwrap().unwrap();
    // Had it run, its refused input would have failed it.
    assert_eq!(stored.items[0].state, ItemState::Completed);
    assert_eq!(stored.items[0].error, None);
    assert!(
        stored.items[0]
            .results
            .values()
            .all(|r| r.state != ModelState::Failed),
        "{stored:?}"
    );
    assert_eq!(stored.state, TaskState::Completed, "the rollup still runs");
}

#[tokio::test]
async fn a_keyed_tasks_id_comes_from_its_owner_and_key() {
    let id = crate::request_task_id;
    assert_eq!(id(Some("alice"), "r"), id(Some("alice"), "r"));
    let ids = [
        id(Some("alice"), "r"),
        id(Some("bob"), "r"),
        id(Some("alice"), "s"),
        id(None, "r"),
        id(Some(""), "r"),
        // Where the owner ends can't be moved into the key.
        id(Some("al"), "icer"),
    ];
    let distinct: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(distinct.len(), ids.len(), "{ids:?}");
    for i in &ids {
        uuid::Uuid::parse_str(i).unwrap();
    }

    // A task stored under the key's id after this instance looked for the
    // key (by another instance) is the one the submission gets.
    let f = fixture_with(NSFW_ONLY).await;
    let first = Task::new(
        id(None, "k"),
        vec![Item::new(image(), vec!["nsfw".into()], None)],
        None,
    );
    f.storage.create_task(&first, None, None).await.unwrap();
    let keyed = Submission {
        request_id: Some("k".into()),
        ..local_file_submission()
    };
    let got = f.engine.submit(keyed, None).await.unwrap();
    assert_eq!((got.id.as_str(), got.existing), (first.id.as_str(), true));
    assert_eq!(f.engine.status().await.unwrap().items_in_flight, 0);
    let text = f.engine.metrics_text().await.unwrap();
    assert!(
        text.contains("apollo_submissions_total{outcome=\"existing\"} 1"),
        "{text}"
    );
}

#[tokio::test]
async fn only_files_directly_in_the_upload_dir_are_read_or_removed() {
    let f = fixture_with(NSFW_ONLY).await;
    let dir = f.engine.ensure_upload_dir().unwrap();
    for (path, staged) in [
        (dir.join("a.bin"), true),
        (dir.join("sub").join("a.bin"), false),
        (dir.join("..").join("a.bin"), false),
        (dir.clone(), false),
        (PathBuf::from("/etc/hosts"), false),
    ] {
        assert_eq!(f.engine.is_staged_upload(&path), staged, "{path:?}");
    }

    // A task row pointing elsewhere: the file is neither read nor removed.
    let elsewhere = tempfile::tempdir().unwrap();
    let file = elsewhere.path().join("private.png");
    std::fs::write(&file, b"keep me").unwrap();
    let mut row = task(
        "t",
        TaskState::Queued,
        ItemState::Queued,
        Some(file.clone()),
    );
    row.items[0].models = vec!["nsfw".into()];
    f.storage.create_task(&row, None, None).await.unwrap();
    assert_eq!(f.engine.recover().await.unwrap(), 1);
    in_flight_drops_to_zero(&f.engine).await;
    let stored = f.storage.get_task("t").await.unwrap().unwrap();
    let error = stored.items[0].error.clone().unwrap();
    assert_eq!(error.kind, ErrorKind::Fetch, "{error:?}");
    assert!(error.message.contains("not an upload"), "{error:?}");
    assert_eq!(std::fs::read(&file).unwrap(), b"keep me");
}

#[tokio::test]
async fn a_watch_sees_changes_nobody_announced() {
    let f = fixture().await;
    f.storage
        .create_task(
            &task("t", TaskState::Processing, ItemState::Processing, None),
            None,
            None,
        )
        .await
        .unwrap();
    let mut watch = f.engine.watch("t");
    assert_eq!(
        watch.next().await.unwrap().unwrap().state,
        TaskState::Processing
    );
    // Written past this engine, as another instance would: not announced.
    f.storage
        .set_item_state(
            "t",
            0,
            ItemState::Completed,
            None,
            Fence::Unchecked,
            PLANTED,
        )
        .await
        .unwrap();
    f.storage
        .set_task_state("t", TaskState::Completed, Fence::Unchecked)
        .await
        .unwrap();
    let seen = tokio::time::timeout(Duration::from_secs(10), watch.next())
        .await
        .expect("the watch reads the task again")
        .unwrap()
        .unwrap();
    assert_eq!(seen.state, TaskState::Completed);
}

/// A webhook receiver that records each event: its kind, task and task state.
#[derive(Default)]
struct Recorder(std::sync::Mutex<Vec<(crate::EventKind, String, TaskState)>>);

#[async_trait::async_trait]
impl crate::WebhookSink for Recorder {
    async fn deliver(&self, event: &crate::TaskEvent) -> Result<(), crate::WebhookError> {
        let mut events = self.0.lock().unwrap();
        events.push((event.kind, event.task.id.clone(), event.task.state));
        Ok(())
    }

    fn target(&self) -> String {
        "test://recorder".into()
    }
}

impl Recorder {
    fn finished(&self, task_id: &str) -> Vec<TaskState> {
        let events = self.0.lock().unwrap();
        events
            .iter()
            .filter(|(kind, id, _)| *kind == crate::EventKind::Finished && id == task_id)
            .map(|(_, _, state)| *state)
            .collect()
    }
}

const NSFW: &str = "[models.nsfw]\narchitecture = \"vit\"\nrepo = \"test/nsfw\"\n";

#[tokio::test]
async fn a_rollup_that_fails_still_finishes_the_task_and_sends_its_finished_webhook() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(
        &dir,
        &format!("[webhook]\nurl = \"http://127.0.0.1:1\"\n{NSFW}"),
    );
    let storage = apollo_storage::open(&config.database).await.unwrap();
    // The database drops the connection each time the task's finish is written,
    // three times in a row.
    let failing = crate::testing::FailFinish::new(storage.clone(), 3);
    let sink = Arc::new(Recorder::default());
    let engine = Engine::new(Arc::new(config), Arc::new(failing), Some(sink.clone()));
    let id = engine
        .submit(local_file_submission(), None)
        .await
        .unwrap()
        .id;

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while sink.finished(&id).is_empty() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        sink.finished(&id),
        [TaskState::Failed],
        "one finished event, for the failed task"
    );
    let stored = storage.get_task(&id).await.unwrap().unwrap();
    assert_eq!(stored.state, TaskState::Failed, "the task was rolled up");
    let pending = storage.items_pending_webhook().await.unwrap();
    assert!(pending.is_empty(), "nothing left pending: {pending:?}");
}

#[tokio::test]
async fn with_webhooks_off_nothing_is_kept_to_send_later() {
    let f = fixture_with(NSFW).await;
    let id = f
        .engine
        .submit(local_file_submission(), None)
        .await
        .unwrap()
        .id;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let task = f.storage.get_task(&id).await.unwrap().unwrap();
        if task.state == TaskState::Failed || std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The item's finish is recorded as handled (in the write that finished
    // it), so turning webhooks on later doesn't send events for tasks that
    // finished while they were off.
    let pending = f.storage.items_pending_webhook().await.unwrap();
    assert!(
        pending.is_empty(),
        "left pending with webhooks off: {pending:?}"
    );
}

/// A webhook receiver for the delivery tests: records events as [`Recorder`]
/// does, and of the finished ones holds the first for `hold_first` and
/// refuses the next `fail`.
#[derive(Default)]
struct Flaky {
    recorder: Recorder,
    hold_first: Duration,
    fail: std::sync::atomic::AtomicUsize,
    /// Finished events it was sent.
    attempts: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl crate::WebhookSink for Flaky {
    async fn deliver(&self, event: &crate::TaskEvent) -> Result<(), crate::WebhookError> {
        use std::sync::atomic::Ordering;
        if event.kind != crate::EventKind::Finished {
            return self.recorder.deliver(event).await;
        }
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(self.hold_first).await;
        }
        if self
            .fail
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(crate::WebhookError("refused".into()));
        }
        self.recorder.deliver(event).await
    }

    fn target(&self) -> String {
        "test://recorder".into()
    }
}

impl Flaky {
    fn attempts(&self) -> usize {
        self.attempts.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// An engine on SQLite with webhook `sink`, and `toml` besides the nsfw model.
async fn engine_with_sink(
    toml: &str,
    sink: Arc<dyn crate::WebhookSink>,
) -> (Engine, Arc<dyn Storage>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(&dir, &format!("{toml}{NSFW}"));
    let storage = apollo_storage::open(&config.database).await.unwrap();
    let engine = Engine::new(Arc::new(config), storage.clone(), Some(sink));
    (engine, storage, dir)
}

/// Wait until `id`'s item has finished, and its finished webhook is no longer
/// pending.
async fn wait_delivered(storage: &Arc<dyn Storage>, id: &str, limit: Duration) {
    let deadline = Instant::now() + limit;
    loop {
        let task = storage.get_task(id).await.unwrap().unwrap();
        let pending = storage.items_pending_webhook().await.unwrap();
        if crate::aggregate::item_terminal(task.items[0].state)
            && !pending.iter().any(|p| p.task_id == id)
        {
            return;
        }
        assert!(Instant::now() < deadline, "still pending: {pending:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn redelivery_skips_an_item_being_delivered_here() {
    let sink = Arc::new(Flaky {
        hold_first: Duration::from_millis(2500),
        ..Default::default()
    });
    let webhook = "[webhook]\nurl = \"http://127.0.0.1:1\"\nredelivery_secs = 1\n";
    let (engine, storage, _dir) = engine_with_sink(webhook, sink.clone()).await;
    let id = engine
        .submit(local_file_submission(), None)
        .await
        .unwrap()
        .id;
    // The first delivery takes 2.5 s, through two rounds of redelivery.
    wait_delivered(&storage, &id, Duration::from_secs(10)).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(sink.recorder.finished(&id), [TaskState::Failed]);
    assert_eq!(sink.attempts(), 1, "delivered again while being delivered");
}

#[tokio::test]
async fn a_restart_delivers_what_the_last_process_had_claimed() {
    let sink = Arc::new(Recorder::default());
    let (engine, storage, _dir) = engine_with_sink("", sink.clone()).await;
    // Finished by the last process, which was delivering its webhook (for an
    // hour yet) when it stopped.
    storage
        .create_task(
            &task("t", TaskState::Processing, ItemState::Processing, None),
            None,
            None,
        )
        .await
        .unwrap();
    let claim = apollo_storage::Delivery::Claim {
        holder: "old/proc",
        target: "test://recorder",
        ttl: Duration::from_secs(3600),
    };
    assert!(
        storage
            .set_item_state("t", 0, ItemState::Completed, None, Fence::Unchecked, claim)
            .await
            .unwrap()
    );
    assert!(
        storage
            .set_task_state("t", TaskState::Completed, Fence::Unchecked)
            .await
            .unwrap()
    );

    // One process per SQLite database: its start sends it at once.
    engine.recover().await.unwrap();
    wait_delivered(&storage, "t", Duration::from_secs(5)).await;
    assert_eq!(sink.finished("t"), [TaskState::Completed]);
}

#[tokio::test]
async fn a_failed_delivery_hands_its_claim_back() {
    let sink = Arc::new(Flaky {
        fail: 3.into(),
        ..Default::default()
    });
    let (engine, storage, _dir) = engine_with_sink("", sink.clone()).await;
    let id = engine
        .submit(local_file_submission(), None)
        .await
        .unwrap()
        .id;
    // Three attempts, over about 4 s, then the claim is handed back: anyone
    // may take it.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let taken = storage
            .claim_pending_webhooks(
                "other/proc",
                None,
                apollo_storage::ClaimMode::Open,
                Duration::from_secs(60),
            )
            .await
            .unwrap();
        if taken.iter().any(|p| p.task_id == id) {
            break;
        }
        assert!(Instant::now() < deadline, "the claim wasn't handed back");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(sink.attempts(), 3);
    assert!(sink.recorder.finished(&id).is_empty());
}

#[tokio::test]
async fn turning_webhooks_off_settles_this_instances_deliveries() {
    let sink = Arc::new(Flaky {
        fail: 1.into(),
        ..Default::default()
    });
    let (engine, storage, dir) = engine_with_sink("", sink.clone()).await;
    let id = engine
        .submit(local_file_submission(), None)
        .await
        .unwrap()
        .id;
    // The first attempt fails; before the next, webhooks are turned off.
    let deadline = Instant::now() + Duration::from_secs(5);
    while sink.attempts() == 0 {
        assert!(Instant::now() < deadline, "never delivered");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    engine.reload(config_in(&dir, NSFW), None).await.unwrap();
    // The next attempt finds them off, and marks the item: nothing is kept.
    wait_delivered(&storage, &id, Duration::from_secs(5)).await;
    assert_eq!(sink.attempts(), 1);
}

#[tokio::test]
async fn a_rollup_that_fails_is_tried_again() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(&dir, NSFW);
    let storage = apollo_storage::open(&config.database).await.unwrap();
    // Twice: the rollup as the item finishes, and the one after the task's
    // run. With no webhook, only trying again finishes the task.
    let failing = crate::testing::FailFinish::new(storage.clone(), 2);
    let engine = Engine::new(Arc::new(config), Arc::new(failing), None);
    let id = engine
        .submit(local_file_submission(), None)
        .await
        .unwrap()
        .id;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let state = storage.get_task(&id).await.unwrap().unwrap().state;
        if state == TaskState::Failed {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the task stayed {state:?} after its rollups failed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// An engine with one slot and a one-second lease, on SQLite behind a
/// [`Stalled`](crate::testing::Stalled) store. With `shared`, the store says
/// it is shared, so the engine renews its leases as on SurrealDB.
async fn stalled_engine(
    shared: bool,
) -> (
    Engine,
    Arc<dyn Storage>,
    crate::testing::StallGate,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config_in(&dir, &format!("[app]\nmax_concurrent = 1\n{NSFW}"));
    config.database.lease_secs = 1;
    let sqlite = apollo_storage::open(&config.database).await.unwrap();
    let (stalled, gate) = if shared {
        crate::testing::Stalled::new_shared(sqlite.clone())
    } else {
        crate::testing::Stalled::new(sqlite.clone())
    };
    let engine = Engine::new(Arc::new(config), Arc::new(stalled), None);
    (engine, sqlite, gate, dir)
}

fn in_flight(engine: &Engine) -> usize {
    engine
        .inner
        .in_flight
        .load(std::sync::atomic::Ordering::SeqCst)
}

/// Whether the run of `id` has lost its claim; `None` when there is none.
fn run_lost(engine: &Engine, id: &str) -> Option<bool> {
    engine.inner.runs.lock().unwrap().get(id).map(|r| r.lost())
}

/// Whether `done` turned true within `limit`.
async fn wait_within(limit: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    while !done() {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    true
}

#[tokio::test]
async fn a_run_without_confirmed_renewals_stops_within_its_lease() {
    let (engine, sqlite, gate, _dir) = stalled_engine(true).await;
    // The only slot is taken, so the item waits for it.
    let slot = engine.inner.gate.clone().acquire(0).await;
    let id = engine
        .submit(local_file_submission(), None)
        .await
        .unwrap()
        .id;
    assert!(wait_within(Duration::from_secs(2), || in_flight(&engine) == 1).await);
    // Renewals that answer keep it going past its first lease.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(run_lost(&engine, &id), Some(false));

    // The database stops answering: the run goes on for a while, then stops
    // before its lease can lapse.
    gate.shut();
    let shut = Instant::now();
    let stopped = wait_within(Duration::from_secs(2), || {
        in_flight(&engine) == 0 && engine.inner.runs.lock().unwrap().is_empty()
    })
    .await;
    let took = shut.elapsed();
    assert!(stopped, "the run never stopped");
    assert!(
        took >= Duration::from_millis(400) && took < Duration::from_secs(1),
        "stopped {took:?} after the database went away"
    );
    // It wrote nothing: the item wasn't cancelled.
    let stored = sqlite.get_task(&id).await.unwrap().unwrap();
    assert_eq!(
        (stored.state, stored.items[0].state),
        (TaskState::Processing, ItemState::Queued)
    );
    // Its claim comes back once the database answers, not before.
    let grace = Duration::from_secs(10);
    let open = || async {
        sqlite
            .claimable_tasks(
                apollo_storage::ClaimMode::Open,
                engine.node(),
                &[],
                grace,
                None,
                10,
            )
            .await
            .unwrap()
            .iter()
            .any(|c| c.id == id)
    };
    assert!(!open().await, "handed back while the database was away");
    gate.open();
    let deadline = Instant::now() + Duration::from_secs(1);
    while !(open().await && engine.inner.unreleased.lock().unwrap().is_empty()) {
        assert!(Instant::now() < deadline, "the claim wasn't handed back");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Handed back: the next claim counts no attempt.
    let next = apollo_storage::Lease {
        token: "next",
        node: engine.node(),
        process: "p",
        ttl: Duration::from_secs(60),
    };
    assert_eq!(
        sqlite
            .claim_task(&id, &next, apollo_storage::ClaimMode::Open, grace)
            .await
            .unwrap(),
        Some(0)
    );
    drop(slot);
}

#[tokio::test]
async fn renewals_that_answer_keep_a_run_going() {
    let (engine, sqlite, _gate, _dir) = stalled_engine(true).await;
    let slot = engine.inner.gate.clone().acquire(0).await;
    let id = engine
        .submit(local_file_submission(), None)
        .await
        .unwrap()
        .id;
    // Three leases on, renewals still confirm the claim.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(run_lost(&engine, &id), Some(false));
    assert_eq!(in_flight(&engine), 1);

    // With the slot, the item runs to its end, as without leases.
    drop(slot);
    assert!(
        wait_within(Duration::from_secs(5), || {
            in_flight(&engine) == 0 && engine.inner.runs.lock().unwrap().is_empty()
        })
        .await
    );
    let stored = sqlite.get_task(&id).await.unwrap().unwrap();
    assert_eq!(stored.state, TaskState::Failed, "local files are refused");
    let pending = sqlite.items_pending_webhook().await.unwrap();
    assert!(
        pending.iter().all(|p| p.task_id != id),
        "its finished webhook is left pending: {pending:?}"
    );
}

#[tokio::test]
async fn sqlite_runs_have_no_renewal_limit() {
    let (engine, sqlite, gate, _dir) = stalled_engine(false).await;
    let slot = engine.inner.gate.clone().acquire(0).await;
    let id = engine
        .submit(local_file_submission(), None)
        .await
        .unwrap()
        .id;
    // Nothing renews a lease on SQLite, so an outage stops nothing.
    gate.shut();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(run_lost(&engine, &id), Some(false));

    gate.open();
    drop(slot);
    assert!(
        wait_within(Duration::from_secs(5), || {
            in_flight(&engine) == 0 && engine.inner.runs.lock().unwrap().is_empty()
        })
        .await
    );
    let stored = sqlite.get_task(&id).await.unwrap().unwrap();
    assert_eq!(stored.state, TaskState::Failed);
}

#[tokio::test]
async fn renewal_cap_keeps_to_the_deadline() {
    let ms = Duration::from_millis;
    let ns = Duration::from_nanos;
    // (lease, renewal period, how long a run goes unconfirmed, the floor)
    for (lease_secs, period, limit, floor) in [
        (30, ms(10_000), ms(25_000), ms(5_000)),
        (1, ns(333_333_333), ns(833_333_333), ns(166_666_666)),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config_in(&dir, "");
        config.database.lease_secs = lease_secs;
        let storage = apollo_storage::open(&config.database).await.unwrap();
        let engine = Engine::new(Arc::new(config), storage, None);
        let ttl = Duration::from_secs(lease_secs.into());
        let mut n = 0;
        // A run whose last confirmed renewal was sent `ago`.
        let mut run = |ago: Duration| {
            n += 1;
            engine
                .register(&n.to_string(), "t".into(), Instant::now() - ago)
                .unwrap()
        };
        let cap = |runs: &[&crate::lease::Running]| {
            let ctls: Vec<_> = runs.iter().map(|r| r.ctl.clone()).collect();
            engine.renewal_cap(&ctls)
        };

        assert_eq!(engine.unconfirmed_limit(), limit);
        // A renewal period, but no further than the deadline, and at least
        // the floor.
        let fresh = run(Duration::ZERO);
        assert_eq!(cap(&[&fresh]), period);
        let late = run(ttl * 2 / 3);
        assert_eq!(cap(&[&late]), floor);
        let lapsed = run(ttl * 4 / 3);
        assert_eq!(cap(&[&lapsed]), floor);
        // 8/30 of the lease left until the deadline.
        let between = run(ttl * 17 / 30);
        let left = cap(&[&between]);
        assert!(
            left <= ttl * 8 / 30 && left > ttl * 7 / 30,
            "{left:?} of {ttl:?}"
        );
        // The earliest deadline among them.
        let both = cap(&[&fresh, &between]);
        assert!(both <= left && both > ttl * 7 / 30, "{both:?} of {ttl:?}");
        // A confirmation moves the deadline on, never back.
        lapsed.ctl.confirm(Instant::now());
        assert_eq!(cap(&[&lapsed]), period);
        lapsed.ctl.confirm(Instant::now() - ttl);
        assert_eq!(cap(&[&lapsed]), period);
    }
}
