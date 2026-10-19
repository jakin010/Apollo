//! Engine-level tests against a real SQLite store. No model is ever loaded: these
//! cover lifecycle paths (recovery, cancellation, cleanup) that run without
//! inference.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use apollo_config::{Config, DatabaseConfig, SqliteConfig};
use apollo_domain::{
    ErrorKind, Input, Item, ItemState, MediaKind, ModelResult, ModelState, Task, TaskState,
};
use apollo_storage::Storage;

use crate::{Engine, EngineError, Run, Submission};

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
        .create_task(&task(
            "t",
            TaskState::Processing,
            ItemState::Processing,
            Some(upload.clone()),
        ))
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
        .create_task(&task("q", TaskState::Queued, ItemState::Queued, None))
        .await
        .unwrap();
    f.storage
        .create_task(&task("c", TaskState::Completed, ItemState::Completed, None))
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
        .create_task(&task(
            "t",
            TaskState::Processing,
            ItemState::Processing,
            Some(upload.clone()),
        ))
        .await
        .unwrap();

    f.engine.cancel("t").await.unwrap();
    // The item is still running: its upload must survive.
    f.engine.rollup_task_state("t").await;
    assert!(upload.exists());

    // The item reaches its cancellation checkpoint (what cancel_item persists).
    f.storage
        .set_item_state("t", 0, ItemState::Cancelled, None)
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

#[tokio::test]
async fn graceful_shutdown_does_not_count_as_a_crash() {
    let f = fixture().await;
    f.storage
        .create_task(&task(
            "t",
            TaskState::Processing,
            ItemState::Processing,
            None,
        ))
        .await
        .unwrap();
    f.storage.increment_attempts("t").await.unwrap();
    f.storage.increment_attempts("t").await.unwrap();

    f.engine.shutdown().await;

    // The next resume lands on 2, not 3.
    assert_eq!(f.storage.increment_attempts("t").await.unwrap(), 2);
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
        .create_task(&task("q", TaskState::Queued, ItemState::Queued, None))
        .await
        .unwrap();
    f.storage
        .create_task(&task("c", TaskState::Completed, ItemState::Completed, None))
        .await
        .unwrap();
    f.storage
        .create_task(&task(
            "c2",
            TaskState::Completed,
            ItemState::Completed,
            None,
        ))
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
    f.storage.create_task(&earlier).await.unwrap();

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
        .create_task(&task("w", TaskState::Queued, ItemState::Queued, None))
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
            .set_item_state("w", 0, ItemState::Processing, None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let done = ModelResult::skipped();
        storage
            .upsert_model_result("w", 0, "m", &done)
            .await
            .unwrap();
        storage
            .set_item_state("w", 0, ItemState::Completed, None)
            .await
            .unwrap();
        storage
            .set_task_state("w", TaskState::Completed)
            .await
            .unwrap();
    });

    let mut states = Vec::new();
    while let Some(t) = watch.next().await.unwrap() {
        states.push(t.state);
    }
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
            .create_task(&task(
                id,
                TaskState::Processing,
                ItemState::Processing,
                None,
            ))
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
        s.set_task_state("fast", TaskState::Failed).await.unwrap();
    });
    let started = std::time::Instant::now();
    let fast = f
        .engine
        .wait_for("fast", Duration::from_secs(10))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fast.state, TaskState::Failed);
    assert!(started.elapsed() < Duration::from_secs(5));

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

    // nsfw: only its timeout changes (read per use) -> kept.
    // category: its labels change (baked into the loaded model) -> reloaded.
    // general: enabled now -> added. A new strategy and pipeline, too.
    let changed = PIPELINE
        .replace(
            "repo = \"test/nsfw\"",
            "repo = \"test/nsfw\"\n    timeout = 99",
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
