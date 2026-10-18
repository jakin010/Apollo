//! Engine-level tests against a real SQLite store. No model is ever loaded: these
//! cover lifecycle paths (recovery, cancellation, cleanup) that run without
//! inference.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use apollo_config::{Config, DatabaseConfig, SqliteConfig};
use apollo_domain::{ErrorKind, Input, Item, ItemState, ModelState, Task, TaskState};
use apollo_storage::Storage;

use crate::{Engine, EngineError};

struct Fixture {
    engine: Engine,
    storage: Arc<dyn Storage>,
    _dir: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config {
        app: Default::default(),
        webhook: None,
        auth: None,
        limits: Default::default(),
        database: DatabaseConfig {
            sqlite: Some(SqliteConfig {
                path: dir.path().join("apollo.db").to_string_lossy().into_owned(),
                ..Default::default()
            }),
            ..Default::default()
        },
        cache: None,
        strategies: Default::default(),
        pipelines: Default::default(),
        models: Default::default(),
    };
    config.app.cache_dir = Some(dir.path().to_string_lossy().into_owned());
    let storage = apollo_storage::open(&config.database).await.unwrap();
    let engine = Engine::new(Arc::new(config), storage.clone(), None);
    Fixture {
        engine,
        storage,
        _dir: dir,
    }
}

fn task(id: &str, state: TaskState, item_state: ItemState, upload: Option<PathBuf>) -> Task {
    let input = match upload {
        Some(path) => Input::Bytes { path, video: false },
        None => Input::Image(apollo_domain::Url {
            main: "https://example.com/a.png".into(),
            fallback: None,
        }),
    };
    Task {
        id: id.into(),
        state,
        items: vec![Item {
            input,
            models: vec!["m".into()],
            pipeline: None,
            state: item_state,
            results: BTreeMap::new(),
            error: None,
            retries: 0,
        }],
    }
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
