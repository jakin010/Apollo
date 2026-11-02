//! Per-instance state behind shared data: request_id locks, uploads on one
//! server's disk, the result cache key, and change notifications.

use super::*;

use apollo_domain::{ErrorKind, Item};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until the task id comes from the request_id (a second create finds the \
            first, also for tasks without an owner)"]
async fn one_request_id_through_two_instances_makes_two_tasks() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", NSFW).await;
        let b = fleet.start("b", NSFW).await;

        /// Submit `owner`'s `request_id` through A, then (a client's retry)
        /// through B while A is still storing the first try. Returns the
        /// number of tasks with that key, whether the retry got the first
        /// task, and whether it was flagged as existing.
        async fn twice(
            fleet: &Fleet,
            a: &Instance,
            b: &Instance,
            owner: Option<&str>,
            request_id: &str,
        ) -> (usize, bool, bool) {
            let keyed = || Submission {
                request_id: Some(request_id.to_string()),
                ..refused()
            };
            let (first, retry) = (keyed(), keyed());
            let (owner_a, owner_b) = (owner.map(String::from), owner.map(String::from));
            let store_a = a.hold_storage(1, |c| matches!(c, Call::CreateTask(_)));
            let store_b = b.hold_storage(1, |c| matches!(c, Call::CreateTask(_)));
            let first = a.spawn(move |e| async move { e.submit(first, owner_a).await });
            store_a.entered("a stores the task").await;
            let retry = b.spawn(move |e| async move { e.submit(retry, owner_b).await });
            store_b.entered("b gets as far as storing a task").await;
            store_a.release();
            let first = first.await.unwrap().unwrap();
            store_b.release();
            let retry = retry.await.unwrap().unwrap();
            a.wait_idle().await;
            b.wait_idle().await;
            fleet.log.delivered("a", &first.id).await;
            if retry.id != first.id {
                fleet.log.delivered("b", &retry.id).await;
            }
            let tasks = fleet
                .tasks(None)
                .await
                .into_iter()
                .filter(|t| {
                    t.owner.as_deref() == owner && t.request_id.as_deref() == Some(request_id)
                })
                .count();
            (tasks, retry.id == first.id, retry.existing)
        }

        let alice = twice(&fleet, &a, &b, Some("alice"), "r").await;
        let nobody = twice(&fleet, &a, &b, None, "r-anon").await;
        assert_eq!(
            [alice, nobody],
            [(1, true, true); 2],
            "the same request_id through two instances made separate tasks, for \
             alice and for no owner: (tasks with the key, the retry got the first \
             task, flagged existing)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until the upload sweep only removes old files"]
async fn a_starting_instance_deletes_an_upload_in_progress() {
    fleet_test(|fleet| async move {
        // Both servers use one cache directory (e.g. shared storage for the
        // model weights).
        let shared = tempfile::tempdir().unwrap();
        let a = fleet.start_in("a", NSFW, shared.path()).await;
        // An upload is arriving at A when B starts.
        let path = a.stage(b"not an image");
        let b = fleet.start_in("b", NSFW, shared.path()).await;
        b.recover().await;
        let kept = path.exists();

        let t = a.submit(uploaded(&path)).await.id;
        a.wait_idle().await;
        fleet.log.delivered("a", &t).await;
        let error = fleet.task(&t).await.items[0].error.clone().unwrap();

        assert_eq!(
            (kept, error.kind),
            (true, ErrorKind::Decode),
            "B's start deleted A's upload in progress: (kept, A's item error \
             kind); A's error: {}",
            error.message
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until uploads carry their instance (others leave them for a grace \
            period, then fail them with a clear error); the lease PR expires A's lease before B \
            starts, so that only this keeps B off the task"]
async fn an_upload_to_a_dead_instance_fails_elsewhere_at_once() {
    fleet_test(|fleet| async move {
        let a = fleet
            .start("a", &format!("[app]\nmax_concurrent = 1\n{NSFW}"))
            .await;
        let slot = a.engine.inner.gate.clone().acquire(0).await;
        let path = a.stage(b"not an image");
        let t = a.submit(uploaded(&path)).await.id;
        wait_until("a's item waits for a slot", || a.slots_waiting() == 1).await;
        let _a_disk = a.crash();
        drop(slot);

        // B runs on another server, which can't see A's disk.
        let uploads = path.parent().unwrap().to_path_buf();
        let away = uploads.with_extension("elsewhere");
        std::fs::rename(&uploads, &away).unwrap();
        let b = fleet.start("b", NSFW).await;
        let resumed = b.recover().await;
        b.wait_idle().await;
        if resumed > 0 {
            fleet.log.delivered("b", &t).await;
        }
        std::fs::rename(&away, &uploads).unwrap();
        let task = fleet.task(&t).await;

        assert!(
            !finished(task.state),
            "B failed a task uploaded to A as soon as it started: {:?}",
            task.items[0].error
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until upload paths are checked (only <upload_dir>/<uuid>.bin)"]
async fn a_task_row_makes_an_instance_read_and_delete_any_file() {
    fleet_test(|fleet| async move {
        let lists = tempfile::tempdir().unwrap();
        let b = fleet
            .start("b", &pdq_model(lists.path(), "ab", 1, "known"))
            .await;
        // An image, so a result for it shows B read it.
        let elsewhere = tempfile::tempdir().unwrap();
        let file = elsewhere.path().join("private.ppm");
        std::fs::write(&file, ppm(32, 32, 3)).unwrap();
        // Whoever can write to the shared database adds a task pointing there.
        let task = Task::new(
            uuid::Uuid::new_v4().to_string(),
            vec![Item::new(
                Input::Bytes {
                    path: file.clone(),
                    kind: MediaKind::Image,
                },
                vec!["hash".into()],
                None,
            )],
            None,
        );
        fleet.store.create_task(&task).await.unwrap();

        assert_eq!(b.recover().await, 1);
        b.wait_idle().await;
        fleet.log.delivered("b", &task.id).await;
        let task = fleet.task(&task.id).await;

        assert_eq!(
            (file.exists(), hash_names(&task)),
            (true, None),
            "B read a file outside its upload directory, classified it and \
             deleted it: (file kept, B's result for it)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until a takeover checks the task's models resolve locally; the lease \
            PR expires A's lease before B starts, so that only this keeps B off the task"]
async fn an_instance_without_the_model_fails_a_task_it_takes_over() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let special = "[models.special]\narchitecture = \"vit\"\nrepo = \"test/special\"\n";
        let a = fleet.start("a", &format!("{NSFW}{special}")).await;
        let t = a
            .submit(Submission {
                run: Run::Models(vec!["special".into()]),
                ..image_at(&origin.url("/t.ppm"))
            })
            .await
            .id;
        let _download = origin.next("a downloads the input").await;
        a.crash();

        // B has no `special` model.
        let b = fleet.start("b", NSFW).await;
        let resumed = b.recover().await;
        if b.in_flight() > 0 {
            origin
                .next("b downloads the input")
                .await
                .reply(Reply::Bytes(ppm(16, 16, 1)));
        }
        b.wait_idle().await;
        if resumed > 0 {
            fleet.log.delivered("b", &t).await;
        }
        let task = fleet.task(&t).await;

        assert!(
            !finished(task.state),
            "B finished a task for a model it doesn't have: {:?}",
            task.items[0].results.get("special")
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until the result cache key covers the model's config"]
async fn differently_configured_instances_share_cached_results() {
    fleet_test(|fleet| async move {
        // One model name, a different block list on each instance: each
        // answers with its own category.
        let lists = tempfile::tempdir().unwrap();
        let a = fleet
            .start(
                "a",
                &format!("[cache]\n{}", pdq_model(lists.path(), "ab", 1, "known")),
            )
            .await;
        let b = fleet
            .start(
                "b",
                &format!("[cache]\n{}", pdq_model(lists.path(), "cd", 2, "other")),
            )
            .await;

        /// The category names `instance` answers for an upload of `bytes`.
        async fn classify(fleet: &Fleet, instance: &Instance, bytes: &[u8]) -> Vec<String> {
            let submission = Submission {
                run: Run::Models(vec!["hash".into()]),
                ..uploaded(&instance.stage(bytes))
            };
            let t = instance.submit(submission).await.id;
            instance.wait_idle().await;
            fleet.log.delivered(instance.who, &t).await;
            let task = fleet.task(&t).await;
            hash_names(&task).unwrap_or_else(|| panic!("no classification in {task:?}"))
        }

        let image = ppm(64, 64, 7);
        let other = ppm(64, 64, 99);
        assert_eq!(
            classify(&fleet, &b, &other).await,
            ["other"],
            "B's own answer"
        );
        assert_eq!(classify(&fleet, &a, &image).await, ["known"], "A's");

        assert_eq!(
            classify(&fleet, &b, &image).await,
            ["other"],
            "B answered with A's cached result for an image A had classified"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until a watch re-reads the task now and then, within 4 s (changes \
            elsewhere aren't announced)"]
async fn a_wait_on_another_instance_misses_the_finish() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let a = fleet.start("a", NSFW).await;
        let b = fleet.start("b", NSFW).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download = origin.next("a downloads the input").await;

        // A client waits for the task through B (`wait_seconds`, WatchTask).
        let waiting = b.spawn({
            let t = t.clone();
            move |e| async move {
                let started = Instant::now();
                let task = e
                    .wait_for(&t, Duration::from_secs(5))
                    .await
                    .unwrap()
                    .unwrap();
                (task.state, started.elapsed())
            }
        });
        fleet
            .log
            .wait_for("b's wait reads the task", |who, c| {
                who == "b" && matches!(c, Call::GetTask(id) if *id == t)
            })
            .await;
        download.reply(Reply::Garbage);
        fleet.log.delivered("a", &t).await;
        let (state, took) = waiting.await.unwrap();

        assert!(
            finished(state) && took < Duration::from_secs(4),
            "B's wait missed A finishing the task: after {took:?} it returned {state:?}"
        );
    })
    .await;
}
