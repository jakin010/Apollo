//! Per-instance state behind shared data: request_id locks, uploads on one
//! server's disk, the result cache key, and change notifications.

use super::*;

use apollo_domain::{ErrorKind, Item};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_request_id_through_two_instances_makes_two_tasks() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", NSFW).await;
        let b = fleet.start("b", NSFW).await;

        /// Submit `owner`'s `request_id` through A, then (a client's retry)
        /// through B while A is still storing the first try; then both
        /// stores go ahead at once. Returns the number of tasks with that
        /// key, whether both got the same task, and whether exactly one was
        /// flagged as existing.
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
            // Both stores go ahead at once, so their writes overlap.
            store_a.release();
            store_b.release();
            let first = first.await.unwrap().unwrap();
            let retry = retry.await.unwrap().unwrap();
            a.wait_idle().await;
            b.wait_idle().await;
            fleet.log.settled("a", &first.id).await;
            fleet.log.settled("b", &retry.id).await;
            let tasks = fleet
                .tasks(None)
                .await
                .into_iter()
                .filter(|t| {
                    t.owner.as_deref() == owner && t.request_id.as_deref() == Some(request_id)
                })
                .count();
            // Either may have stored it; the other got it as existing.
            (
                tasks,
                retry.id == first.id,
                first.existing != retry.existing,
            )
        }

        let alice = twice(&fleet, &a, &b, Some("alice"), "r").await;
        let nobody = twice(&fleet, &a, &b, None, "r-anon").await;
        assert_eq!(
            [alice, nobody],
            [(1, true, true); 2],
            "the same request_id through two instances made separate tasks, for \
             alice and for no owner: (tasks with the key, both got the same \
             task, exactly one flagged existing)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_starting_instance_deletes_an_upload_in_progress() {
    fleet_test(|fleet| async move {
        // Both servers use one cache directory (e.g. shared storage for the
        // model weights).
        let shared = tempfile::tempdir().unwrap();
        let a = fleet.start_in("a", NSFW, shared.path()).await;
        // An upload is arriving at A when B starts; another, two hours old,
        // is an orphan.
        let path = a.stage(b"not an image");
        let orphan = a.stage(b"orphan");
        std::fs::File::options()
            .write(true)
            .open(&orphan)
            .unwrap()
            .set_modified(std::time::SystemTime::now() - Duration::from_secs(7200))
            .unwrap();
        let b = fleet.start_in("b", NSFW, shared.path()).await;
        b.recover().await;
        let (kept, swept) = (path.exists(), !orphan.exists());

        let t = a.submit(uploaded(&path)).await.id;
        a.wait_idle().await;
        fleet.log.delivered("a", &t).await;
        let error = fleet.task(&t).await.items[0].error.clone().unwrap();

        assert_eq!(
            (kept, swept, error.kind),
            (true, true, ErrorKind::Decode),
            "B's start deleted A's upload in progress: (kept, the old orphan \
             swept, A's item error kind); A's error: {}",
            error.message
        );
    })
    .await;
}

/// A dead instance A with an upload task `t` and a plain one `c` (an http
/// input, unpinned), both waiting for A's only slot when A crashed; B running.
async fn dead_with_an_upload(
    fleet: &Fleet,
    origin: &Origin,
) -> (Instance, String, String, Option<tempfile::TempDir>) {
    let toml = format!("{SHORT_LEASE}{NSFW}");
    let b = fleet.start("b", &toml).await;
    b.recover().await;
    let a = fleet
        .start("a", &format!("[app]\nmax_concurrent = 1\n{toml}"))
        .await;
    let slot = a.engine.inner.gate.clone().acquire(0).await;
    let t = a.submit(uploaded(&a.stage(b"not an image"))).await.id;
    let c = a.submit(image_at(&origin.url("/c.png"))).await.id;
    wait_until("a's items wait for a slot", || a.slots_waiting() == 2).await;
    let disk = a.crash();
    drop(slot);
    (b, t, c, disk)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upload_to_a_dead_instance_fails_elsewhere_at_once() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let (b, t, c, _disk) = dead_with_an_upload(&fleet, &origin).await;
        // Both leases lapsed, within the upload's grace.
        fleet.db.expire_lease(&t, LEASE).await;
        fleet.db.expire_lease(&c, LEASE).await;

        // B takes the plain task over...
        let download = origin.next("b takes the plain task over").await;
        assert_eq!(download.path, "/c.png");
        download.reply(Reply::Garbage);
        b.wait_idle().await;
        fleet.log.settled("b", &c).await;
        // ...and, a few passes on, has left the upload alone.
        tokio::time::sleep(LEASE).await;
        let task = fleet.task(&t).await;
        let claims = fleet
            .log
            .count(|who, call| who == "b" && matches!(call, Call::Claim(id, Some(_)) if *id == t));

        assert!(
            !finished(task.state) && claims == 0,
            "B took over a task uploaded to A within its grace: {:?}, B's claims {claims}",
            task.items[0].error
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_instances_upload_fails_after_its_grace() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let (b, t, c, _disk) = dead_with_an_upload(&fleet, &origin).await;
        // A has been gone longer than the grace (ten leases).
        fleet.db.expire_lease(&t, 20 * LEASE).await;
        fleet.db.expire_lease(&c, LEASE).await;
        origin
            .next("b takes the plain task over")
            .await
            .reply(Reply::Garbage);
        fleet.log.delivered("b", &t).await;
        b.wait_idle().await;
        fleet.log.settled("b", &c).await;
        let task = fleet.task(&t).await;
        let error = task.items[0].error.clone().unwrap();

        assert_eq!(
            (task.state, error.kind, fleet.log.finished(&t)),
            (
                TaskState::Failed,
                ErrorKind::Fetch,
                vec![("b", TaskState::Failed)]
            )
        );
        assert!(
            error.message.contains("uploaded to node a"),
            "{}",
            error.message
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
        fleet.store.create_task(&task, None, None).await.unwrap();

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
async fn an_instance_without_the_model_fails_a_task_it_takes_over() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        // B has no `special` model.
        let b = fleet.start("b", &format!("{SHORT_LEASE}{NSFW}")).await;
        b.recover().await;
        let special = "[models.special]\narchitecture = \"vit\"\nrepo = \"test/special\"\n";
        let a = fleet
            .start("a", &format!("{SHORT_LEASE}{NSFW}{special}"))
            .await;
        let t = a
            .submit(Submission {
                run: Run::Models(vec!["special".into()]),
                ..image_at(&origin.url("/t.ppm"))
            })
            .await
            .id;
        let c = a.submit(image_at(&origin.url("/c.ppm"))).await.id;
        let _downloads = [
            origin.next("a downloads an input").await,
            origin.next("a downloads an input").await,
        ];
        a.crash();
        // Both leases lapsed.
        fleet.db.expire_lease(&t, LEASE).await;
        fleet.db.expire_lease(&c, LEASE).await;

        // B takes over the task it can run...
        let download = origin.next("b takes the nsfw task over").await;
        assert_eq!(download.path, "/c.ppm");
        download.reply(Reply::Garbage);
        b.wait_idle().await;
        fleet.log.settled("b", &c).await;
        // ...and, a few passes on, has left the other alone.
        tokio::time::sleep(LEASE).await;
        let task = fleet.task(&t).await;
        let claims = fleet
            .log
            .count(|who, call| who == "b" && matches!(call, Call::Claim(id, Some(_)) if *id == t));

        assert!(
            !finished(task.state) && claims == 0 && origin.hits() == 3,
            "B took over a task for a model it doesn't have: {:?}, B's claims {claims}",
            task.items[0].results.get("special")
        );
    })
    .await;
}

/// Not ignored: instances whose model gives other answers for the same
/// bytes don't share cached results (the key covers the model's settings and
/// its block list's contents).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identically_configured_instances_share_cached_results() {
    fleet_test(|fleet| async move {
        // The same block list on each, at a path of its own.
        let (lists_a, lists_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let model = |dir: &Path| format!("[cache]\n{}", pdq_model(dir, "ab", 1, "known"));
        let a = fleet.start("a", &model(lists_a.path())).await;
        let b = fleet.start("b", &model(lists_b.path())).await;

        let image = ppm(64, 64, 7);
        assert_eq!(classify(&fleet, &a, &image).await, ["known"], "A's");
        assert_eq!(classify(&fleet, &b, &image).await, ["known"], "B's");
        assert_eq!(
            b.engine.inner.metrics.cache_hits.load(Ordering::Relaxed),
            1,
            "B answered from A's cached result"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_upload_whose_node_died_is_finished_as_cancelled() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let (b, t, c, _disk) = dead_with_an_upload(&fleet, &origin).await;
        // Cancelled through B; A's lease lapses, well within the upload's
        // grace: finishing it as cancelled reads no input, so B does it.
        b.cancel(&t).await;
        fleet.db.expire_lease(&t, LEASE).await;
        fleet.log.delivered("b", &t).await;
        let task = fleet.task(&t).await;
        fleet.db.expire_lease(&c, LEASE).await;
        origin
            .next("b takes the plain task over")
            .await
            .reply(Reply::Garbage);
        b.wait_idle().await;
        fleet.log.settled("b", &c).await;

        assert_eq!(
            (task.state, task.items[0].state, fleet.log.finished(&t)),
            (
                TaskState::Cancelled,
                ItemState::Cancelled,
                vec![("b", TaskState::Cancelled)]
            )
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_no_instance_can_run_is_failed_once_stale() {
    fleet_test(|fleet| async move {
        let b = fleet.start("b", &format!("{SHORT_LEASE}{NSFW}")).await;
        // A task for a model B doesn't have, whose runner stopped long ago
        // (past the grace): nobody that looked could run it.
        let task = Task::new(
            uuid::Uuid::new_v4().to_string(),
            vec![Item::new(refused().input, vec!["gone".into()], None)],
            None,
        );
        let crashed = apollo_storage::Lease {
            token: "crashed",
            node: "gone",
            process: "p",
            ttl: LEASE,
        };
        fleet
            .store
            .create_task(&task, Some(&crashed), None)
            .await
            .unwrap();
        fleet.db.expire_lease(&task.id, 20 * LEASE).await;

        b.recover().await;
        fleet.log.delivered("b", &task.id).await;
        b.wait_idle().await;
        let end = fleet.task(&task.id).await;
        let error = end.items[0].error.clone().unwrap();

        assert_eq!(
            (end.state, error.kind),
            (TaskState::Failed, ErrorKind::ModelUnavailable),
            "{}",
            error.message
        );
        assert!(error.message.contains("gone"), "{}", error.message);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_takes_its_own_pinned_task_back_even_when_full() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let toml = format!("[app]\nmax_concurrent = 1\nmax_pending = 1\n{SHORT_LEASE}{NSFW}");
        let a = fleet.start("a", &toml).await;
        let slot = a.engine.inner.gate.clone().acquire(0).await;
        let t = a.submit(uploaded(&a.stage(b"not an image"))).await.id;
        wait_until("a's item waits for a slot", || a.slots_waiting() == 1).await;
        let disk = a.crash().unwrap();
        drop(slot);
        fleet.db.expire_lease(&t, LEASE).await;

        // A comes back (same node, same disk), and is full at once: no room
        // for new work, though a slot is free.
        let full = format!("[app]\nmax_concurrent = 2\nmax_pending = 1\n{SHORT_LEASE}{NSFW}");
        let back = fleet.start_in("a", &full, disk.path()).await;
        back.submit(image_at(&origin.url("/busy.png"))).await;
        let busy = origin.next("a downloads its new work").await;
        let room = back
            .on(|e| async move {
                let probe = refused();
                e.check_submission(&probe.input, &probe.run).is_ok()
            })
            .await;
        assert!(!room, "full");
        back.recover().await;
        fleet.log.delivered("a", &t).await;
        let error = fleet.task(&t).await.items[0].error.clone().unwrap();
        busy.reply(Reply::Garbage);
        back.wait_idle().await;

        // It ran it: its upload was read (and isn't an image).
        assert_eq!(error.kind, ErrorKind::Decode, "{}", error.message);
    })
    .await;
}
