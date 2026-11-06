//! Startup recovery and task ownership: an instance's start takes over every
//! unfinished task in the database, whoever runs it.

use super::*;

use apollo_storage::ClaimMode;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_starting_instance_reruns_a_task_another_is_running() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let toml = format!("{SHORT_LEASE}{NSFW}");
        let a = fleet.start("a", &toml).await;
        let b = fleet.start("b", &toml).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download_a = origin.next("a downloads the input").await;

        // B starts while A is downloading, then looks for tasks to take over
        // for a few leases, settled from the first one on: A's renewals keep
        // the task A's.
        let resumed = b.recover().await;
        let late = origin.next_within(3 * LEASE).await;
        let (holder, live) = fleet.db.lease(&t).await;

        download_a.reply(Reply::Garbage);
        fleet.log.delivered("a", &t).await;
        if let Some(download) = late {
            download.reply(Reply::Garbage);
        }
        b.wait_idle().await;
        fleet.log.settled("b", &t).await;

        assert!(
            holder.as_deref().is_some_and(|h| h.starts_with("a/")) && live,
            "A held a live lease: {holder:?}, live {live}"
        );
        assert_eq!(
            (resumed, origin.hits(), fleet.log.finished(&t).len()),
            (0, 1, 1),
            "B took over the task A was running: \
             (tasks B resumed at its start, downloads, finished webhooks)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_start_undoes_a_cancel() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let a = fleet.start("a", NSFW).await;
        let b = fleet.start("b", NSFW).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download_a = origin.next("a downloads the input").await;

        // A's lease lapses (A stalls, say) and B takes the task over; it's
        // cancelled before B's start write.
        fleet.db.expire_lease(&t, Duration::from_secs(1)).await;
        let start = b.hold_storage(1, |c| {
            matches!(c, Call::SetTaskState(_, TaskState::Processing, _))
        });
        assert!(b.take(&t, ClaimMode::Expired).await);
        start.entered("b starts its run").await;
        a.cancel(&t).await;
        assert_eq!(fleet.task(&t).await.state, TaskState::Cancelled);
        start.release();
        let download_b = origin
            .next_unless("b runs its copy or gives it up", || b.in_flight() == 0)
            .await;
        let after_b = fleet.task(&t).await;

        download_a.reply(Reply::Garbage);
        a.wait_idle().await;
        fleet.log.settled("a", &t).await;
        if let Some(download) = download_b {
            download.reply(Reply::Garbage);
        }
        b.wait_idle().await;
        fleet.log.settled("b", &t).await;
        let end = fleet.task(&t).await;

        assert_eq!(
            (
                after_b.state,
                after_b.finished_at.is_some(),
                origin.hits(),
                end.state
            ),
            (TaskState::Cancelled, true, 1, TaskState::Cancelled),
            "B's start write put the cancelled task back to work: \
             (state after B's start, finish time kept, downloads, final state)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn starting_instances_use_up_a_healthy_tasks_attempts() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let toml = format!("{SHORT_LEASE}{NSFW}");
        let a = fleet.start("a", &toml).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let mut downloads = vec![origin.next("a downloads the input").await];

        // Four more instances start (a scale-out, say) while A runs the task,
        // and look for tasks to take over for a few leases: no crash anywhere.
        let mut peers = Vec::new();
        let mut resumed = Vec::new();
        for who in ["b1", "b2", "b3", "b4"] {
            let peer = fleet.start(who, &toml).await;
            resumed.push(peer.recover().await);
            peers.push(peer);
        }
        while let Some(download) = origin.next_within(3 * LEASE).await {
            downloads.push(download);
        }
        let after = fleet.task(&t).await;
        let attempts = fleet.db.attempts(&t).await;

        for download in downloads {
            download.reply(Reply::Garbage);
        }
        a.wait_idle().await;
        for peer in &peers {
            peer.wait_idle().await;
        }

        assert_eq!(
            (resumed, attempts, after.state, origin.hits()),
            (vec![0, 0, 0, 0], 0, TaskState::Processing, 1),
            "instances starting up spent the attempts of a task A was running \
             fine: (tasks each resumed, attempts, state, downloads); item error {:?}",
            after.items[0].error
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nobody_takes_over_a_dead_instances_task() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let toml = format!("{SHORT_LEASE}{NSFW}");
        let b = fleet.start("b", &toml).await;
        b.recover().await;
        let a = fleet.start("a", &toml).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let _download = origin.next("a downloads the input").await;
        // While A runs it, renewing its lease, B leaves it alone.
        let early = origin.next_within(3 * LEASE).await;
        assert!(early.is_none(), "B took over a task A was still running");

        a.crash();
        let crashed = Instant::now();
        let taken = origin.next_within(WAIT).await;
        let took = crashed.elapsed();
        let state = fleet.task(&t).await.state;
        if let Some(download) = taken {
            download.reply(Reply::Garbage);
        }
        b.wait_idle().await;
        fleet.log.settled("b", &t).await;

        let claimed = fleet
            .log
            .count(|who, c| who == "b" && matches!(c, Call::Claim(id, Some(1)) if *id == t));
        assert!(
            claimed == 1 && finished(fleet.task(&t).await.state),
            "B never took over the task of the instance that died: after {took:?} \
             it is {state:?}, B's claims {claimed}"
        );
        // Within its lease, a renewal period and a pass after the crash.
        assert!(took < 3 * LEASE, "took {took:?}");
        assert_eq!(fleet.db.attempts(&t).await, 1, "a crash counts one attempt");
    })
    .await;
}

/// Not ignored: an instance starting while another delivers a finished
/// webhook leaves it alone (the delivery claim is the other's).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_starting_instance_resends_a_webhook_being_delivered() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", NSFW).await;
        let b = fleet.start("b", NSFW).await;
        let hold = a.hold_webhook(1, |c| matches!(c, Call::Webhook(_, EventKind::Finished, _)));
        let t = a.submit(refused()).await.id;
        hold.entered("a delivers the finished webhook").await;

        // B starts while A's delivery is on its way.
        b.recover().await;
        let resent = wait_within(Duration::from_secs(2), || {
            fleet
                .log
                .count(|who, c| who == "b" && matches!(c, Call::WebhookDelivered(..)))
                > 0
        })
        .await;
        hold.release();
        fleet.log.delivered("a", &t).await;

        assert_eq!(
            (resent, fleet.log.finished(&t)),
            (false, vec![("a", TaskState::Failed)]),
            "B's start resent the finished webhook A was delivering: \
             (B delivered one, finished webhooks by sender)"
        );
    })
    .await;
}

/// Not ignored: another instance's redelivery leaves a finished webhook
/// alone while its deliverer renews the claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_instance_resends_a_webhook_being_delivered() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", NSFW).await;
        // B redelivers pending webhooks every second.
        let webhook = "[webhook]\nurl = \"http://127.0.0.1:9\"\nredelivery_secs = 1\n";
        let _b = fleet.start("b", &format!("{webhook}{NSFW}")).await;
        let hold = a.hold_webhook(1, |c| matches!(c, Call::Webhook(_, EventKind::Finished, _)));
        let t = a.submit(refused()).await.id;
        hold.entered("a delivers the finished webhook").await;

        // A's delivery is on its way through a few of B's redelivery rounds.
        let resent = wait_within(Duration::from_secs(3), || {
            fleet
                .log
                .count(|who, c| who == "b" && matches!(c, Call::WebhookDelivered(..)))
                > 0
        })
        .await;
        hold.release();
        fleet.log.delivered("a", &t).await;

        assert_eq!(
            (resent, fleet.log.finished(&t)),
            (false, vec![("a", TaskState::Failed)]),
            "B's redelivery resent the finished webhook A was delivering: \
             (B delivered one, finished webhooks by sender)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_starting_instance_counts_others_work_as_its_own() {
    let toml = format!("[app]\nmax_pending = 2\n{SHORT_LEASE}{NSFW}");
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let a = fleet.start("a", &toml).await;
        let b = fleet.start("b", &toml).await;
        let mut downloads = Vec::new();
        for path in ["/1.png", "/2.png"] {
            a.submit(image_at(&origin.url(path))).await;
            downloads.push(origin.next("a downloads an input").await);
        }
        // And one task nobody holds (stored before leases existed): B may
        // take that one.
        let free = Task::new(
            uuid::Uuid::new_v4().to_string(),
            vec![apollo_domain::Item::new(
                image_at(&origin.url("/3.png")).input,
                vec!["nsfw".into()],
                None,
            )],
            None,
        );
        fleet.store.create_task(&free, None, None).await.unwrap();

        // B starts with no work of its own.
        let resumed = b.recover().await;
        for _ in 0..resumed {
            downloads.push(origin.next("b downloads a resumed input").await);
        }
        let held = b.in_flight();
        let room = b
            .on(|e| async move {
                let probe = refused();
                e.check_submission(&probe.input, &probe.run)
                    .map_err(|e| e.to_string())
            })
            .await;

        for download in downloads {
            download.reply(Reply::Garbage);
        }
        a.wait_idle().await;
        b.wait_idle().await;

        assert_eq!(
            (resumed, held, room.clone()),
            (1, 1, Ok(())),
            "B counts A's tasks as its own: (tasks B took, its items in flight, room)"
        );
    })
    .await;
}

/// Not ignored: the poison guard leaves a task cancelled meanwhile cancelled,
/// with its items finished as cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_cancelled_while_failed_as_poison_ends_cancelled() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", NSFW).await;
        let b = fleet.start("b", &format!("{SHORT_LEASE}{NSFW}")).await;
        // A task that has used up its resume attempts; its last runner
        // crashed (its claim was never handed back), and its lease lapsed.
        let mut task = Task::new(
            uuid::Uuid::new_v4().to_string(),
            vec![apollo_domain::Item::new(
                refused().input,
                vec!["nsfw".into()],
                None,
            )],
            None,
        );
        task.state = TaskState::Processing;
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
        for _ in 0..3 {
            fleet.store.increment_attempts(&task.id).await.unwrap();
        }
        fleet.db.expire_lease(&task.id, LEASE).await;

        // B takes it over once it's been connected for a whole lease (its
        // start takes only unclaimed tasks); it's cancelled (through A) just
        // as B claims it.
        let hold = b.hold_storage(1, |c| matches!(c, Call::Claim(..)));
        // Its start, or its takeover loop, claims it: either way it ends as
        // poison, not resumed.
        let starting = b.spawn(|e| async move { e.recover().await.unwrap() });
        hold.entered("b claims the task").await;
        a.cancel(&task.id).await;
        hold.release();
        assert_eq!(starting.await.unwrap(), 0);
        fleet
            .log
            .wait_for("b fails it as poison", |who, c| {
                who == "b" && matches!(c, Call::Claim(id, Some(4)) if *id == task.id)
            })
            .await;
        b.wait_idle().await;
        fleet.log.settled("b", &task.id).await;

        let end = fleet.task(&task.id).await;
        assert_eq!(
            (end.state, end.items[0].state),
            (TaskState::Cancelled, ItemState::Cancelled)
        );
        assert_eq!(fleet.log.finished(&task.id), [("b", TaskState::Cancelled)]);
    })
    .await;
}

/// Not ignored: a graceful stop hands its tasks back, a task submitted while
/// it drained included, for another instance to take at once, counting no
/// attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_graceful_stop_hands_its_tasks_back() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let toml = format!("{SHORT_LEASE}{NSFW}");
        let b = fleet.start("b", &toml).await;
        b.recover().await;
        let a = fleet.start("a", &toml).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download_a = origin.next("a downloads the input").await;
        // A drains before it stops: what it is sent meanwhile still runs.
        assert!(a.engine.lifecycle().drain());
        let late = a.submit(image_at(&origin.url("/late.png"))).await.id;
        let download_late = origin
            .next("a downloads the input sent while draining")
            .await;

        assert!(a.on(|e| async move { e.hand_back().await }).await);
        assert_eq!(
            fleet
                .log
                .count(|who, c| who == "a" && *c == Call::Release(2)),
            1
        );
        let download_b = origin.next("b takes a handed-back task").await;
        let download_b2 = origin.next("b takes the other").await;
        // A's runs stopped: what they fetched now changes nothing.
        download_a.reply(Reply::Garbage);
        download_late.reply(Reply::Garbage);
        download_b.reply(Reply::Garbage);
        download_b2.reply(Reply::Garbage);
        for id in [&t, &late] {
            fleet.log.delivered("b", id).await;
        }
        b.wait_idle().await;

        for id in [&t, &late] {
            assert_eq!(
                (fleet.db.attempts(id).await, fleet.log.finished(id)),
                (0, vec![("b", TaskState::Failed)]),
                "(attempts, finished webhooks) of {id}"
            );
        }
        assert_eq!(
            fleet
                .log
                .count(|who, c| who == "a" && matches!(c, Call::SetItemState(_, _, _, true))),
            2,
            "A wrote only its items' starts"
        );
    })
    .await;
}

/// Not ignored: a task cancelled while its runner was dead gets its items
/// finished, as cancelled, by whoever takes its lapsed lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_task_whose_runner_died_is_finished() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let toml = format!("{SHORT_LEASE}{NSFW}");
        let b = fleet.start("b", &toml).await;
        b.recover().await;
        // A's lease is the default one, so it can't lapse before the test says.
        let a = fleet.start("a", NSFW).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let _download = origin.next("a downloads the input").await;
        a.crash();

        // Cancelled through B, which doesn't run it: its item is left running.
        b.cancel(&t).await;
        let cancelled = fleet.task(&t).await;
        assert_eq!(
            (cancelled.state, cancelled.items[0].state),
            (TaskState::Cancelled, ItemState::Processing)
        );
        fleet.db.expire_lease(&t, LEASE).await;
        fleet.log.delivered("b", &t).await;
        b.wait_idle().await;
        let end = fleet.task(&t).await;

        assert_eq!(
            (end.items[0].state, fleet.log.finished(&t), origin.hits()),
            (ItemState::Cancelled, vec![("b", TaskState::Cancelled)], 1),
            "(item, finished webhooks, downloads)"
        );
    })
    .await;
}

/// Not ignored: a runner cut off from the database (its renewals and its
/// hand-back hang) stops before its lease lapses, and writes nothing more; the
/// instance that takes the task over then runs it, counting one attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_runner_whose_lease_was_taken_over_stops() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let toml = format!("{SHORT_LEASE}{NSFW}");
        let b = fleet.start("b", &toml).await;
        b.recover().await;
        let a = fleet.start("a", &toml).await;
        let cut = a.hold_storage(usize::MAX, |c| {
            matches!(c, Call::Renew(n) if *n > 0) || matches!(c, Call::Release(_))
        });
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download_a = origin.next("a downloads the input").await;
        cut.entered("a renews its lease").await;

        // A stops within its lease, before B may take the task over.
        let stopped = wait_within(LEASE, || a.run_lost(&t) == Some(true)).await;
        let b_claimed = fleet
            .log
            .count(|who, c| who == "b" && matches!(c, Call::Claim(id, Some(_)) if *id == t));
        assert!(
            stopped && b_claimed == 0,
            "A stopped within its lease: {stopped}, B had claimed it {b_claimed} times"
        );

        // A's hand-back hangs too, so B takes the lapsed lease over, counting
        // an attempt.
        let download_b = origin.next("b takes the lapsed task over").await;
        let claim_b = fleet
            .log
            .lock()
            .iter()
            .position(|(who, c)| *who == "b" && *c == Call::Claim(t.clone(), Some(1)))
            .expect("B's claim, counting an attempt");
        // A's download ends: A writes nothing for it.
        download_a.reply(Reply::Garbage);
        a.wait_idle().await;
        // A's hand-back goes through now, and finds the task taken.
        cut.open();
        fleet
            .log
            .wait_for("a hands its claim back", |who, c| {
                who == "a" && *c == Call::Release(0)
            })
            .await;
        download_b.reply(Reply::Garbage);
        fleet.log.delivered("b", &t).await;
        b.wait_idle().await;

        assert_eq!(fleet.log.finished(&t), [("b", TaskState::Failed)]);
        assert_eq!(fleet.db.attempts(&t).await, 1);
        let after: Vec<Call> = fleet.log.lock()[claim_b..]
            .iter()
            .filter(|(who, _)| *who == "a")
            .map(|(_, c)| c.clone())
            .collect();
        assert_eq!(after, [Call::Release(0)], "A's calls after B's claim");
    })
    .await;
}

/// Not ignored: a database outage for every instance, longer than the lease,
/// stops the running task; once the database is back its runner hands it back,
/// and it runs again with no attempt counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_database_outage_for_everyone_hands_runs_back_uncounted() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let toml = format!("{SHORT_LEASE}{NSFW}");
        let b = fleet.start("b", &toml).await;
        b.recover().await;
        let a = fleet.start("a", &toml).await;
        a.recover().await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download_a = origin.next("a downloads the input").await;
        // B may take lapsed leases over: only its takeover read, timing out,
        // keeps it from taking A's task once the database is back.
        assert!(
            wait_within(2 * LEASE, || b.engine.settled()).await,
            "B settled"
        );

        // Every lease call of both hangs.
        let lease_calls = |c: &Call| {
            matches!(c, Call::Renew(n) if *n > 0)
                || matches!(c, Call::Release(_) | Call::Claimable | Call::Claim(..))
        };
        let hold_a = a.hold_storage(usize::MAX, lease_calls);
        let hold_b = b.hold_storage(usize::MAX, lease_calls);
        assert!(
            wait_within(LEASE, || a.run_lost(&t) == Some(true)).await,
            "A stopped within its lease"
        );
        download_a.reply(Reply::Garbage);
        a.wait_idle().await;
        // The outage outlasts the lease on the database's clock.
        tokio::time::sleep(2 * LEASE).await;
        let during = fleet.task(&t).await;
        assert_eq!(
            during.items[0].state,
            ItemState::Processing,
            "A wrote nothing after it stopped"
        );

        // The database answers B half a lease before A. B's reads timed out, so
        // it isn't settled, and it leaves A's lapsed lease alone until A's
        // hand-back lands.
        hold_b.open();
        tokio::time::sleep(LEASE / 2).await;
        hold_a.open();
        let download = origin.next("the task runs again").await;
        download.reply(Reply::Garbage);
        fleet
            .log
            .wait_for(
                "the finished webhook is delivered",
                |_, c| matches!(c, Call::WebhookDelivered(id, _) if *id == t),
            )
            .await;
        a.wait_idle().await;
        b.wait_idle().await;

        assert_eq!(
            (
                fleet.db.attempts(&t).await,
                fleet
                    .log
                    .count(|who, c| who == "a" && *c == Call::Release(1)),
                fleet.log.finished(&t).len(),
            ),
            (0, 1, 1),
            "(attempts, A's hand-backs, finished webhooks)"
        );
    })
    .await;
}

/// Not ignored: a look for tasks to take over that hangs for less than a lease
/// and then finds a lease lapsed (the database was out of reach for everyone)
/// looks again half a lease later instead of taking it: by then the lease's
/// holder has handed it back, and it runs again uncounted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_look_for_lapsed_leases_takes_none() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let toml = format!("{SHORT_LEASE}{NSFW}");
        let b = fleet.start("b", &toml).await;
        b.recover().await;
        let a = fleet.start("a", &toml).await;
        a.recover().await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download_a = origin.next("a downloads the input").await;
        assert!(
            wait_within(2 * LEASE, || b.engine.settled()).await,
            "B settled"
        );

        // The database is out of reach: A stops, and B's next look hangs.
        let hold_a = a.hold_storage(usize::MAX, |c| {
            matches!(c, Call::Renew(n) if *n > 0)
                || matches!(c, Call::Release(_) | Call::Claimable | Call::Claim(..))
        });
        let read_b = b.hold_storage(1, |c| matches!(c, Call::Claimable));
        read_b.entered("b looks for tasks to take over").await;
        let hung = Instant::now();
        assert!(
            wait_within(LEASE, || a.run_lost(&t) == Some(true)).await,
            "A stopped within its lease"
        );
        download_a.reply(Reply::Garbage);
        a.wait_idle().await;
        // A's lease lapses on the database's clock.
        let deadline = Instant::now() + 2 * LEASE;
        while fleet.db.lease(&t).await.1 {
            assert!(Instant::now() < deadline, "A's lease never lapsed");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if let Some(rest) = (LEASE / 2).checked_sub(hung.elapsed()) {
            tokio::time::sleep(rest).await;
        }

        // The database is back for both: B's look answers, with A's lapsed
        // lease; A hands its task back.
        read_b.release();
        hold_a.open();
        tokio::time::sleep(LEASE / 4).await;
        let taken = fleet
            .log
            .count(|who, c| who == "b" && matches!(c, Call::Claim(id, Some(_)) if *id == t));

        let download = origin.next("the task runs again").await;
        download.reply(Reply::Garbage);
        fleet
            .log
            .wait_for(
                "the finished webhook is delivered",
                |_, c| matches!(c, Call::WebhookDelivered(id, _) if *id == t),
            )
            .await;
        a.wait_idle().await;
        b.wait_idle().await;
        assert_eq!(
            (taken, fleet.db.attempts(&t).await),
            (0, 0),
            "(B's claims right after its slow look, attempts)"
        );
    })
    .await;
}

/// Not ignored: renewals a stopped run sent, applied late (the database
/// client queued them), and its hand-back, in whatever order they land, count
/// no attempt: the task runs again once, as if handed back at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renewal_applied_after_the_stop_changes_nothing() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let a = fleet.start("a", &format!("{SHORT_LEASE}{NSFW}")).await;
        a.recover().await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download_a = origin.next("a downloads the input").await;

        // A's renewals and hand-backs are queued, not lost; and its takeover
        // can't run the task again until the test says.
        let queued = a.hold_storage_detached(usize::MAX, |c| {
            matches!(c, Call::Renew(n) if *n > 0) || matches!(c, Call::Release(_))
        });
        let reads = a.hold_storage(usize::MAX, |c| matches!(c, Call::Claimable));
        assert!(
            wait_within(LEASE, || a.run_lost(&t) == Some(true)).await,
            "A stopped within its lease"
        );
        download_a.reply(Reply::Garbage);
        a.wait_idle().await;
        // The queued calls land after the lease lapsed.
        tokio::time::sleep(2 * LEASE).await;
        queued.open();
        fleet
            .log
            .wait_for("a hands its claim back", |who, c| {
                who == "a" && *c == Call::Release(1)
            })
            .await;
        reads.open();
        let download = origin.next("the task runs again").await;
        download.reply(Reply::Garbage);
        fleet.log.delivered("a", &t).await;
        a.wait_idle().await;

        assert_eq!(
            (
                fleet.db.attempts(&t).await,
                fleet
                    .log
                    .count(|who, c| who == "a" && *c == Call::Release(1)),
                fleet.log.finished(&t),
            ),
            (0, 1, vec![("a", TaskState::Failed)]),
            "(attempts, A's hand-backs, finished webhooks)"
        );
    })
    .await;
}

/// Not ignored: a takeover takes only as much as there is room for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_takeover_stops_at_the_room_left() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let b = fleet
            .start("b", &format!("[app]\nmax_pending = 2\n{SHORT_LEASE}{NSFW}"))
            .await;
        // Three tasks nobody holds.
        let mut ids = Vec::new();
        for path in ["/1.png", "/2.png", "/3.png"] {
            let task = Task::new(
                uuid::Uuid::new_v4().to_string(),
                vec![apollo_domain::Item::new(
                    image_at(&origin.url(path)).input,
                    vec!["nsfw".into()],
                    None,
                )],
                None,
            );
            fleet.store.create_task(&task, None, None).await.unwrap();
            ids.push(task.id);
        }

        let resumed = b.recover().await;
        let mut downloads = Vec::new();
        for _ in 0..resumed {
            downloads.push(origin.next("b downloads a task it took").await);
        }
        let mut left = 0;
        for id in &ids {
            if fleet.db.lease(id).await.0.is_none() {
                left += 1;
            }
        }
        for download in downloads {
            download.reply(Reply::Garbage);
        }
        b.wait_idle().await;

        assert_eq!(
            (resumed, left),
            (2, 1),
            "(tasks B took, tasks left unclaimed)"
        );
    })
    .await;
}
