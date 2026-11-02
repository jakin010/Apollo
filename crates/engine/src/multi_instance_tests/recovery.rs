//! Startup recovery and task ownership: an instance's start takes over every
//! unfinished task in the database, whoever runs it.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until task leases (a start resumes only unclaimed or expired tasks)"]
async fn a_starting_instance_reruns_a_task_another_is_running() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let a = fleet.start("a", NSFW).await;
        let b = fleet.start("b", NSFW).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download_a = origin.next("a downloads the input").await;

        // B starts while A is downloading.
        let resumed = b.recover().await;
        let download_b = match b.in_flight() {
            0 => None,
            _ => Some(origin.next("b downloads it too").await),
        };

        download_a.reply(Reply::Garbage);
        fleet.log.delivered("a", &t).await;
        if let Some(download) = download_b {
            download.reply(Reply::Garbage);
            fleet.log.delivered("b", &t).await;
        }
        b.wait_idle().await;

        assert_eq!(
            (resumed, origin.hits(), fleet.log.finished(&t).len()),
            (0, 1, 1),
            "B's start took over the task A was running: \
             (tasks B resumed, downloads, finished webhooks)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until conditional state writes (the start write only on an active task)"]
async fn a_stale_start_undoes_a_cancel() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let a = fleet.start("a", NSFW).await;
        let b = fleet.start("b", NSFW).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download_a = origin.next("a downloads the input").await;

        // B loads the task to resume it; it's cancelled before B starts it.
        let stale = fleet.task(&t).await;
        a.cancel(&t).await;
        assert_eq!(fleet.task(&t).await.state, TaskState::Cancelled);
        b.on(move |e| async move {
            let reservations: Vec<_> = stale
                .items
                .iter()
                .map(|item| e.reserve_resumed(&item.models))
                .collect();
            tokio::spawn(async move { e.run_task(stale, reservations).await });
        })
        .await;
        let download_b = origin
            .next_unless("b runs its copy or gives it up", || b.in_flight() == 0)
            .await;
        let after_b = fleet.task(&t).await;

        download_a.reply(Reply::Garbage);
        fleet.log.delivered("a", &t).await;
        if let Some(download) = download_b {
            download.reply(Reply::Garbage);
            fleet.log.delivered("b", &t).await;
        }
        b.wait_idle().await;
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
#[ignore = "multi-instance: until task leases (attempts count only takeovers of a dead instance's task)"]
async fn starting_instances_use_up_a_healthy_tasks_attempts() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let a = fleet.start("a", NSFW).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let mut downloads = vec![origin.next("a downloads the input").await];

        // Four more instances start (a scale-out, say) while A runs the task:
        // no crash anywhere.
        let mut peers = Vec::new();
        let mut resumed = Vec::new();
        for who in ["b1", "b2", "b3", "b4"] {
            let peer = fleet.start(who, NSFW).await;
            resumed.push(peer.recover().await);
            if peer.in_flight() > 0 {
                downloads.push(origin.next("the new instance downloads it too").await);
            }
            peers.push(peer);
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
            (resumed, attempts, after.state),
            (vec![0, 0, 0, 0], 0, TaskState::Processing),
            "instances starting up spent the attempts of a task A was running \
             fine, and failed it as poison: (tasks each resumed, attempts, \
             state); item error {:?}",
            after.items[0].error
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until task leases (a running instance takes over expired ones); \
            the lease PR gives this test a short lease"]
async fn nobody_takes_over_a_dead_instances_task() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let b = fleet.start("b", NSFW).await;
        b.recover().await;
        let a = fleet.start("a", NSFW).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let _download = origin.next("a downloads the input").await;
        // With leases, first check here that B leaves the task alone for a few
        // lease periods while A runs it (so A renews its lease), then crash A.
        a.crash();

        let taken = origin.next_within(Duration::from_secs(10)).await;
        let state = fleet.task(&t).await.state;
        if let Some(download) = taken {
            download.reply(Reply::Garbage);
            fleet.log.delivered("b", &t).await;
        }
        b.wait_idle().await;

        assert!(
            fleet.log.count(|who, _| who == "b") > 0 && finished(fleet.task(&t).await.state),
            "B never took over the task of the instance that died: 10 s on it is {state:?}"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until webhook delivery claims (a start resends only unclaimed events)"]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until webhook delivery claims (redelivery skips events being delivered)"]
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
#[ignore = "multi-instance: until task leases (a start reserves room only for tasks it takes)"]
async fn a_starting_instance_counts_others_work_as_its_own() {
    let toml = format!("[app]\nmax_pending = 2\n{NSFW}");
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let a = fleet.start("a", &toml).await;
        let b = fleet.start("b", &toml).await;
        let mut downloads = Vec::new();
        for path in ["/1.png", "/2.png"] {
            a.submit(image_at(&origin.url(path))).await;
            downloads.push(origin.next("a downloads an input").await);
        }

        // B starts with no work of its own.
        let resumed = b.recover().await;
        for _ in 0..resumed {
            downloads.push(origin.next("b downloads a resumed input").await);
        }
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

        assert!(
            room.is_ok(),
            "B refuses new work as if A's tasks were its own: {room:?}"
        );
    })
    .await;
}
