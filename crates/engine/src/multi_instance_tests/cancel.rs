//! Cancels and terminal states: the stop signal is in-process, and state
//! writes don't check what another instance wrote meanwhile.

use super::*;

use apollo_domain::Classification;

/// `[app] max_concurrent = 1`, so a test can take the only slot.
fn one_slot(models: &str) -> String {
    format!("[app]\nmax_concurrent = 1\n{models}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until cross-instance cancel (the runner sees a cancel written elsewhere, \
            at its next lease renewal; the lease PR gives this test a short lease)"]
async fn a_cancel_through_another_instance_doesnt_stop_the_task() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", &one_slot(NSFW)).await;
        let b = fleet.start("b", NSFW).await;
        // A's item waits for A's only slot.
        let slot = a.engine.inner.gate.clone().acquire(0).await;
        let t = a.submit(refused()).await.id;
        wait_until("a's item waits for a slot", || a.slots_waiting() == 1).await;

        b.cancel(&t).await;
        // Cancelled on A, the wait would end at once (see the engine test
        // a_cancelled_item_waiting_for_a_slot_gives_back_its_place). From B, A
        // can only learn of it when it next renews its lease.
        let stopped = wait_within(WAIT, || a.in_flight() == 0).await;
        let b_tokens = b.engine.inner.cancels.lock().unwrap().len();

        drop(slot);
        a.wait_idle().await;
        fleet.log.delivered("a", &t).await;
        let item = fleet.task(&t).await.items[0].state;

        assert_eq!(
            (stopped, item, b_tokens),
            (true, ItemState::Cancelled, 0),
            "a cancel through B didn't reach A: (A's item stopped waiting, its \
             final state, cancel signals B keeps for tasks it doesn't run)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until conditional state writes (the first terminal state wins)"]
async fn finishing_overwrites_a_cancel_from_another_instance() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", NSFW).await;
        let b = fleet.start("b", NSFW).await;
        // A's item has failed; A has read the task to roll it up, and is about
        // to write it failed.
        let hold = a.hold_storage(1, |c| matches!(c, Call::SetTaskState(_, s) if finished(*s)));
        let t = a.submit(refused()).await.id;
        hold.entered("a marks the task failed").await;

        b.cancel(&t).await;
        let cancelled = fleet.task(&t).await.state;
        hold.release();
        a.wait_idle().await;
        fleet.log.delivered("a", &t).await;

        assert_eq!(
            (cancelled, fleet.task(&t).await.state),
            (TaskState::Cancelled, TaskState::Cancelled),
            "A's rollup overwrote the cancel B had just written: (after the cancel, at the end)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "multi-instance: until conditional state writes (a finished item stays finished)"]
async fn a_second_runner_overwrites_a_finished_result() {
    fleet_test(|fleet| async move {
        let url = "file:///nonexistent/apollo-test.png";
        let a = fleet
            .start("a", &one_slot(&format!("[cache]\n{NSFW}")))
            .await;
        let b = fleet.start("b", &format!("[cache]\n{NSFW}")).await;
        // A's item missed the result cache and waits for A's only slot.
        let slot = a.engine.inner.gate.clone().acquire(0).await;
        let t = a.submit(image_at(url)).await.id;
        wait_until("a's item waits for a slot", || a.slots_waiting() == 1).await;

        // Meanwhile another task's result for this input is cached, and B
        // runs its own copy of the task, as a takeover does: it finishes it
        // from the cache.
        let content = sha256_hex(b"the image");
        fleet
            .store
            .url_cache_store(&sha256_hex(url.as_bytes()), "nsfw", "main", &content)
            .await
            .unwrap();
        fleet
            .store
            .cache_store(
                &content,
                "nsfw",
                "main",
                &ModelOutput::Classification(Classification::default()),
            )
            .await
            .unwrap();
        let copy = fleet.task(&t).await;
        b.on(move |e| async move {
            let reservations: Vec<_> = copy
                .items
                .iter()
                .map(|item| e.reserve_resumed(&item.models))
                .collect();
            e.run_task(copy, reservations).await;
        })
        .await;
        fleet.log.delivered("b", &t).await;
        let by_b = fleet.task(&t).await;
        assert_eq!(
            (by_b.state, by_b.items[0].results["nsfw"].state),
            (TaskState::Completed, ModelState::Done),
            "B finished it from the cache"
        );

        // Then A's item gets its slot.
        drop(slot);
        a.wait_idle().await;
        let a_wrote = wait_within(Duration::from_secs(2), || {
            fleet
                .log
                .count(|who, c| who == "a" && matches!(c, Call::WebhookDelivered(..)))
                > 0
        })
        .await;
        let end = fleet.task(&t).await;

        assert_eq!(
            (
                end.items[0].state,
                end.items[0].results["nsfw"].state,
                fleet.log.finished(&t).len(),
                a_wrote,
            ),
            (ItemState::Completed, ModelState::Done, 1, false),
            "A ran the item B had finished and overwrote its result: (item, \
             result, finished webhooks, A sent one)"
        );
    })
    .await;
}
