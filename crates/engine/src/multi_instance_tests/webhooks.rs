//! Finished webhooks with several instances: each is sent by one instance at a
//! time, under a delivery claim on the database, and by another once that
//! instance stops or gives up.

use super::*;

/// Redelivery every second, to the fleet's receiver.
const REDELIVERY: &str = "[webhook]\nurl = \"http://127.0.0.1:9\"\nredelivery_secs = 1\n";

/// How many times `who` claimed the delivery of `task`'s webhook.
fn claims_of(fleet: &Fleet, who: &str, task: &str) -> usize {
    fleet.log.count(|w, c| {
        w == who
            && matches!(c, Call::ClaimWebhooks(items) if items.iter().any(|(id, _)| id == task))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crashed_instances_webhook_is_sent_once_its_claim_lapses() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", &format!("{SHORT_LEASE}{NSFW}")).await;
        let b = fleet
            .start("b", &format!("{REDELIVERY}{SHORT_LEASE}{NSFW}"))
            .await;
        b.recover().await;
        let hold = a.hold_webhook(1, |c| matches!(c, Call::Webhook(_, EventKind::Finished, _)));
        let t = a.submit(refused()).await.id;
        hold.entered("a delivers the finished webhook").await;
        let (holder, live) = fleet.db.delivery_claim(&t, 0).await;
        assert!(
            holder.as_deref().is_some_and(|h| h.starts_with("a/")) && live,
            "A's delivery claim: {holder:?}, live {live}"
        );

        // A dies mid-delivery: once its claim lapses, B sends the event.
        a.crash();
        fleet.log.delivered("b", &t).await;

        assert_eq!(fleet.log.finished(&t), [("b", TaskState::Failed)]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_delivery_keeps_its_claim() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", &format!("{SHORT_LEASE}{NSFW}")).await;
        let b = fleet
            .start("b", &format!("{REDELIVERY}{SHORT_LEASE}{NSFW}"))
            .await;
        b.recover().await;
        let hold = a.hold_webhook(1, |c| matches!(c, Call::Webhook(_, EventKind::Finished, _)));
        let t = a.submit(refused()).await.id;
        hold.entered("a delivers the finished webhook").await;

        // A's delivery takes three leases, through B's redelivery rounds
        // (settled after the first): A's renewals keep the claim A's.
        tokio::time::sleep(3 * LEASE).await;
        let taken = claims_of(&fleet, "b", &t);
        hold.release();
        fleet.log.delivered("a", &t).await;

        assert_eq!(
            (taken, fleet.log.finished(&t)),
            (0, vec![("a", TaskState::Failed)]),
            "(B's claims of the delivery, finished webhooks by sender)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_delivery_is_taken_by_the_next_round() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", &format!("{SHORT_LEASE}{NSFW}")).await;
        let b = fleet
            .start("b", &format!("{REDELIVERY}{SHORT_LEASE}{NSFW}"))
            .await;
        b.recover().await;
        // A's receiver refuses all three of A's attempts.
        a.fail_webhooks(3);
        let t = a.submit(refused()).await.id;
        fleet
            .log
            .wait_for("a hands the delivery back", |who, c| {
                who == "a" && matches!(c, Call::ReleaseWebhook(id, 0) if *id == t)
            })
            .await;

        fleet.log.delivered("b", &t).await;
        assert_eq!(fleet.log.finished(&t), [("b", TaskState::Failed)]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_instance_with_another_webhook_leaves_the_fleets_events() {
    fleet_test(|fleet| async move {
        let toml = format!("{REDELIVERY}{SHORT_LEASE}{NSFW}");
        let a = fleet.start("a", &format!("{SHORT_LEASE}{NSFW}")).await;
        let hold = a.hold_webhook(1, |c| matches!(c, Call::Webhook(_, EventKind::Finished, _)));
        let t = a.submit(refused()).await.id;
        hold.entered("a delivers the finished webhook").await;
        a.crash();

        // B sends to another receiver: A's event isn't its to send, even once
        // A's claim has lapsed.
        let b = fleet
            .start_with("b", &toml, SinkKind::Target("test://other"))
            .await;
        b.recover().await;
        tokio::time::sleep(3 * LEASE).await;
        let (taken, sent) = (claims_of(&fleet, "b", &t), fleet.log.finished(&t));

        // C sends to the fleet's receiver, as A did.
        let c = fleet.start("c", &toml).await;
        c.recover().await;
        fleet.log.delivered("c", &t).await;

        assert_eq!(
            (taken, sent, fleet.log.finished(&t)),
            (0, vec![], vec![("c", TaskState::Failed)]),
            "(B's claims, finished webhooks before C, and after)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_instance_with_webhooks_off_settles_only_its_own_items() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", &format!("{SHORT_LEASE}{NSFW}")).await;
        // A gives up on its event: it's pending, and anyone's to send.
        a.fail_webhooks(3);
        let t = a.submit(refused()).await.id;
        fleet
            .log
            .wait_for("a hands the delivery back", |who, c| {
                who == "a" && matches!(c, Call::ReleaseWebhook(id, 0) if *id == t)
            })
            .await;

        // B has webhooks off: it neither sends nor settles A's event, at its
        // start or a lease later.
        let b = fleet
            .start_with("b", &format!("{SHORT_LEASE}{NSFW}"), SinkKind::Off)
            .await;
        b.recover().await;
        tokio::time::sleep(2 * LEASE).await;
        let pending = fleet.store.items_pending_webhook().await.unwrap();
        assert!(
            pending.iter().any(|p| p.task_id == t),
            "A's event is still pending: {pending:?}"
        );

        // B's own task leaves nothing pending.
        let own = b.submit(refused()).await.id;
        fleet
            .log
            .wait_for("b fails its task's item", |who, c| {
                who == "b"
                    && matches!(c, Call::SetItemState(id, 0, ItemState::Failed, true) if *id == own)
            })
            .await;
        b.wait_idle().await;
        let pending = fleet.store.items_pending_webhook().await.unwrap();
        assert!(
            !pending.iter().any(|p| p.task_id == own),
            "B's own event is pending: {pending:?}"
        );
        assert_eq!(
            fleet.log.count(
                |who, c| who == "b" && matches!(c, Call::Webhook(..) | Call::ClaimWebhooks(_))
            ),
            0,
            "B sent or claimed a webhook"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_sends_a_crashed_instances_event_without_a_redelivery_loop() {
    fleet_test(|fleet| async move {
        let a = fleet.start("a", &format!("{SHORT_LEASE}{NSFW}")).await;
        let hold = a.hold_webhook(1, |c| matches!(c, Call::Webhook(_, EventKind::Finished, _)));
        let t = a.submit(refused()).await.id;
        hold.entered("a delivers the finished webhook").await;
        a.crash();

        // B has no redelivery loop: its start sends what lapsed once it has
        // settled.
        let no_loop = "[webhook]\nurl = \"http://127.0.0.1:9\"\nredelivery_secs = 0\n";
        let b = fleet
            .start("b", &format!("{no_loop}{SHORT_LEASE}{NSFW}"))
            .await;
        b.recover().await;
        fleet.log.delivered("b", &t).await;
        assert_eq!(fleet.log.finished(&t), [("b", TaskState::Failed)]);
    })
    .await;
}
