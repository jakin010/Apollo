//! Draining before a stop: a draining instance still runs what it is sent but
//! takes nothing over from other instances, and a stop that can't reach the
//! database ends at the hand-back limit.

use super::*;

use crate::HAND_BACK_LIMIT;

/// Redelivery every second, to the fleet's receiver.
const REDELIVERY: &str = "[webhook]\nurl = \"http://127.0.0.1:9\"\nredelivery_secs = 1\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_instance_takes_nothing_over() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let toml = format!("{REDELIVERY}{SHORT_LEASE}{NSFW}");
        let b = fleet.start("b", &toml).await;
        b.recover().await;
        let a = fleet.start("a", &format!("{SHORT_LEASE}{NSFW}")).await;
        // A dies running one task and delivering another's finished webhook,
        // while B drains.
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let _download = origin.next("a downloads the input").await;
        let hold = a.hold_webhook(1, |c| matches!(c, Call::Webhook(_, EventKind::Finished, _)));
        let w = a.submit(refused()).await.id;
        hold.entered("a delivers the finished webhook").await;
        assert!(b.engine.lifecycle().drain());
        a.crash();

        // Both claims lapse within a lease: B, settled, takes neither over.
        let taken = origin.next_within(4 * LEASE).await;
        assert!(taken.is_none(), "B took the task over while draining");
        let claims = |f: fn(&Call) -> bool| fleet.log.count(|who, c| who == "b" && f(c));
        assert_eq!(
            (
                claims(|c| matches!(c, Call::Claim(..))),
                claims(|c| matches!(c, Call::ClaimWebhooks(_)))
            ),
            (0, 0),
            "(B's task claims, B's webhook claims)"
        );

        // An instance that isn't draining takes both.
        let c = fleet.start("c", &toml).await;
        c.recover().await;
        origin
            .next("c takes the task over")
            .await
            .reply(Reply::Garbage);
        fleet.log.delivered("c", &t).await;
        fleet.log.delivered("c", &w).await;
        c.wait_idle().await;
        assert_eq!(
            (
                fleet.log.finished(&t),
                fleet.log.finished(&w),
                fleet.db.attempts(&t).await
            ),
            (
                vec![("c", TaskState::Failed)],
                vec![("c", TaskState::Failed)],
                1
            ),
            "(finished webhooks of the task, of the other task, the task's attempts)"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_on_a_stalled_link_ends_within_the_limit() {
    fleet_test(|fleet| async move {
        let origin = Origin::start();
        let toml = format!("{SHORT_LEASE}{NSFW}");
        let b = fleet.start("b", &toml).await;
        b.recover().await;
        let a = fleet.start("a", &toml).await;
        let t = a.submit(image_at(&origin.url("/t.png"))).await.id;
        let download_a = origin.next("a downloads the input").await;

        // A's link stalls as it stops: its renewals and its hand-back hang.
        let _cut = a.hold_storage(usize::MAX, |c| {
            matches!(c, Call::Renew(n) if *n > 0) || matches!(c, Call::Release(_))
        });
        let started = Instant::now();
        let handed_back = a.on(|e| async move { e.hand_back().await }).await;
        let took = started.elapsed();
        assert!(!handed_back);
        assert!(
            took >= HAND_BACK_LIMIT && took < HAND_BACK_LIMIT + Duration::from_secs(1),
            "gave up after {took:?}"
        );
        a.crash();

        // Its claim lapses, and the takeover counts an attempt.
        let download_b = origin.next("b takes the task over").await;
        download_a.reply(Reply::Garbage);
        download_b.reply(Reply::Garbage);
        fleet.log.delivered("b", &t).await;
        b.wait_idle().await;
        assert_eq!(
            (fleet.db.attempts(&t).await, fleet.log.finished(&t)),
            (1, vec![("b", TaskState::Failed)]),
            "(attempts, finished webhooks)"
        );
    })
    .await;
}
