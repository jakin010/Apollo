//! Database contact: an instance whose link to the shared database stalls
//! reports the database stale, and only that instance; it is ok again a
//! third of a lease after the link is back.

use super::*;

use crate::Database;

/// The database state `instance` reports now.
fn database(instance: &Instance) -> Database {
    instance.engine.lifecycle().load().database
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_link_makes_only_its_instance_stale() {
    fleet_test(|fleet| async move {
        let toml = format!("{SHORT_LEASE}{NSFW}");
        let a = fleet.start("a", &toml).await;
        let b = fleet.start("b", &toml).await;
        a.recover().await;
        b.recover().await;
        for instance in [&a, &b] {
            assert!(matches!(database(instance), Database::Ok { .. }));
            assert!(instance.engine.lifecycle().ready_strict());
        }

        // A's reads hang; B's don't.
        let cut = a.hold_storage(usize::MAX, |c| matches!(c, Call::Claimable));
        let cut_at = Instant::now();
        wait_until("a reports the database stale", || database(&a).is_stale()).await;
        // Two leases with no answer, then the next sample.
        assert!(cut_at.elapsed() < 2 * LEASE + Duration::from_secs(2));
        let lc = a.engine.lifecycle();
        assert!(lc.ready() && !lc.ready_strict());
        assert_eq!(lc.weight(), 1);
        assert_eq!(lc.load().degraded(), ["database"]);
        // B stays in touch all along.
        for _ in 0..10 {
            assert!(matches!(database(&b), Database::Ok { .. }));
            assert!(b.engine.lifecycle().ready_strict());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // A's link is back: ok once it has been in touch for a third of a
        // lease.
        cut.open();
        let back = Instant::now();
        wait_until("a reports the database ok", || {
            matches!(database(&a), Database::Ok { .. })
        })
        .await;
        assert!(back.elapsed() >= LEASE / 3, "{:?}", back.elapsed());
        let since = a.engine.inner.connected_since.lock().unwrap().unwrap();
        assert!(since.elapsed() >= LEASE / 3);
        assert!(a.engine.lifecycle().ready_strict());
        assert_eq!(a.engine.lifecycle().load().degraded(), Vec::<&str>::new());
    })
    .await;
}
