//! Connection age. A gRPC connection open for `[app].max_connection_age_secs`
//! (give or take 10%) is told to finish (GOAWAY), so its client reconnects,
//! perhaps to another instance behind an L4 or DNS balancer. The calls still
//! open on it then get `[app].max_connection_age_grace_secs` before it is cut.
//!
//! tonic's own `max_connection_age` doesn't do that: in 0.14.6 it panics the
//! connection's task right after the GOAWAY, and with a grace set it sends no
//! GOAWAY at all, only cutting the connection once both have passed. So each
//! connection gets a server of its own, whose shutdown is its age.

use std::hash::{BuildHasher, RandomState};
use std::io;
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tonic::transport::server::Router;

use crate::stop::Connection;

/// How long connections stay open.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Aging {
    /// A connection is told to finish this long after it opened, give or
    /// take 10%.
    pub(crate) age: Duration,
    /// It is cut this long after that.
    pub(crate) grace: Duration,
}

impl Aging {
    /// From `[app]`'s seconds; `None` when the age is 0 (off).
    pub(crate) fn from_secs(age: u32, grace: u32) -> Option<Self> {
        (age > 0).then(|| Self {
            age: Duration::from_secs(age.into()),
            grace: Duration::from_secs(grace.into()),
        })
    }

    /// One connection's age: within 10% of `age`, at random, so connections
    /// opened together don't all reconnect together.
    fn pick(&self) -> Duration {
        // Each RandomState has fresh random keys.
        let random = RandomState::new().hash_one(0u8);
        let share = random as f64 / u64::MAX as f64;
        self.age.mul_f64(0.9 + 0.2 * share)
    }
}

/// Serve each connection `incoming` gives on a server of its own, which tells
/// it to finish at its age and cuts it `grace` later. Once `incoming` ends
/// (the stop), every open connection is told to finish at once; this ends
/// when they have.
pub(crate) async fn serve<S>(router: Router, incoming: S, aging: Aging)
where
    S: Stream<Item = io::Result<Connection>>,
{
    let (stop, stopping) = watch::channel(false);
    let mut open = JoinSet::new();
    tokio::pin!(incoming);
    loop {
        tokio::select! {
            conn = incoming.next() => match conn {
                Some(Ok(conn)) => {
                    open.spawn(serve_one(router.clone(), conn, aging, stopping.clone()));
                }
                // As tonic does: note it and accept the next.
                Some(Err(e)) => tracing::debug!(error = %e, "accept error"),
                None => break,
            },
            Some(_) = open.join_next() => {}
        }
    }
    let _ = stop.send(true);
    while open.join_next().await.is_some() {}
}

/// Serve `conn` until it closes. It is told to finish at its age or at the
/// stop, whichever comes first, and cut `grace` after its age.
async fn serve_one(
    router: Router,
    conn: Connection,
    aging: Aging,
    mut stopping: watch::Receiver<bool>,
) {
    let age = aging.pick();
    let (conn, closed) = conn.cut_after(age + aging.grace);
    let old = tokio::time::sleep(age);
    let finish = async move {
        tokio::select! {
            () = old => tracing::debug!(?age, "a connection reached its age: telling it to reconnect"),
            _ = stopping.wait_for(|stop| *stop) => {}
            // It closed first: nothing is left to serve.
            _ = closed => {}
        }
    };
    let conn =
        futures::stream::once(async { Ok::<_, io::Error>(conn) }).chain(futures::stream::pending());
    if let Err(e) = router.serve_with_incoming_shutdown(conn, finish).await {
        tracing::debug!(error = %e, "serving a connection failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ages_are_off_at_zero_and_spread_within_ten_percent() {
        assert_eq!(Aging::from_secs(0, 300), None);
        let aging = Aging::from_secs(100, 300).unwrap();
        assert_eq!(aging.grace, Duration::from_secs(300));
        let ages: Vec<Duration> = (0..1000).map(|_| aging.pick()).collect();
        let (low, high) = (Duration::from_secs(90), Duration::from_secs(110));
        assert!(ages.iter().all(|a| (low..=high).contains(a)), "{ages:?}");
        // Spread over the range, not all the same.
        assert!(ages.iter().any(|a| *a < Duration::from_secs(95)));
        assert!(ages.iter().any(|a| *a > Duration::from_secs(105)));
    }
}
