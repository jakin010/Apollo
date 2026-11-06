//! gRPC health (`grpc.health.v1.Health`) on the gRPC port, following the
//! process's [`Lifecycle`]:
//!
//! | Name | Starting | Serving | Draining | Stopping |
//! |---|---|---|---|---|
//! | `""`, `apollo.v1.Inference`, `readiness` | NOT_SERVING | SERVING | NOT_SERVING | cleared |
//! | `liveness` | SERVING | SERVING | SERVING | cleared |
//!
//! A cleared name ends its Watch streams (with OK), and Check answers it
//! NOT_FOUND, so an open Watch never holds a stop.

use tokio::sync::watch;
use tonic_health::ServingStatus;
use tonic_health::pb::health_server::HealthServer;
use tonic_health::server::{HealthReporter, HealthService};

use apollo_engine::{Lifecycle, Phase};
use apollo_proto::inference_server::SERVICE_NAME as INFERENCE;

/// What a name's status follows.
#[derive(Clone, Copy)]
enum Follows {
    /// SERVING while the lifecycle is ready for work.
    Readiness,
    /// SERVING until the stop.
    Liveness,
}

/// Every name, and what it follows.
const NAMES: [(&str, Follows); 4] = [
    ("", Follows::Readiness),
    (INFERENCE, Follows::Readiness),
    ("readiness", Follows::Readiness),
    ("liveness", Follows::Liveness),
];

/// Each name's status, in [`NAMES`] order; `None` once it is cleared.
type Answers = [Option<ServingStatus>; NAMES.len()];

/// What each name answers as `lc` stands now.
fn answers(lc: &Lifecycle) -> Answers {
    if lc.phase() >= Phase::Stopping {
        return [None; NAMES.len()];
    }
    NAMES.map(|(_, follows)| {
        let up = match follows {
            Follows::Readiness => lc.ready(),
            Follows::Liveness => true,
        };
        Some(if up {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        })
    })
}

/// The gRPC health names, following a [`Lifecycle`].
pub struct Health {
    reporter: HealthReporter,
}

impl Health {
    /// Every name, answering as `lc` stands now, and a task that follows it
    /// until the stop clears them. Call it inside the runtime.
    pub async fn new(lc: &Lifecycle) -> Self {
        // Changes from here on wake the task.
        let phase = lc.subscribe();
        let mut reporter = HealthReporter::new();
        // Sets `""` too, which tonic-health starts as SERVING.
        let now = answers(lc);
        for ((name, _), answer) in NAMES.iter().zip(now) {
            set(&mut reporter, name, answer).await;
        }
        tokio::spawn(follow(reporter.clone(), lc.clone(), phase, now));
        Self { reporter }
    }

    pub(crate) fn service(&self) -> HealthServer<HealthService> {
        HealthServer::new(HealthService::from_health_reporter(self.reporter.clone()))
    }
}

async fn set(reporter: &mut HealthReporter, name: &str, answer: Option<ServingStatus>) {
    match answer {
        Some(status) => reporter.set_service_status(name, status).await,
        None => reporter.clear_service_status(name).await,
    }
}

/// Apply each change in what the names answer, and only a change (each
/// update reaches every Watch), until they are cleared.
async fn follow(
    mut reporter: HealthReporter,
    lc: Lifecycle,
    mut phase: watch::Receiver<Phase>,
    mut last: Answers,
) {
    while last.iter().any(Option::is_some) {
        // `lc` keeps the sender, so this can't fail.
        if phase.changed().await.is_err() {
            return;
        }
        let now = answers(&lc);
        for (((name, _), was), is) in NAMES.iter().zip(last).zip(now) {
            if was != is {
                set(&mut reporter, name, is).await;
            }
        }
        last = now;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apollo_engine::Step;
    use futures::StreamExt;
    use tonic::Request;
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus as Wire;
    use tonic_health::pb::health_server::Health as _;

    use super::*;

    const READINESS: [&str; 3] = ["", INFERENCE, "readiness"];

    fn request(name: &str) -> Request<HealthCheckRequest> {
        Request::new(HealthCheckRequest {
            service: name.into(),
        })
    }

    /// What a Check of `name` answers: its status, or the error's code.
    async fn check(svc: &HealthService, name: &str) -> Result<Wire, tonic::Code> {
        svc.check(request(name))
            .await
            .map(|r| r.into_inner().status())
            .map_err(|s| s.code())
    }

    /// Wait (1 s at most) until a Check of `name` answers `want`.
    async fn answers_soon(svc: &HealthService, name: &str, want: Result<Wire, tonic::Code>) {
        let mut now = check(svc, name).await;
        for _ in 0..100 {
            if now == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            now = check(svc, name).await;
        }
        assert_eq!(now, want, "{name:?}");
    }

    type Watch = tonic_health::server::WatchStream;

    async fn watch(svc: &HealthService, name: &str) -> Watch {
        svc.watch(request(name)).await.unwrap().into_inner()
    }

    /// A Watch's next status (1 s at most); `None` when it ended.
    async fn next(watch: &mut Watch) -> Option<Wire> {
        tokio::time::timeout(Duration::from_secs(1), watch.next())
            .await
            .expect("an update or the end")
            .map(|r| r.unwrap().status())
    }

    /// Whether a Watch stays quiet for 100 ms.
    async fn quiet(watch: &mut Watch) -> bool {
        tokio::time::timeout(Duration::from_millis(100), watch.next())
            .await
            .is_err()
    }

    #[tokio::test]
    async fn health_is_not_serving_until_serving() {
        let lc = Lifecycle::new();
        let health = Health::new(&lc).await;
        let svc = HealthService::from_health_reporter(health.reporter.clone());
        let mut live = watch(&svc, "liveness").await;
        assert_eq!(next(&mut live).await, Some(Wire::Serving));
        let mut ready = watch(&svc, "").await;
        assert_eq!(next(&mut ready).await, Some(Wire::NotServing));
        assert_eq!(
            check(&svc, "apollo.v1.Webhook").await,
            Err(tonic::Code::NotFound)
        );

        for step in [
            Step::ConfiguringAuth,
            Step::RecoveringTasks,
            Step::LoadingModels,
        ] {
            lc.starting(step);
            for name in READINESS {
                assert_eq!(check(&svc, name).await, Ok(Wire::NotServing), "{name:?}");
            }
            assert_eq!(check(&svc, "liveness").await, Ok(Wire::Serving));
        }
        // A step changes no status, so watchers hear nothing.
        assert!(quiet(&mut ready).await && quiet(&mut live).await);

        lc.serving();
        assert_eq!(next(&mut ready).await, Some(Wire::Serving));
        for name in READINESS {
            answers_soon(&svc, name, Ok(Wire::Serving)).await;
        }
        assert_eq!(check(&svc, "liveness").await, Ok(Wire::Serving));
        assert!(quiet(&mut live).await, "liveness was serving throughout");
    }

    #[tokio::test]
    async fn health_watch_sees_the_drain() {
        let lc = Lifecycle::new();
        lc.serving();
        let health = Health::new(&lc).await;
        let svc = HealthService::from_health_reporter(health.reporter.clone());
        let mut ready = Vec::new();
        for name in READINESS {
            let mut w = watch(&svc, name).await;
            assert_eq!(next(&mut w).await, Some(Wire::Serving), "{name:?}");
            ready.push(w);
        }
        let mut live = watch(&svc, "liveness").await;
        assert_eq!(next(&mut live).await, Some(Wire::Serving));

        assert!(lc.drain());
        for w in &mut ready {
            assert_eq!(next(w).await, Some(Wire::NotServing));
        }
        assert!(quiet(&mut live).await, "liveness doesn't change");
        assert_eq!(check(&svc, "liveness").await, Ok(Wire::Serving));

        // The stop clears every name: watches end, checks find nothing.
        lc.stopping();
        for w in &mut ready {
            assert_eq!(next(w).await, None);
        }
        assert_eq!(next(&mut live).await, None);
        for name in READINESS.into_iter().chain(["liveness"]) {
            answers_soon(&svc, name, Err(tonic::Code::NotFound)).await;
            let late = svc.watch(request(name)).await.map(|_| ());
            assert_eq!(late.map_err(|s| s.code()), Err(tonic::Code::NotFound));
        }
    }
}
