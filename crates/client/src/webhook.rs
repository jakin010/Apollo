//! The `Webhook` receiver: serve this to get per-item results pushed to you.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use apollo_proto::webhook_server::{Webhook, WebhookServer};
use apollo_proto::{Ack, Task};

/// Handle task-status callbacks. Invoked once per item as it reaches a terminal
/// state, carrying the full current [`Task`]. Delivery is at-least-once, so
/// dedupe (e.g. by `Task.id` plus which items are terminal).
///
/// Implement with `#[tonic::async_trait]` (re-exported as
/// [`apollo_client::async_trait`](crate::async_trait)).
#[tonic::async_trait]
pub trait WebhookHandler: Send + Sync + 'static {
    async fn on_task_status(&self, task: Task);
}

struct Service<H> {
    handler: Arc<H>,
}

// Manual Clone: tonic clones the service per connection, and `Arc<H>` is always
// cloneable regardless of whether `H` is.
impl<H> Clone for Service<H> {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
        }
    }
}

#[tonic::async_trait]
impl<H: WebhookHandler> Webhook for Service<H> {
    async fn task_status(&self, request: Request<Task>) -> Result<Response<Ack>, Status> {
        self.handler.on_task_status(request.into_inner()).await;
        Ok(Response::new(Ack {}))
    }
}

/// Serve the webhook receiver on `addr` until the process exits.
pub async fn serve_webhook<H: WebhookHandler>(
    addr: SocketAddr,
    handler: H,
) -> Result<(), tonic::transport::Error> {
    let svc = Service {
        handler: Arc::new(handler),
    };
    Server::builder()
        .add_service(WebhookServer::new(svc))
        .serve(addr)
        .await
}

/// Like [`serve_webhook`], but stops gracefully when `shutdown` resolves.
pub async fn serve_webhook_with_shutdown<H, F>(
    addr: SocketAddr,
    handler: H,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    H: WebhookHandler,
    F: Future<Output = ()>,
{
    let svc = Service {
        handler: Arc::new(handler),
    };
    Server::builder()
        .add_service(WebhookServer::new(svc))
        .serve_with_shutdown(addr, shutdown)
        .await
}
