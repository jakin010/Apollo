//! Client SDK for the **apollo** inference gRPC service.
//!
//! Two halves, matching the two services in `inference.proto`:
//!
//! * [`Client`] — call the `Inference` service: [`Client::classify`],
//!   [`Client::classify_stream`], [`Client::get_task`], [`Client::watch_task`],
//!   [`Client::list_tasks`], [`Client::cancel_task`], [`Client::list_models`].
//!   `https://` endpoints use TLS.
//! * [`serve_webhook`] + [`WebhookHandler`] — stand up the `Webhook` receiver so
//!   apollo can push task events to you. Point the apollo server's
//!   `[webhook].url` at the address you serve on. Enable the `reflection` feature
//!   to expose gRPC server reflection on the receiver.
//!
//! All protobuf message types are re-exported here, so depending on this crate is
//! enough — you do not need `apollo-proto` directly. Build requests with the
//! [`item`] helpers.
//!
//! ```no_run
//! # async fn ex() -> Result<(), Box<dyn std::error::Error>> {
//! use apollo_client::item::{self, ClassifyRequestExt};
//! use apollo_client::{Client, TaskState};
//!
//! // The server requires a token by default (mint one with `apollo token`).
//! let client = Client::builder("http://127.0.0.1:8080")
//!     .token("v4.public.…")
//!     .build()
//!     .await?;
//! let task = client
//!     .classify(item::image("https://example.com/cat.jpg", ["resnet"]).wait_seconds(30))
//!     .await?;
//! if task.state() == TaskState::Completed {
//!     for result in &task.results {
//!         println!("{}: {:?}", result.model, result.output);
//!     }
//! }
//! # Ok(()) }
//! ```
//!
//! ```no_run
//! # async fn ex() -> Result<(), Box<dyn std::error::Error>> {
//! use apollo_client::{serve_webhook, WebhookHandler, TaskEvent, Ack};
//! use tonic::{Response, Status};
//!
//! struct Sink;
//! #[tonic::async_trait]
//! impl WebhookHandler for Sink {
//!     async fn on_event(&self, event: TaskEvent) -> Result<Response<Ack>, Status> {
//!         let kind = event.r#type();
//!         let task = event.task.unwrap_or_default();
//!         println!("{kind:?}: task {} is {:?}", task.id, task.state());
//!         Ok(Response::new(Ack {}))
//!     }
//! }
//! serve_webhook("0.0.0.0:9090".parse()?, Sink).await?;
//! # Ok(()) }
//! ```

mod client;
mod webhook;

pub mod item;

pub use client::{Client, ClientBuilder, ClientError, StreamInit};
pub use webhook::{WebhookHandler, WebhookReceiver, serve_webhook, serve_webhook_with_shutdown};

// Re-export the wire types so downstreams don't need to depend on `apollo-proto`.
/// The `ModelResult.output` oneof (`Classification`, `Scan`).
pub use apollo_proto::model_result::Output;
pub use apollo_proto::{
    Ack, Classification, ClassifyRequest, Error, ErrorType, Label, ListModelsResponse,
    ListTasksRequest, ListTasksResponse, Media, MediaKind, ModelInfo, ModelResult, ModelState,
    PipelineInfo, PipelineStepInfo, Prediction, Progress, ResultView, Sample, Scan, Segment,
    StopIf, Task, TaskEvent, TaskEventType, TaskState, Timestamp, Trigger,
};

/// The webhook signing scheme (header names, `sign`, `verify`), for verifying
/// deliveries in a receiver you build yourself.
pub use apollo_proto::webhook_signature;

// `tonic::async_trait` is re-exported for implementing [`WebhookHandler`] without
// taking a direct dependency on the `async-trait` crate.
pub use tonic::async_trait;
