//! Client SDK for the **apollo** inference gRPC service.
//!
//! Two halves, matching the two services in `inference.proto`:
//!
//! * [`Client`] — call the `Inference` service: [`Client::classify`],
//!   [`Client::get_task`].
//! * [`serve_webhook`] + [`WebhookHandler`] — stand up the `Webhook` receiver so
//!   apollo can push per-item results to you. Point the apollo server's
//!   `[webhook].url` at the address you serve on. Enable the `reflection` feature
//!   to expose gRPC server reflection on the receiver.
//!
//! All protobuf message types are re-exported here, so depending on this crate is
//! enough — you do not need `apollo-proto` directly. Build the inputs with the
//! [`item`] helpers.
//!
//! ```no_run
//! # async fn ex() -> Result<(), Box<dyn std::error::Error>> {
//! use apollo_client::{Client, item};
//!
//! let client = Client::connect("http://127.0.0.1:8080").await?;
//! let task_id = client
//!     .classify(item::image("https://example.com/cat.jpg", ["resnet"]))
//!     .await?;
//! let task = client.get_task(&task_id).await?;
//! println!("state = {:?}", task.state());
//! # Ok(()) }
//! ```
//!
//! ```no_run
//! # async fn ex() -> Result<(), Box<dyn std::error::Error>> {
//! use apollo_client::{serve_webhook, WebhookHandler, Task};
//!
//! struct Sink;
//! #[tonic::async_trait]
//! impl WebhookHandler for Sink {
//!     async fn on_task_status(&self, task: Task) {
//!         println!("task {} -> {:?}", task.id, task.state());
//!     }
//! }
//! serve_webhook("0.0.0.0:9090".parse()?, Sink).await?;
//! # Ok(()) }
//! ```

mod client;
mod webhook;

pub mod item;

pub use client::{Client, ClientBuilder, ClientError};
pub use webhook::{serve_webhook, serve_webhook_with_shutdown, WebhookHandler, WebhookReceiver};

// Re-export the wire types so downstreams don't need to depend on `apollo-proto`.
pub use apollo_proto::{
    Ack, CategoryScores, Classification, Frame, FrameScan, InputItem, ItemResult, ItemState,
    ModelResult, ModelState, Prediction, Task, TaskCreated, TaskState, Url,
};
/// The `InputItem.input` oneof (`ImageUrl`, `VideoUrl`, `Text`, `AudioUrl`).
pub use apollo_proto::input_item::Input as InputKind;
/// The `ModelResult.output` oneof (`Classification`, `FrameScan`).
pub use apollo_proto::model_result::Output as ModelOutput;

// `tonic::async_trait` is re-exported for implementing [`WebhookHandler`] without
// taking a direct dependency on the `async-trait` crate.
pub use tonic::async_trait;
