//! `apollo-server` — the gRPC surface.
//!
//! - `service` — the `Inference` service (Classify / ClassifyBatch / GetTask),
//!   backed by an [`apollo_engine::Engine`], plus the `serve` helpers.
//! - `convert` — proto <-> `apollo-domain` conversions (the engine is wire-free,
//!   so all protobuf knowledge lives here).
//! - `webhook` — [`GrpcWebhookSink`], the concrete outbound `TaskStatus` client
//!   injected into the engine.
//!
//! Request validation is intentionally *not* duplicated here: `Engine::submit`
//! rejects unknown/disabled models and modality mismatches synchronously, and
//! [`service`] maps that error to gRPC `InvalidArgument`.

mod convert;
mod service;
mod webhook;

pub use service::{inference_service, serve, serve_with_shutdown, InferenceService};
pub use webhook::GrpcWebhookSink;
