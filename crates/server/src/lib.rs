//! `apollo-server` — the gRPC surface.
//!
//! - `service` — the `Inference` service (Classify / GetTask / CancelTask),
//!   backed by an [`apollo_engine::Engine`], plus the `serve` helpers.
//! - `convert` — proto <-> `apollo-domain` conversions (the engine is wire-free,
//!   so all protobuf knowledge lives here).
//! - `webhook` — [`GrpcWebhookSink`], the concrete outbound `TaskStatus` client
//!   injected into the engine.
//! - `auth` — optional PASETO v4 (asymmetric) token verification, applied to the
//!   `Inference` service as a tonic interceptor (health/reflection stay open).
//!
//! Request validation is intentionally *not* duplicated here: `Engine::submit`
//! rejects unknown/disabled models and modality mismatches synchronously, and
//! [`service`] maps that error to gRPC `InvalidArgument`.

mod auth;
mod convert;
mod service;
mod webhook;

pub use auth::{AuthInterceptor, KeyError};
pub use service::{inference_service, serve, serve_with_shutdown, InferenceService};
pub use webhook::GrpcWebhookSink;
