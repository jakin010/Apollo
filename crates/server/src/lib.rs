//! `apollo-server` — the gRPC surface.
//!
//! - `service` — the `Inference` service (Classify, ClassifyStream, GetTask,
//!   WatchTask, ListTasks, CancelTask, ListModels), backed by an
//!   [`apollo_engine::Engine`], plus the `serve` helpers.
//! - `convert` — proto <-> `apollo-domain` conversions (the engine is wire-free,
//!   so all protobuf knowledge lives here).
//! - `webhook` — [`GrpcWebhookSink`], the concrete outbound `TaskEvent` client
//!   injected into the engine.
//! - `auth` — PASETO v4 (asymmetric) token verification with per-token
//!   revocation, applied to the `Inference` service as a tonic interceptor
//!   (health/reflection stay open); identifies the [`Caller`] that owns tasks.
//!
//! Model validation is intentionally *not* duplicated here: `Engine::submit`
//! rejects unknown/disabled models and modality mismatches synchronously, and
//! [`service`] maps that error to gRPC `InvalidArgument`. Only the wire-level
//! checks (a set `media` and `run`, metadata and `wait_seconds` limits) live in
//! `convert`.

mod auth;
mod convert;
mod service;
mod webhook;

pub use auth::{AuthInterceptor, AuthSettings, Caller, KeyError, Revocation};
pub use service::{InferenceService, TlsSettings, inference_service, serve, serve_with_shutdown};
pub use webhook::GrpcWebhookSink;
