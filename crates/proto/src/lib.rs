//! Re-exports the gRPC types generated from the `apollo.v1` protobuf package.
//!
//! The schema is split across three files — `common.proto` (shared messages and
//! enums), `inference.proto` (the `Inference` service), and `webhook.proto` (the
//! `Webhook` service) — but all share the `apollo.v1` package, so the generated
//! code lands in a single module: the message types plus `inference_server` /
//! `inference_client` and `webhook_server` / `webhook_client`.

tonic::include_proto!("apollo.v1");

/// Self-contained `FileDescriptorSet` covering the shared messages and the
/// `Inference` service only. Register this with `tonic_reflection` on the apollo
/// server so tools like `grpcurl` discover `Inference` — and not `Webhook`.
pub const INFERENCE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/inference_descriptor.bin"));

/// Self-contained `FileDescriptorSet` covering the shared messages and the
/// `Webhook` service only. Register this on a webhook *receiver* so its reflection
/// advertises `Webhook` — and not `Inference`.
pub const WEBHOOK_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/webhook_descriptor.bin"));
