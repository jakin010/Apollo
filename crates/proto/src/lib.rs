//! Re-exports the gRPC types generated from `proto/inference.proto`.
//!
//! Generated under the `apollo.v1` package: the message types plus
//! `inference_server` / `inference_client` and `webhook_server` /
//! `webhook_client` modules.

tonic::include_proto!("apollo.v1");

/// Encoded protobuf `FileDescriptorSet` for the `apollo.v1` package. Feed this to
/// `tonic_reflection` so clients like `grpcurl` can discover the service schema.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/apollo_descriptor.bin"));
