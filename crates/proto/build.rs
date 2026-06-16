//! Compiles `proto/inference.proto` into Rust (messages + gRPC server/client) via
//! tonic-prost-build, and emits an encoded `FileDescriptorSet` so the server can
//! offer gRPC reflection. Output lands in `OUT_DIR` and is pulled in by `lib.rs`.
//!
//! Requires `protoc` on the build host (e.g. `apt-get install protobuf-compiler`
//! or `brew install protobuf`).

use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let proto_dir = manifest.join("..").join("..").join("proto");
    let proto = proto_dir.join("inference.proto");

    println!("cargo:rerun-if-changed={}", proto.display());

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("apollo_descriptor.bin"))
        .compile_protos(&[proto], &[proto_dir])?;

    Ok(())
}
