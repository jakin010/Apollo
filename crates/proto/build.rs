//! Compiles the `apollo.v1` protobuf schema into Rust (messages + gRPC
//! server/client) via tonic-prost-build, and emits a SEPARATE, self-contained
//! `FileDescriptorSet` per service so each side's gRPC reflection advertises only
//! the service it serves:
//!   * `inference_descriptor.bin` — shared messages + the `Inference` service
//!   * `webhook_descriptor.bin`   — shared messages + the `Webhook` service
//! Both land in `OUT_DIR` and are pulled in by `lib.rs`.
//!
//! Requires `protoc` on the build host (e.g. `apt-get install protobuf-compiler`
//! or `brew install protobuf`); set `PROTOC` to point at a specific binary.

use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let proto_dir = manifest.join("..").join("..").join("proto");

    let common = proto_dir.join("common.proto");
    let inference = proto_dir.join("inference.proto");
    let webhook = proto_dir.join("webhook.proto");

    for p in [&common, &inference, &webhook] {
        println!("cargo:rerun-if-changed={}", p.display());
    }

    // Generate Rust once from all three files: the shared messages plus both
    // service stubs, all under the single `apollo.v1` module.
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[common.clone(), inference.clone(), webhook.clone()],
            &[proto_dir.clone()],
        )?;

    // Emit one self-contained descriptor set per service. `--include_imports`
    // folds common.proto into each, so neither set references the other service.
    let protoc = env::var_os("PROTOC").unwrap_or_else(|| OsString::from("protoc"));
    descriptor_set(
        &protoc,
        &proto_dir,
        &inference,
        &out_dir.join("inference_descriptor.bin"),
    )?;
    descriptor_set(
        &protoc,
        &proto_dir,
        &webhook,
        &out_dir.join("webhook_descriptor.bin"),
    )?;

    Ok(())
}

/// Run `protoc` to write a self-contained `FileDescriptorSet` for `proto`.
fn descriptor_set(
    protoc: &OsString,
    include: &Path,
    proto: &Path,
    out: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new(protoc)
        .arg("--include_imports")
        .arg("-I")
        .arg(include)
        .arg(format!("--descriptor_set_out={}", out.display()))
        .arg(proto)
        .status()
        .map_err(|e| format!("failed to run protoc ({protoc:?}): {e}"))?;
    if !status.success() {
        return Err(format!("protoc failed to emit a descriptor for {}", proto.display()).into());
    }
    Ok(())
}
