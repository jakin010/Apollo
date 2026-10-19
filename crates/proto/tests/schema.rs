//! Schema-stability tests for the `apollo.v1` protobuf messages.
//!
//! Field numbers and wire types are the *contract*: a serialized `Task` in the
//! database, or a client built against an older schema, decodes purely by field
//! number. Renaming a field is source-compatible and harmless on the wire;
//! renumbering one, reusing a retired number, or changing a field's type is a
//! silent, backward-incompatible break that the compiler will not catch. These
//! tests pin the contract so any such change fails loudly and forces a conscious
//! decision.
//!
//! Two complementary layers:
//!   1. **Descriptor** — decode the generated `FileDescriptorSet` and assert every
//!      message's fields by (name → number, type, cardinality, referenced type).
//!      This reads the compiled schema directly, so it covers names and exact
//!      proto types (int32 vs uint32 vs enum, which share a wire type).
//!   2. **Behavioral** — encode the actual generated Rust structs and inspect the
//!      bytes, confirming the structs the rest of the codebase uses really do
//!      serialize with those field numbers and wire types; plus a full
//!      encode/decode round trip over a populated `Task`.

use apollo_proto::*;
use prost::Message;
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Descriptor helpers
// ---------------------------------------------------------------------------

/// Decode both service descriptor sets and index every top-level message by its
/// simple name. `INFERENCE_DESCRIPTOR_SET` alone already folds in `common.proto`
/// (via `--include_imports`), but merging both is robust if the split changes.
fn all_messages() -> BTreeMap<String, DescriptorProto> {
    let mut out = BTreeMap::new();
    for bytes in [INFERENCE_DESCRIPTOR_SET, WEBHOOK_DESCRIPTOR_SET] {
        let fds = FileDescriptorSet::decode(bytes).expect("descriptor set should decode");
        for file in fds.file {
            for msg in file.message_type {
                let name = msg.name().to_string();
                out.entry(name).or_insert(msg);
            }
        }
    }
    out
}

fn fields_by_name(msg: &DescriptorProto) -> BTreeMap<String, FieldDescriptorProto> {
    msg.field
        .iter()
        .map(|f| (f.name().to_string(), f.clone()))
        .collect()
}

/// Assert a scalar field's number, proto type, and cardinality.
#[track_caller]
fn scalar(f: &FieldDescriptorProto, number: i32, ty: Type, label: Label) {
    assert_eq!(f.number(), number, "`{}` field number", f.name());
    assert_eq!(f.r#type(), ty, "`{}` proto type", f.name());
    assert_eq!(f.label(), label, "`{}` cardinality", f.name());
}

/// Assert a message-typed field's number, referenced type, and cardinality.
#[track_caller]
fn message(f: &FieldDescriptorProto, number: i32, type_name: &str, label: Label) {
    assert_eq!(f.number(), number, "`{}` field number", f.name());
    assert_eq!(
        f.r#type(),
        Type::Message,
        "`{}` should be a message",
        f.name()
    );
    assert_eq!(f.type_name(), type_name, "`{}` referenced type", f.name());
    assert_eq!(f.label(), label, "`{}` cardinality", f.name());
}

/// Assert an enum-typed field's number, referenced enum, and cardinality.
#[track_caller]
fn enumeration(f: &FieldDescriptorProto, number: i32, type_name: &str, label: Label) {
    assert_eq!(f.number(), number, "`{}` field number", f.name());
    assert_eq!(f.r#type(), Type::Enum, "`{}` should be an enum", f.name());
    assert_eq!(f.type_name(), type_name, "`{}` referenced type", f.name());
    assert_eq!(f.label(), label, "`{}` cardinality", f.name());
}

// ---------------------------------------------------------------------------
// Per-message field pins (numbers + types + cardinality)
// ---------------------------------------------------------------------------

use Label::{Optional, Repeated};

#[test]
fn classify_requests() {
    let msgs = all_messages();

    let m = fields_by_name(&msgs["Media"]);
    enumeration(&m["kind"], 1, ".apollo.v1.MediaKind", Optional);
    scalar(&m["url"], 2, Type::String, Optional);
    scalar(&m["data"], 3, Type::Bytes, Optional);
    scalar(&m["fallback_url"], 4, Type::String, Optional);
    assert_eq!(m.len(), 4);

    let ml = fields_by_name(&msgs["ModelList"]);
    scalar(&ml["names"], 1, Type::String, Repeated);
    assert_eq!(ml.len(), 1);

    let cr = fields_by_name(&msgs["ClassifyRequest"]);
    message(&cr["media"], 1, ".apollo.v1.Media", Optional);
    scalar(&cr["text"], 8, Type::String, Optional);
    message(&cr["models"], 2, ".apollo.v1.ModelList", Optional);
    scalar(&cr["pipeline"], 3, Type::String, Optional);
    message(
        &cr["metadata"],
        4,
        ".apollo.v1.ClassifyRequest.MetadataEntry",
        Repeated,
    );
    scalar(&cr["request_id"], 5, Type::String, Optional);
    scalar(&cr["wait_seconds"], 6, Type::Uint32, Optional);
    enumeration(&cr["view"], 7, ".apollo.v1.ResultView", Optional);
    assert_eq!(cr.len(), 8);

    // The stream's init mirrors ClassifyRequest, with `kind` for `media`.
    let si = fields_by_name(&msgs["ClassifyStreamInit"]);
    enumeration(&si["kind"], 1, ".apollo.v1.MediaKind", Optional);
    message(&si["models"], 2, ".apollo.v1.ModelList", Optional);
    scalar(&si["pipeline"], 3, Type::String, Optional);
    message(
        &si["metadata"],
        4,
        ".apollo.v1.ClassifyStreamInit.MetadataEntry",
        Repeated,
    );
    scalar(&si["request_id"], 5, Type::String, Optional);
    scalar(&si["wait_seconds"], 6, Type::Uint32, Optional);
    enumeration(&si["view"], 7, ".apollo.v1.ResultView", Optional);
    assert_eq!(si.len(), 7);

    let cc = fields_by_name(&msgs["ClassifyChunk"]);
    message(&cc["init"], 1, ".apollo.v1.ClassifyStreamInit", Optional);
    scalar(&cc["data"], 2, Type::Bytes, Optional);
    assert_eq!(cc.len(), 2);
}

#[test]
fn task_requests() {
    let msgs = all_messages();

    for name in ["GetTaskRequest", "WatchTaskRequest"] {
        let f = fields_by_name(&msgs[name]);
        scalar(&f["task_id"], 1, Type::String, Optional);
        enumeration(&f["view"], 2, ".apollo.v1.ResultView", Optional);
        assert_eq!(f.len(), 2, "{name}");
    }
    let c = fields_by_name(&msgs["CancelTaskRequest"]);
    scalar(&c["task_id"], 1, Type::String, Optional);
    assert_eq!(c.len(), 1);

    let lt = fields_by_name(&msgs["ListTasksRequest"]);
    enumeration(&lt["states"], 1, ".apollo.v1.TaskState", Repeated);
    message(
        &lt["created_after"],
        2,
        ".google.protobuf.Timestamp",
        Optional,
    );
    message(
        &lt["created_before"],
        3,
        ".google.protobuf.Timestamp",
        Optional,
    );
    scalar(&lt["page_size"], 4, Type::Uint32, Optional);
    scalar(&lt["page_token"], 5, Type::String, Optional);
    assert_eq!(lt.len(), 5);

    let ltr = fields_by_name(&msgs["ListTasksResponse"]);
    message(&ltr["tasks"], 1, ".apollo.v1.Task", Repeated);
    scalar(&ltr["next_page_token"], 2, Type::String, Optional);
    assert_eq!(ltr.len(), 2);

    assert!(msgs["ListModelsRequest"].field.is_empty());
    assert!(msgs["Ack"].field.is_empty(), "Ack carries no fields");
}

#[test]
fn model_discovery() {
    let msgs = all_messages();

    let r = fields_by_name(&msgs["ListModelsResponse"]);
    message(&r["models"], 1, ".apollo.v1.ModelInfo", Repeated);
    message(&r["pipelines"], 2, ".apollo.v1.PipelineInfo", Repeated);
    assert_eq!(r.len(), 2);

    let mi = fields_by_name(&msgs["ModelInfo"]);
    scalar(&mi["name"], 1, Type::String, Optional);
    scalar(&mi["architecture"], 2, Type::String, Optional);
    scalar(&mi["repo"], 3, Type::String, Optional);
    enumeration(&mi["accepts"], 4, ".apollo.v1.MediaKind", Repeated);
    scalar(&mi["enabled"], 5, Type::Bool, Optional);
    scalar(&mi["loaded"], 6, Type::Bool, Optional);
    message(&mi["labels"], 7, ".apollo.v1.Label", Repeated);
    message(&mi["trigger"], 8, ".apollo.v1.Trigger", Optional);
    scalar(&mi["video_strategy"], 9, Type::String, Optional);
    assert_eq!(mi.len(), 9);

    let l = fields_by_name(&msgs["Label"]);
    scalar(&l["id"], 1, Type::Uint32, Optional);
    scalar(&l["name"], 2, Type::String, Optional);
    assert_eq!(l.len(), 2);

    let t = fields_by_name(&msgs["Trigger"]);
    scalar(&t["labels"], 1, Type::Uint32, Repeated);
    scalar(&t["threshold"], 2, Type::Float, Optional);
    assert_eq!(t.len(), 2);

    let p = fields_by_name(&msgs["PipelineInfo"]);
    scalar(&p["name"], 1, Type::String, Optional);
    message(&p["steps"], 2, ".apollo.v1.PipelineStepInfo", Repeated);
    assert_eq!(p.len(), 2);

    let ps = fields_by_name(&msgs["PipelineStepInfo"]);
    scalar(&ps["models"], 1, Type::String, Repeated);
    message(&ps["stop_if"], 2, ".apollo.v1.StopIf", Optional);
    assert_eq!(ps.len(), 2);

    let si = fields_by_name(&msgs["StopIf"]);
    scalar(&si["model"], 1, Type::String, Optional);
    message(&si["trigger"], 2, ".apollo.v1.Trigger", Optional);
    assert_eq!(si.len(), 2);
}

#[test]
fn task_and_results() {
    let msgs = all_messages();

    let t = fields_by_name(&msgs["Task"]);
    scalar(&t["id"], 1, Type::String, Optional);
    enumeration(&t["state"], 2, ".apollo.v1.TaskState", Optional);
    message(&t["error"], 3, ".apollo.v1.Error", Optional);
    message(&t["results"], 4, ".apollo.v1.ModelResult", Repeated);
    scalar(&t["pipeline"], 5, Type::String, Optional);
    message(&t["metadata"], 6, ".apollo.v1.Task.MetadataEntry", Repeated);
    scalar(&t["request_id"], 7, Type::String, Optional);
    message(&t["created_at"], 8, ".google.protobuf.Timestamp", Optional);
    message(&t["updated_at"], 9, ".google.protobuf.Timestamp", Optional);
    message(
        &t["finished_at"],
        10,
        ".google.protobuf.Timestamp",
        Optional,
    );
    message(&t["progress"], 11, ".apollo.v1.Progress", Optional);
    scalar(&t["retries"], 12, Type::Uint32, Optional);
    assert_eq!(t.len(), 12);

    let p = fields_by_name(&msgs["Progress"]);
    scalar(&p["models_total"], 1, Type::Uint32, Optional);
    scalar(&p["models_finished"], 2, Type::Uint32, Optional);
    assert_eq!(p.len(), 2);

    let m = fields_by_name(&msgs["ModelResult"]);
    scalar(&m["model"], 1, Type::String, Optional);
    enumeration(&m["state"], 2, ".apollo.v1.ModelState", Optional);
    message(&m["error"], 3, ".apollo.v1.Error", Optional);
    message(
        &m["classification"],
        4,
        ".apollo.v1.Classification",
        Optional,
    );
    message(&m["scan"], 5, ".apollo.v1.Scan", Optional);
    assert_eq!(m.len(), 5);

    let e = fields_by_name(&msgs["Error"]);
    enumeration(&e["kind"], 1, ".apollo.v1.ErrorType", Optional);
    scalar(&e["message"], 2, Type::String, Optional);
    assert_eq!(e.len(), 2);
}

#[test]
fn classification_and_scan() {
    let msgs = all_messages();

    let c = fields_by_name(&msgs["Classification"]);
    message(&c["predictions"], 1, ".apollo.v1.Prediction", Repeated);
    assert_eq!(c.len(), 1);

    let p = fields_by_name(&msgs["Prediction"]);
    scalar(&p["label"], 1, Type::Uint32, Optional);
    scalar(&p["name"], 2, Type::String, Optional);
    scalar(&p["score"], 3, Type::Float, Optional);
    assert_eq!(p.len(), 3);

    let s = fields_by_name(&msgs["Scan"]);
    message(&s["aggregated"], 1, ".apollo.v1.Classification", Optional);
    message(&s["segments"], 2, ".apollo.v1.Segment", Repeated);
    message(&s["samples"], 3, ".apollo.v1.Sample", Repeated);
    scalar(&s["sample_count"], 4, Type::Uint32, Optional);
    assert_eq!(s.len(), 4);

    let sa = fields_by_name(&msgs["Sample"]);
    scalar(&sa["index"], 1, Type::Uint32, Optional);
    scalar(&sa["start"], 2, Type::Double, Optional);
    scalar(&sa["end"], 3, Type::Double, Optional);
    message(
        &sa["classification"],
        4,
        ".apollo.v1.Classification",
        Optional,
    );
    assert_eq!(sa.len(), 4);

    let sg = fields_by_name(&msgs["Segment"]);
    scalar(&sg["label"], 1, Type::Uint32, Optional);
    scalar(&sg["name"], 2, Type::String, Optional);
    scalar(&sg["start"], 3, Type::Double, Optional);
    scalar(&sg["end"], 4, Type::Double, Optional);
    scalar(&sg["peak"], 5, Type::Float, Optional);
    scalar(&sg["peak_at"], 6, Type::Double, Optional);
    scalar(&sg["samples"], 7, Type::Uint32, Optional);
    scalar(&sg["bound_start"], 8, Type::Double, Optional);
    scalar(&sg["bound_end"], 9, Type::Double, Optional);
    assert_eq!(sg.len(), 9);
}

#[test]
fn webhook_event() {
    let msgs = all_messages();
    let e = fields_by_name(&msgs["TaskEvent"]);
    enumeration(&e["type"], 1, ".apollo.v1.TaskEventType", Optional);
    message(&e["task"], 2, ".apollo.v1.Task", Optional);
    scalar(&e["attempt"], 3, Type::Uint32, Optional);
    message(&e["sent_at"], 4, ".google.protobuf.Timestamp", Optional);
    assert_eq!(e.len(), 4);
}

// ---------------------------------------------------------------------------
// Enum value pins — enum numbers are equally part of the wire contract.
// ---------------------------------------------------------------------------

#[test]
fn enum_values() {
    assert_eq!(ErrorType::Unspecified as i32, 0);
    assert_eq!(ErrorType::Fetch as i32, 1);
    assert_eq!(ErrorType::Decode as i32, 2);
    assert_eq!(ErrorType::Inference as i32, 3);
    assert_eq!(ErrorType::Timeout as i32, 4);
    assert_eq!(ErrorType::Cancelled as i32, 5);
    assert_eq!(ErrorType::ModelUnavailable as i32, 6);
    assert_eq!(ErrorType::Internal as i32, 7);

    assert_eq!(TaskState::Unspecified as i32, 0);
    assert_eq!(TaskState::Queued as i32, 1);
    assert_eq!(TaskState::Processing as i32, 2);
    assert_eq!(TaskState::Completed as i32, 3);
    assert_eq!(TaskState::Failed as i32, 4);
    assert_eq!(TaskState::Cancelled as i32, 5);

    assert_eq!(ModelState::Unspecified as i32, 0);
    assert_eq!(ModelState::Queued as i32, 1);
    assert_eq!(ModelState::Processing as i32, 2);
    assert_eq!(ModelState::Completed as i32, 3);
    assert_eq!(ModelState::Failed as i32, 4);
    assert_eq!(ModelState::Skipped as i32, 5);
    assert_eq!(ModelState::Cancelled as i32, 6);

    assert_eq!(MediaKind::Unspecified as i32, 0);
    assert_eq!(MediaKind::Image as i32, 1);
    assert_eq!(MediaKind::Video as i32, 2);
    assert_eq!(MediaKind::Audio as i32, 3);
    assert_eq!(MediaKind::Text as i32, 4);

    assert_eq!(ResultView::Unspecified as i32, 0);
    assert_eq!(ResultView::Summary as i32, 1);
    assert_eq!(ResultView::Full as i32, 2);

    assert_eq!(TaskEventType::Unspecified as i32, 0);
    assert_eq!(TaskEventType::Progress as i32, 1);
    assert_eq!(TaskEventType::Finished as i32, 2);
}

// ---------------------------------------------------------------------------
// Behavioral: the real Rust structs encode with the expected tags.
// ---------------------------------------------------------------------------

// protobuf wire types.
const VARINT: u64 = 0; // int32/64, uint32/64, bool, enum
const I64: u64 = 1; // double, fixed64
const LEN: u64 = 2; // string, bytes, embedded message, packed repeated
const I32: u64 = 5; // float, fixed32

/// Encode `msg` — which must have exactly ONE non-default field set — and return
/// the leading field key on the wire as `(field_number, wire_type)`. proto3 omits
/// default/zero fields, so the first varint emitted is that field's key:
/// `key = (field_number << 3) | wire_type`.
fn first_wire_tag<M: Message>(msg: &M) -> (u64, u64) {
    let mut buf = Vec::new();
    msg.encode(&mut buf).expect("encoding should not fail");
    assert!(
        !buf.is_empty(),
        "message encoded to zero bytes — set exactly one non-default field"
    );
    let mut key = 0u64;
    let mut shift = 0u32;
    for &b in &buf {
        key |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    (key >> 3, key & 0b111)
}

#[test]
fn wire_tags_cover_each_type() {
    // string (length-delimited), field 1
    assert_eq!(
        first_wire_tag(&GetTaskRequest {
            task_id: "x".into(),
            view: 0,
        }),
        (1, LEN)
    );
    // enum (varint), field 2
    assert_eq!(
        first_wire_tag(&GetTaskRequest {
            task_id: String::new(),
            view: ResultView::Full as i32,
        }),
        (2, VARINT)
    );

    // oneof arms carry their own field numbers: media #1, models #2, pipeline #3
    assert_eq!(
        first_wire_tag(&ClassifyRequest {
            input: Some(classify_request::Input::Media(Media::default())),
            ..Default::default()
        }),
        (1, LEN)
    );
    assert_eq!(
        first_wire_tag(&ClassifyRequest {
            run: Some(classify_request::Run::Pipeline("p".into())),
            ..Default::default()
        }),
        (3, LEN)
    );
    assert_eq!(
        first_wire_tag(&ClassifyRequest {
            wait_seconds: 5,
            ..Default::default()
        }),
        (6, VARINT)
    );
    assert_eq!(
        first_wire_tag(&Media {
            source: Some(media::Source::Data(vec![1])),
            ..Default::default()
        }),
        (3, LEN)
    );

    // uint32 (varint) field 1 and float (32-bit) field 3 on Prediction
    assert_eq!(
        first_wire_tag(&Prediction {
            label: 7,
            ..Default::default()
        }),
        (1, VARINT)
    );
    assert_eq!(
        first_wire_tag(&Prediction {
            score: 1.5,
            ..Default::default()
        }),
        (3, I32)
    );

    // double (64-bit) field 2 on Sample
    assert_eq!(
        first_wire_tag(&Sample {
            start: 2.5,
            ..Default::default()
        }),
        (2, I64)
    );

    // bytes (length-delimited) field 2, via the ClassifyChunk oneof
    assert_eq!(
        first_wire_tag(&ClassifyChunk {
            payload: Some(classify_chunk::Payload::Data(vec![1, 2, 3])),
        }),
        (2, LEN)
    );

    // enum state on Task is field 2 (varint)
    assert_eq!(
        first_wire_tag(&Task {
            state: TaskState::Queued as i32,
            ..Default::default()
        }),
        (2, VARINT)
    );

    // repeated string is NOT packed (only scalars-with-fixed-wire are): each
    // element is its own length-delimited field #1.
    assert_eq!(
        first_wire_tag(&ModelList {
            names: vec!["only".into()],
        }),
        (1, LEN)
    );
}

fn classification(label: u32, name: &str, score: f32) -> Classification {
    Classification {
        predictions: vec![Prediction {
            label,
            name: name.into(),
            score,
        }],
    }
}

#[test]
fn encoding_is_deterministic() {
    // Map entries are emitted in key order, so equal messages encode to equal
    // bytes regardless of insertion order — webhook signatures rely on this.
    let event = |pairs: &[(&str, &str)]| TaskEvent {
        r#type: TaskEventType::Finished as i32,
        task: Some(Task {
            id: "t".into(),
            metadata: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        }),
        attempt: 1,
        sent_at: None,
    };
    let a = event(&[("zeta", "1"), ("alpha", "2"), ("mid", "3")]).encode_to_vec();
    let b = event(&[("mid", "3"), ("zeta", "1"), ("alpha", "2")]).encode_to_vec();
    assert_eq!(a, b);
    // Re-encoding a decoded message reproduces the bytes exactly.
    assert_eq!(TaskEvent::decode(&a[..]).unwrap().encode_to_vec(), a);
}

#[test]
fn full_task_round_trips() {
    // A completed task with a classification, a scan with a segment and
    // samples, and a failed model, plus every task-level field.
    let task = Task {
        id: "task-123".into(),
        state: TaskState::Completed as i32,
        error: None,
        results: vec![
            ModelResult {
                model: "vit".into(),
                state: ModelState::Completed as i32,
                error: None,
                output: Some(model_result::Output::Classification(classification(
                    3, "cat", 0.97,
                ))),
            },
            ModelResult {
                model: "nsfw".into(),
                state: ModelState::Completed as i32,
                error: None,
                output: Some(model_result::Output::Scan(Scan {
                    aggregated: Some(classification(1, "nsfw", 0.9)),
                    segments: vec![Segment {
                        label: 1,
                        name: "nsfw".into(),
                        start: 4.0,
                        end: 6.0,
                        peak: 0.9,
                        peak_at: 5.0,
                        samples: 3,
                        bound_start: 2.0,
                        bound_end: 8.0,
                    }],
                    samples: vec![Sample {
                        index: 0,
                        start: 5.0,
                        end: 5.0,
                        classification: Some(classification(1, "nsfw", 0.9)),
                    }],
                    sample_count: 1,
                })),
            },
            ModelResult {
                model: "siglip".into(),
                state: ModelState::Failed as i32,
                error: Some(Error {
                    kind: ErrorType::Inference as i32,
                    message: "model errored".into(),
                }),
                output: None,
            },
        ],
        pipeline: "moderation".into(),
        metadata: BTreeMap::from([("ref".to_string(), "42".to_string())]),
        request_id: "r-1".into(),
        created_at: Some(Timestamp {
            seconds: 100,
            nanos: 0,
        }),
        updated_at: Some(Timestamp {
            seconds: 110,
            nanos: 0,
        }),
        finished_at: Some(Timestamp {
            seconds: 110,
            nanos: 0,
        }),
        progress: Some(Progress {
            models_total: 3,
            models_finished: 3,
        }),
        retries: 1,
    };

    let decoded = Task::decode(&task.encode_to_vec()[..]).expect("decode");
    assert_eq!(
        task, decoded,
        "Task should survive an encode/decode round trip"
    );
}
