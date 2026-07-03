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

#[test]
fn request_and_ack_messages() {
    let msgs = all_messages();

    let cr = fields_by_name(&msgs["ClassifyRequest"]);
    message(&cr["item"], 1, ".apollo.v1.InputItem", Label::Optional);
    assert_eq!(cr.len(), 1);

    // Three single-string request/response wrappers, all field #1.
    for name in ["GetTaskRequest", "CancelRequest", "TaskCreated"] {
        let f = fields_by_name(&msgs[name]);
        scalar(&f["task_id"], 1, Type::String, Label::Optional);
        assert_eq!(f.len(), 1, "{name} should have exactly one field");
    }

    assert!(msgs["Ack"].field.is_empty(), "Ack carries no fields");
}

#[test]
fn url_and_input_item() {
    let msgs = all_messages();

    let u = fields_by_name(&msgs["Url"]);
    scalar(&u["main"], 1, Type::String, Label::Optional);
    scalar(&u["fallback"], 2, Type::String, Label::Optional); // proto3 optional
    assert_eq!(u.len(), 2);

    // InputItem: a repeated scalar, a 4-arm oneof (fields 2..=5), and a trailing
    // proto3-optional scalar. oneof members appear as ordinary numbered fields.
    let i = fields_by_name(&msgs["InputItem"]);
    scalar(&i["models"], 1, Type::String, Label::Repeated);
    message(&i["image_url"], 2, ".apollo.v1.Url", Label::Optional);
    message(&i["video_url"], 3, ".apollo.v1.Url", Label::Optional);
    scalar(&i["text"], 4, Type::String, Label::Optional);
    message(&i["audio_url"], 5, ".apollo.v1.Url", Label::Optional);
    scalar(&i["pipeline"], 6, Type::String, Label::Optional);
    assert_eq!(i.len(), 6);
}

#[test]
fn task_and_result_messages() {
    let msgs = all_messages();

    let t = fields_by_name(&msgs["Task"]);
    scalar(&t["id"], 1, Type::String, Label::Optional);
    enumeration(&t["state"], 2, ".apollo.v1.TaskState", Label::Optional);
    message(&t["items"], 3, ".apollo.v1.ItemResult", Label::Repeated);
    assert_eq!(t.len(), 3);

    // `map<string, ModelResult> results = 2` is represented on the wire as a
    // repeated synthetic entry message.
    let ir = fields_by_name(&msgs["ItemResult"]);
    enumeration(&ir["state"], 1, ".apollo.v1.ItemState", Label::Optional);
    message(
        &ir["results"],
        2,
        ".apollo.v1.ItemResult.ResultsEntry",
        Label::Repeated,
    );
    message(&ir["error"], 3, ".apollo.v1.Error", Label::Optional);
    assert_eq!(ir.len(), 3);

    let mr = fields_by_name(&msgs["ModelResult"]);
    enumeration(&mr["state"], 1, ".apollo.v1.ModelState", Label::Optional);
    message(
        &mr["classification"],
        2,
        ".apollo.v1.Classification",
        Label::Optional,
    );
    message(
        &mr["frame_scan"],
        3,
        ".apollo.v1.FrameScan",
        Label::Optional,
    );
    message(&mr["error"], 4, ".apollo.v1.Error", Label::Optional);
    assert_eq!(mr.len(), 4);

    let e = fields_by_name(&msgs["Error"]);
    enumeration(&e["kind"], 1, ".apollo.v1.ErrorType", Label::Optional);
    scalar(&e["message"], 2, Type::String, Label::Optional);
    assert_eq!(e.len(), 2);
}

#[test]
fn classification_messages() {
    let msgs = all_messages();

    let c = fields_by_name(&msgs["Classification"]);
    message(
        &c["predictions"],
        1,
        ".apollo.v1.Prediction",
        Label::Repeated,
    );
    assert_eq!(c.len(), 1);

    let fs = fields_by_name(&msgs["FrameScan"]);
    message(
        &fs["aggregated"],
        1,
        ".apollo.v1.Classification",
        Label::Optional,
    );
    message(&fs["frames"], 2, ".apollo.v1.Frame", Label::Repeated);
    assert_eq!(fs.len(), 2);

    let fr = fields_by_name(&msgs["Frame"]);
    scalar(&fr["timestamp"], 1, Type::Double, Label::Optional);
    scalar(&fr["index"], 2, Type::Uint32, Label::Optional);
    message(
        &fr["classification"],
        3,
        ".apollo.v1.Classification",
        Label::Optional,
    );
    assert_eq!(fr.len(), 3);

    let p = fields_by_name(&msgs["Prediction"]);
    scalar(&p["label"], 1, Type::Uint32, Label::Optional);
    scalar(&p["score"], 2, Type::Float, Label::Optional);
    assert_eq!(p.len(), 2);
}

#[test]
fn stream_messages() {
    let msgs = all_messages();

    let si = fields_by_name(&msgs["ClassifyStreamInit"]);
    scalar(&si["models"], 1, Type::String, Label::Repeated);
    scalar(&si["video"], 2, Type::Bool, Label::Optional);
    assert_eq!(si.len(), 2);

    let cc = fields_by_name(&msgs["ClassifyChunk"]);
    message(
        &cc["init"],
        1,
        ".apollo.v1.ClassifyStreamInit",
        Label::Optional,
    );
    scalar(&cc["data"], 2, Type::Bytes, Label::Optional);
    assert_eq!(cc.len(), 2);
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

    assert_eq!(ItemState::Unspecified as i32, 0);
    assert_eq!(ItemState::Queued as i32, 1);
    assert_eq!(ItemState::Processing as i32, 2);
    assert_eq!(ItemState::Completed as i32, 3);
    assert_eq!(ItemState::Failed as i32, 4);
    assert_eq!(ItemState::Cancelled as i32, 5);
    assert_eq!(ItemState::Retrying as i32, 6);

    assert_eq!(ModelState::Unspecified as i32, 0);
    assert_eq!(ModelState::Queued as i32, 1);
    assert_eq!(ModelState::Processing as i32, 2);
    assert_eq!(ModelState::Done as i32, 3);
    assert_eq!(ModelState::Failed as i32, 4);
    assert_eq!(ModelState::Skipped as i32, 5);
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
            task_id: "x".into()
        }),
        (1, LEN)
    );

    // embedded message (length-delimited), field 1
    assert_eq!(
        first_wire_tag(&ClassifyRequest {
            item: Some(InputItem {
                models: vec!["m".into()],
                ..Default::default()
            }),
        }),
        (1, LEN)
    );

    // uint32 (varint) field 1 and float (32-bit) field 2 on Prediction
    assert_eq!(
        first_wire_tag(&Prediction {
            label: 7,
            score: 0.0
        }),
        (1, VARINT)
    );
    assert_eq!(
        first_wire_tag(&Prediction {
            label: 0,
            score: 1.5
        }),
        (2, I32)
    );

    // double (64-bit) field 1 on Frame
    assert_eq!(
        first_wire_tag(&Frame {
            timestamp: 2.5,
            ..Default::default()
        }),
        (1, I64)
    );

    // bool (varint) field 2 on ClassifyStreamInit
    assert_eq!(
        first_wire_tag(&ClassifyStreamInit {
            models: vec![],
            video: true,
        }),
        (2, VARINT)
    );

    // bytes (length-delimited) field 2, via the ClassifyChunk oneof
    assert_eq!(
        first_wire_tag(&ClassifyChunk {
            payload: Some(classify_chunk::Payload::Data(vec![1, 2, 3])),
        }),
        (2, LEN)
    );

    // enum (varint) field 2 — Task.state set to a non-zero value
    assert_eq!(
        first_wire_tag(&Task {
            state: TaskState::Queued as i32,
            ..Default::default()
        }),
        (2, VARINT)
    );

    // oneof arms carry their own field numbers: image_url #2 (message), text #4 (string)
    assert_eq!(
        first_wire_tag(&InputItem {
            input: Some(input_item::Input::ImageUrl(Url {
                main: "u".into(),
                fallback: None,
            })),
            ..Default::default()
        }),
        (2, LEN)
    );
    assert_eq!(
        first_wire_tag(&InputItem {
            input: Some(input_item::Input::Text("hi".into())),
            ..Default::default()
        }),
        (4, LEN)
    );

    // repeated string is NOT packed (only scalars-with-fixed-wire are): each
    // element is its own length-delimited field #1.
    assert_eq!(
        first_wire_tag(&ClassifyStreamInit {
            models: vec!["only".into()],
            video: false,
        }),
        (1, LEN)
    );
}

#[test]
fn full_task_round_trips() {
    // A populated Task exercising nested messages, the results map, a oneof
    // output, repeated predictions, an item-level Error, and every enum family.
    let task = Task {
        id: "task-123".into(),
        state: TaskState::Completed as i32,
        items: vec![
            ItemResult {
                state: ItemState::Completed as i32,
                results: std::collections::HashMap::from([(
                    "vit".to_string(),
                    ModelResult {
                        state: ModelState::Done as i32,
                        output: Some(model_result::Output::Classification(Classification {
                            predictions: vec![
                                Prediction {
                                    label: 3,
                                    score: 0.97,
                                },
                                Prediction {
                                    label: 8,
                                    score: 0.42,
                                },
                            ],
                        })),
                        error: None,
                    },
                )]),
                error: None,
            },
            ItemResult {
                state: ItemState::Failed as i32,
                results: std::collections::HashMap::new(),
                error: Some(Error {
                    kind: ErrorType::Fetch as i32,
                    message: "could not fetch input".into(),
                }),
            },
        ],
    };

    let mut buf = Vec::new();
    task.encode(&mut buf).expect("encode");
    let decoded = Task::decode(&buf[..]).expect("decode");
    assert_eq!(
        task, decoded,
        "Task should survive an encode/decode round trip"
    );
}
