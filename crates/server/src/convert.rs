//! Conversions between proto messages and `apollo-domain` types.
//!
//! Free functions rather than `From` impls: both sides are foreign to this crate,
//! so the orphan rule rules out trait impls. Domain -> proto powers `GetTask` and
//! webhook delivery; proto -> domain turns a request into an engine `Submission`.

use std::collections::HashMap;

use apollo_domain as dom;
use apollo_engine::Submission;
use apollo_proto as pb;

// ------------------------------ proto -> domain ------------------------------

/// Turn a request `InputItem` into an engine [`Submission`]. Errors if the oneof
/// `input` was not set.
pub(crate) fn submission_from_proto(item: pb::InputItem) -> Result<Submission, String> {
    use pb::input_item::Input;
    let input = match item.input {
        Some(Input::ImageUrl(u)) => dom::Input::Image(url_from_proto(u)),
        Some(Input::VideoUrl(u)) => dom::Input::Video(url_from_proto(u)),
        Some(Input::Text(s)) => dom::Input::Text(s),
        Some(Input::AudioUrl(u)) => dom::Input::Audio(url_from_proto(u)),
        None => return Err("input item has no `input` set".into()),
    };
    if item.models.is_empty() && item.pipeline.is_none() {
        return Err("input item must set `models` or a `pipeline`".into());
    }
    Ok(Submission {
        input,
        models: item.models,
        pipeline: item.pipeline,
    })
}

fn url_from_proto(u: pb::Url) -> dom::Url {
    dom::Url {
        main: u.main,
        fallback: u.fallback,
    }
}

// ------------------------------ domain -> proto ------------------------------

/// Full task, for `GetTask` responses and webhook payloads. A domain task carries
/// exactly one input, which maps onto the `Task.result` oneof: a task-level error,
/// a non-terminal/cancelled `state`, or the per-model `models` once finished.
pub(crate) fn task_to_proto(t: dom::Task) -> pb::Task {
    let dom::Task {
        id,
        state,
        mut items,
    } = t;
    let result = (!items.is_empty()).then(|| task_result(state, items.remove(0)));
    pb::Task { id, result }
}

/// Map the (single) domain item + task state onto `Task.result`:
///   * an item-level error, or a `Failed` task -> `error` (implicit "failed");
///   * a `Completed` task                      -> `models` (implicit "completed");
///   * otherwise                     -> `state` (queued / processing / cancelled).
fn task_result(state: dom::TaskState, it: dom::Item) -> pb::task::Result {
    use pb::task::Result as R;
    if let Some(err) = it.error {
        return R::Error(error_to_proto(err));
    }
    match state {
        dom::TaskState::Completed => {
            let models: HashMap<String, pb::Model> = it
                .results
                .into_iter()
                .map(|(label, r)| (label, model_to_proto(r)))
                .collect();
            R::Models(pb::Models {
                pipeline: it.pipeline,
                models,
            })
        }
        // Failed with no attached error: surface a generic task-level error so the
        // oneof still distinguishes failure from a live state.
        dom::TaskState::Failed => R::Error(pb::Error {
            kind: pb::ErrorType::Unspecified as i32,
            message: "task failed".to_string(),
        }),
        dom::TaskState::Queued => R::State(pb::TaskState::Queued as i32),
        dom::TaskState::Processing => R::State(pb::TaskState::Processing as i32),
        dom::TaskState::Cancelled => R::State(pb::TaskState::Cancelled as i32),
    }
}

/// Map one domain model result onto `Model.result`: an error (implicit "failed"),
/// a classification / frame-scan output (implicit "done"), or a live `state`
/// (queued / processing / skipped).
fn model_to_proto(m: dom::ModelResult) -> pb::Model {
    use pb::model::Result as R;
    let result = if let Some(err) = m.error {
        R::Error(error_to_proto(err))
    } else if let Some(output) = m.output {
        match output {
            dom::ModelOutput::Classification(c) => R::Classification(classification_to_proto(c)),
            dom::ModelOutput::FrameScan(f) => R::FrameScan(frame_scan_to_proto(f)),
        }
    } else {
        R::State(model_state(m.state) as i32)
    };
    pb::Model {
        result: Some(result),
    }
}

fn classification_to_proto(c: dom::Classification) -> pb::Classification {
    pb::Classification {
        predictions: c.predictions.into_iter().map(prediction_to_proto).collect(),
    }
}

fn error_to_proto(e: dom::TaskError) -> pb::Error {
    pb::Error {
        kind: error_kind_to_proto(e.kind) as i32,
        message: e.message,
    }
}

fn error_kind_to_proto(k: dom::ErrorKind) -> pb::ErrorType {
    match k {
        dom::ErrorKind::Unspecified => pb::ErrorType::Unspecified,
        dom::ErrorKind::Fetch => pb::ErrorType::Fetch,
        dom::ErrorKind::Decode => pb::ErrorType::Decode,
        dom::ErrorKind::Inference => pb::ErrorType::Inference,
        dom::ErrorKind::Timeout => pb::ErrorType::Timeout,
        dom::ErrorKind::Cancelled => pb::ErrorType::Cancelled,
        dom::ErrorKind::ModelUnavailable => pb::ErrorType::ModelUnavailable,
        dom::ErrorKind::Internal => pb::ErrorType::Internal,
    }
}

fn prediction_to_proto(p: dom::Prediction) -> pb::Prediction {
    pb::Prediction {
        label: p.label,
        score: p.score,
    }
}

fn frame_scan_to_proto(f: dom::FrameScan) -> pb::FrameScan {
    pb::FrameScan {
        aggregated: Some(classification_to_proto(f.aggregated)),
        frames: f.frames.into_iter().map(frame_to_proto).collect(),
    }
}

fn frame_to_proto(fr: dom::Frame) -> pb::Frame {
    pb::Frame {
        timestamp: fr.timestamp,
        index: fr.index,
        classification: Some(classification_to_proto(fr.classification)),
    }
}

// enums (domain has no `Unspecified`; we never emit it)

/// Non-terminal / skipped model state. Done and Failed are conveyed by the
/// output / error arms of `Model.result`, so they never reach here.
fn model_state(s: dom::ModelState) -> pb::ModelState {
    match s {
        dom::ModelState::Queued => pb::ModelState::Queued,
        dom::ModelState::Processing => pb::ModelState::Processing,
        dom::ModelState::Skipped => pb::ModelState::Skipped,
        dom::ModelState::Done | dom::ModelState::Failed => pb::ModelState::Unspecified,
    }
}

#[cfg(test)]
mod tests {
    use super::{submission_from_proto, task_to_proto};
    use apollo_domain as dom;
    use apollo_proto as pb;

    fn pb_url(main: &str) -> pb::Url {
        pb::Url {
            main: main.into(),
            fallback: None,
        }
    }

    #[test]
    fn image_submission_carries_url_and_models() {
        let item = pb::InputItem {
            models: vec!["resnet".into(), "vit".into()],
            pipeline: None,
            input: Some(pb::input_item::Input::ImageUrl(pb::Url {
                main: "http://x/cat.jpg".into(),
                fallback: Some("file:///cat.jpg".into()),
            })),
        };
        let sub = submission_from_proto(item).expect("valid submission");
        assert_eq!(sub.models, vec!["resnet".to_string(), "vit".to_string()]);
        match sub.input {
            dom::Input::Image(u) => {
                assert_eq!(u.main, "http://x/cat.jpg");
                assert_eq!(u.fallback.as_deref(), Some("file:///cat.jpg"));
            }
            _ => panic!("expected an image input"),
        }
    }

    #[test]
    fn each_modality_maps_through() {
        use pb::input_item::Input;
        let item = pb::InputItem {
            models: vec!["m".into()],
            pipeline: None,
            input: Some(Input::VideoUrl(pb_url("v"))),
        };
        assert!(matches!(
            submission_from_proto(item).unwrap().input,
            dom::Input::Video(_)
        ));

        let item = pb::InputItem {
            models: vec!["m".into()],
            pipeline: None,
            input: Some(Input::Text("hello".into())),
        };
        match submission_from_proto(item).unwrap().input {
            dom::Input::Text(t) => assert_eq!(t, "hello"),
            _ => panic!("expected text"),
        }

        let item = pb::InputItem {
            models: vec!["m".into()],
            pipeline: None,
            input: Some(Input::AudioUrl(pb_url("a"))),
        };
        assert!(matches!(
            submission_from_proto(item).unwrap().input,
            dom::Input::Audio(_)
        ));
    }

    #[test]
    fn submission_without_input_is_rejected() {
        let item = pb::InputItem {
            models: vec!["m".into()],
            pipeline: None,
            input: None,
        };
        assert!(submission_from_proto(item).is_err());
    }

    fn item(
        state: dom::ItemState,
        results: std::collections::BTreeMap<String, dom::ModelResult>,
        error: Option<dom::TaskError>,
        pipeline: Option<String>,
    ) -> dom::Item {
        dom::Item {
            input: dom::Input::Image(dom::Url {
                main: "u".into(),
                fallback: None,
            }),
            models: vec!["m".into()],
            pipeline,
            state,
            results,
            error,
            retries: 0,
        }
    }

    #[test]
    fn completed_task_maps_to_models_with_classification() {
        let classification = dom::Classification {
            predictions: vec![dom::Prediction {
                label: 7,
                score: 0.9,
            }],
        };
        let mut results = std::collections::BTreeMap::new();
        results.insert(
            "m".to_string(),
            dom::ModelResult::done(dom::ModelOutput::Classification(classification)),
        );
        let task = dom::Task {
            id: "t1".into(),
            state: dom::TaskState::Completed,
            items: vec![item(
                dom::ItemState::Completed,
                results,
                None,
                Some("p".into()),
            )],
        };

        let pt = task_to_proto(task);
        assert_eq!(pt.id, "t1");
        let models = match pt.result {
            Some(pb::task::Result::Models(m)) => m,
            other => panic!("expected a Models result, got {other:?}"),
        };
        assert_eq!(models.pipeline.as_deref(), Some("p"));
        let model = models.models.get("m").expect("model result present");
        match &model.result {
            Some(pb::model::Result::Classification(c)) => {
                assert_eq!(c.predictions.len(), 1);
                assert_eq!(c.predictions[0].label, 7);
                assert!((c.predictions[0].score - 0.9).abs() < 1e-6);
            }
            other => panic!("expected a classification, got {other:?}"),
        }
    }

    #[test]
    fn task_error_maps_to_error_result() {
        let task = dom::Task {
            id: "t2".into(),
            state: dom::TaskState::Failed,
            items: vec![item(
                dom::ItemState::Failed,
                std::collections::BTreeMap::new(),
                Some(dom::TaskError::fetch("fetch failed")),
                None,
            )],
        };
        let err = match task_to_proto(task).result {
            Some(pb::task::Result::Error(e)) => e,
            other => panic!("expected an Error result, got {other:?}"),
        };
        assert_eq!(err.kind, pb::ErrorType::Fetch as i32);
        assert_eq!(err.message, "fetch failed");
    }

    #[test]
    fn live_task_maps_to_state() {
        let task = dom::Task {
            id: "t3".into(),
            state: dom::TaskState::Processing,
            items: vec![item(
                dom::ItemState::Processing,
                std::collections::BTreeMap::new(),
                None,
                None,
            )],
        };
        match task_to_proto(task).result {
            Some(pb::task::Result::State(s)) => assert_eq!(s, pb::TaskState::Processing as i32),
            other => panic!("expected a State result, got {other:?}"),
        }
    }
}
