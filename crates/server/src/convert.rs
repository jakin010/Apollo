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
    Ok(Submission {
        input,
        models: item.models,
    })
}

fn url_from_proto(u: pb::Url) -> dom::Url {
    dom::Url {
        main: u.main,
        fallback: u.fallback,
    }
}

// ------------------------------ domain -> proto ------------------------------

/// Full task tree, for `GetTask` responses and webhook payloads.
pub(crate) fn task_to_proto(t: dom::Task) -> pb::Task {
    pb::Task {
        id: t.id,
        state: task_state(t.state) as i32,
        items: t.items.into_iter().map(item_to_proto).collect(),
    }
}

fn item_to_proto(it: dom::Item) -> pb::ItemResult {
    let results: HashMap<String, pb::ModelResult> = it
        .results
        .into_iter()
        .map(|(label, r)| (label, model_to_proto(r)))
        .collect();
    pb::ItemResult {
        state: item_state(it.state) as i32,
        results,
        error: it.error.unwrap_or_default(),
    }
}

fn model_to_proto(m: dom::ModelResult) -> pb::ModelResult {
    pb::ModelResult {
        state: model_state(m.state) as i32,
        output: m.output.map(output_to_proto),
        error: m.error.unwrap_or_default(),
    }
}

fn output_to_proto(o: dom::ModelOutput) -> pb::model_result::Output {
    use pb::model_result::Output;
    match o {
        dom::ModelOutput::Classification(c) => Output::Classification(classification_to_proto(c)),
        dom::ModelOutput::FrameScan(f) => Output::FrameScan(frame_scan_to_proto(f)),
    }
}

fn classification_to_proto(c: dom::Classification) -> pb::Classification {
    pb::Classification {
        predictions: c
            .predictions
            .into_iter()
            .map(|p| pb::Prediction {
                label: p.label,
                score: p.score,
            })
            .collect(),
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

fn task_state(s: dom::TaskState) -> pb::TaskState {
    match s {
        dom::TaskState::Queued => pb::TaskState::Queued,
        dom::TaskState::Processing => pb::TaskState::Processing,
        dom::TaskState::Completed => pb::TaskState::Completed,
        dom::TaskState::Failed => pb::TaskState::Failed,
        dom::TaskState::Cancelled => pb::TaskState::Cancelled,
    }
}

fn item_state(s: dom::ItemState) -> pb::ItemState {
    match s {
        dom::ItemState::Queued => pb::ItemState::Queued,
        dom::ItemState::Processing => pb::ItemState::Processing,
        dom::ItemState::Completed => pb::ItemState::Completed,
        dom::ItemState::Retrying => pb::ItemState::Retrying,
        dom::ItemState::Failed => pb::ItemState::Failed,
        dom::ItemState::Cancelled => pb::ItemState::Cancelled,
    }
}

fn model_state(s: dom::ModelState) -> pb::ModelState {
    match s {
        dom::ModelState::Queued => pb::ModelState::Queued,
        dom::ModelState::Processing => pb::ModelState::Processing,
        dom::ModelState::Done => pb::ModelState::Done,
        dom::ModelState::Failed => pb::ModelState::Failed,
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
            input: Some(Input::VideoUrl(pb_url("v"))),
        };
        assert!(matches!(
            submission_from_proto(item).unwrap().input,
            dom::Input::Video(_)
        ));

        let item = pb::InputItem {
            models: vec!["m".into()],
            input: Some(Input::Text("hello".into())),
        };
        match submission_from_proto(item).unwrap().input {
            dom::Input::Text(t) => assert_eq!(t, "hello"),
            _ => panic!("expected text"),
        }

        let item = pb::InputItem {
            models: vec!["m".into()],
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
            input: None,
        };
        assert!(submission_from_proto(item).is_err());
    }

    #[test]
    fn task_maps_states_results_and_classification() {
        let classification = dom::Classification {
            predictions: vec![dom::Prediction {
                label: "cat".into(),
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
            items: vec![dom::Item {
                input: dom::Input::Image(dom::Url {
                    main: "u".into(),
                    fallback: None,
                }),
                models: vec!["m".into()],
                state: dom::ItemState::Completed,
                results,
                error: None,
                retries: 0,
            }],
        };

        let pt = task_to_proto(task);
        assert_eq!(pt.id, "t1");
        assert_eq!(pt.state, pb::TaskState::Completed as i32);
        assert_eq!(pt.items.len(), 1);

        let item = &pt.items[0];
        assert_eq!(item.state, pb::ItemState::Completed as i32);
        assert_eq!(item.error, "");

        let mr = item.results.get("m").expect("model result present");
        assert_eq!(mr.state, pb::ModelState::Done as i32);
        match &mr.output {
            Some(pb::model_result::Output::Classification(c)) => {
                assert_eq!(c.predictions.len(), 1);
                assert_eq!(c.predictions[0].label, "cat");
                assert!((c.predictions[0].score - 0.9).abs() < 1e-6);
            }
            _ => panic!("expected a classification output"),
        }
    }

    #[test]
    fn item_level_error_becomes_string() {
        let task = dom::Task {
            id: "t2".into(),
            state: dom::TaskState::Completed,
            items: vec![dom::Item {
                input: dom::Input::Image(dom::Url {
                    main: "u".into(),
                    fallback: None,
                }),
                models: vec!["m".into()],
                state: dom::ItemState::Failed,
                results: std::collections::BTreeMap::new(),
                error: Some("fetch failed".into()),
                retries: 0,
            }],
        };
        let pt = task_to_proto(task);
        assert_eq!(pt.items[0].state, pb::ItemState::Failed as i32);
        assert_eq!(pt.items[0].error, "fetch failed");
    }
}
