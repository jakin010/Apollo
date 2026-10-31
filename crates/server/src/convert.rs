//! Conversions between proto messages and `apollo-domain` / engine types.
//!
//! Free functions rather than `From` impls: both sides are foreign to this crate,
//! so the orphan rule rules out trait impls. Domain -> proto powers every task
//! response and webhook delivery; proto -> domain turns a request into an engine
//! [`Submission`].

use std::collections::BTreeMap;

use apollo_domain as dom;
use apollo_engine::{Catalog, Run, TaskCursor, TaskQuery};
use apollo_proto as pb;

/// Metadata limits (see `ClassifyRequest.metadata`).
const MAX_METADATA_ENTRIES: usize = 16;
const MAX_METADATA_KEY: usize = 64;
const MAX_METADATA_VALUE: usize = 512;
/// Longest accepted `request_id`, in bytes.
const MAX_REQUEST_ID: usize = 128;
/// Longest accepted `wait_seconds`.
pub(crate) const MAX_WAIT_SECONDS: u32 = 300;
/// `ListTasks` page sizes.
const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 500;

// ------------------------------ proto -> domain ------------------------------

/// Where a request's content comes from.
#[derive(Debug, PartialEq)]
pub(crate) enum Source {
    /// Fetched by the engine.
    Input(dom::Input),
    /// Sent inline; staged to an upload file before submitting.
    Data {
        kind: dom::MediaKind,
        bytes: Vec<u8>,
    },
}

/// A request's `Media`.
/// A `ClassifyRequest`'s input: `media`, or inline `text`.
pub(crate) fn input_from_proto(
    input: Option<pb::classify_request::Input>,
) -> Result<Source, String> {
    use pb::classify_request::Input;
    match input {
        Some(Input::Media(m)) => media_from_proto(Some(m)),
        Some(Input::Text(t)) if t.is_empty() => Err("`text` is empty".into()),
        Some(Input::Text(t)) => Ok(Source::Input(dom::Input::Text(t))),
        None => Err("request has no `media` or `text`".into()),
    }
}

pub(crate) fn media_from_proto(media: Option<pb::Media>) -> Result<Source, String> {
    let media = media.ok_or("request has no `media`")?;
    let kind = media_kind_from_proto(media.kind)?;
    match media.source {
        Some(pb::media::Source::Url(_)) if kind == dom::MediaKind::Text => Err(
            "text cannot come from a `url`; send it as `text`, as `media.data`, or with ClassifyStream"
                .into(),
        ),
        Some(pb::media::Source::Url(url)) if !url.is_empty() => {
            let url = dom::Url {
                main: url,
                fallback: (!media.fallback_url.is_empty()).then_some(media.fallback_url),
            };
            Ok(Source::Input(match kind {
                dom::MediaKind::Image => dom::Input::Image(url),
                dom::MediaKind::Video => dom::Input::Video(url),
                dom::MediaKind::Audio => dom::Input::Audio(url),
                dom::MediaKind::Text => unreachable!("rejected above"),
            }))
        }
        Some(pb::media::Source::Data(bytes)) if !bytes.is_empty() => {
            if !media.fallback_url.is_empty() {
                return Err("`media.fallback_url` only applies to a `url`".into());
            }
            Ok(Source::Data { kind, bytes })
        }
        _ => Err("`media` needs a `url` or `data`".into()),
    }
}

pub(crate) fn media_kind_from_proto(kind: i32) -> Result<dom::MediaKind, String> {
    match pb::MediaKind::try_from(kind) {
        Ok(pb::MediaKind::Image) => Ok(dom::MediaKind::Image),
        Ok(pb::MediaKind::Video) => Ok(dom::MediaKind::Video),
        Ok(pb::MediaKind::Audio) => Ok(dom::MediaKind::Audio),
        Ok(pb::MediaKind::Text) => Ok(dom::MediaKind::Text),
        Ok(pb::MediaKind::Unspecified) => Err("`kind` must be IMAGE, VIDEO, AUDIO or TEXT".into()),
        Err(_) => Err(format!("unknown media kind {kind}")),
    }
}

/// A request's `run` oneof, given as its two arms.
pub(crate) fn run_from_proto(
    models: Option<pb::ModelList>,
    pipeline: Option<String>,
) -> Result<Run, String> {
    match (models, pipeline) {
        (Some(list), _) if !list.names.is_empty() => Ok(Run::Models(list.names)),
        (_, Some(name)) if !name.is_empty() => Ok(Run::Pipeline(name)),
        _ => Err("set `models` (a non-empty list) or a `pipeline`".into()),
    }
}

pub(crate) fn classify_run(run: Option<pb::classify_request::Run>) -> Result<Run, String> {
    use pb::classify_request::Run as R;
    match run {
        Some(R::Models(m)) => run_from_proto(Some(m), None),
        Some(R::Pipeline(p)) => run_from_proto(None, Some(p)),
        None => run_from_proto(None, None),
    }
}

pub(crate) fn stream_run(run: Option<pb::classify_stream_init::Run>) -> Result<Run, String> {
    use pb::classify_stream_init::Run as R;
    match run {
        Some(R::Models(m)) => run_from_proto(Some(m), None),
        Some(R::Pipeline(p)) => run_from_proto(None, Some(p)),
        None => run_from_proto(None, None),
    }
}

/// Check the client's metadata against the documented limits.
pub(crate) fn check_metadata(metadata: &BTreeMap<String, String>) -> Result<(), String> {
    if metadata.len() > MAX_METADATA_ENTRIES {
        return Err(format!(
            "`metadata` has {} entries; at most {MAX_METADATA_ENTRIES} are allowed",
            metadata.len()
        ));
    }
    for (key, value) in metadata {
        if key.is_empty() || key.len() > MAX_METADATA_KEY {
            return Err(format!(
                "`metadata` keys must be 1 to {MAX_METADATA_KEY} bytes (got {:?})",
                key
            ));
        }
        if value.len() > MAX_METADATA_VALUE {
            return Err(format!(
                "`metadata` value for {key:?} is over {MAX_METADATA_VALUE} bytes"
            ));
        }
    }
    Ok(())
}

/// The request's idempotency key: `None` when empty.
pub(crate) fn request_id(raw: String) -> Result<Option<String>, String> {
    if raw.len() > MAX_REQUEST_ID {
        return Err(format!("`request_id` is over {MAX_REQUEST_ID} bytes"));
    }
    Ok((!raw.is_empty()).then_some(raw))
}

pub(crate) fn wait_seconds(secs: u32) -> Result<u32, String> {
    if secs > MAX_WAIT_SECONDS {
        return Err(format!(
            "`wait_seconds` is {secs}; at most {MAX_WAIT_SECONDS} is allowed"
        ));
    }
    Ok(secs)
}

/// Whether to include per-sample results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum View {
    Summary,
    Full,
}

pub(crate) fn view_from_proto(view: i32) -> Result<View, String> {
    match pb::ResultView::try_from(view) {
        Ok(pb::ResultView::Unspecified | pb::ResultView::Summary) => Ok(View::Summary),
        Ok(pb::ResultView::Full) => Ok(View::Full),
        Err(_) => Err(format!("unknown result view {view}")),
    }
}

/// A `ListTasks` request as a storage query. `owner` is the scope: `None` for
/// every task (admins, or authentication disabled), else the caller's own.
pub(crate) fn task_query(
    req: pb::ListTasksRequest,
    owner: Option<Option<String>>,
) -> Result<TaskQuery, String> {
    let states = req
        .states
        .iter()
        .map(|&s| task_state_from_proto(s))
        .collect::<Result<Vec<_>, _>>()?;
    let limit = match req.page_size as usize {
        0 => DEFAULT_PAGE_SIZE,
        n => n.min(MAX_PAGE_SIZE),
    };
    let after = if req.page_token.is_empty() {
        None
    } else {
        Some(parse_page_token(&req.page_token).ok_or("invalid `page_token`")?)
    };
    Ok(TaskQuery {
        owner,
        states,
        created_after: req.created_after.map(|t| t.seconds),
        created_before: req.created_before.map(|t| t.seconds),
        limit,
        after,
    })
}

fn task_state_from_proto(state: i32) -> Result<dom::TaskState, String> {
    match pb::TaskState::try_from(state) {
        Ok(pb::TaskState::Queued) => Ok(dom::TaskState::Queued),
        Ok(pb::TaskState::Processing) => Ok(dom::TaskState::Processing),
        Ok(pb::TaskState::Completed) => Ok(dom::TaskState::Completed),
        Ok(pb::TaskState::Failed) => Ok(dom::TaskState::Failed),
        Ok(pb::TaskState::Cancelled) => Ok(dom::TaskState::Cancelled),
        Ok(pb::TaskState::Unspecified) | Err(_) => Err(format!("unknown task state {state}")),
    }
}

/// `next_page_token`: where the page ended.
pub(crate) fn page_token(cursor: &TaskCursor) -> String {
    format!("{}:{}", cursor.created_at, cursor.id)
}

fn parse_page_token(token: &str) -> Option<TaskCursor> {
    let (created_at, id) = token.split_once(':')?;
    Some(TaskCursor {
        created_at: created_at.parse().ok()?,
        id: id.to_string(),
    })
}

// ------------------------------ domain -> proto ------------------------------

/// A task as returned by every RPC and webhook. A domain task carries one input
/// (its first item); each of its models gets a result, in run order.
pub(crate) fn task_to_proto(t: dom::Task, view: View) -> pb::Task {
    let state = task_state_to_proto(t.state);
    let error = apollo_engine::task_error(&t).cloned().map(error_to_proto);
    let cancelled = t.state == dom::TaskState::Cancelled;
    let dom::Task {
        id,
        items,
        metadata,
        request_id,
        created_at,
        updated_at,
        finished_at,
        ..
    } = t;
    let item = items.into_iter().next();
    let (results, pipeline, progress, retries) = match item {
        Some(item) => {
            let total = item.models.len() as u32;
            let mut stored = item.results;
            let results: Vec<pb::ModelResult> = item
                .models
                .iter()
                .map(|model| {
                    let result = stored
                        .remove(model)
                        .unwrap_or_else(dom::ModelResult::queued);
                    model_to_proto(model.clone(), result, cancelled, view)
                })
                .collect();
            let finished = results
                .iter()
                .filter(|r| {
                    [
                        pb::ModelState::Completed,
                        pb::ModelState::Failed,
                        pb::ModelState::Skipped,
                    ]
                    .contains(&r.state())
                })
                .count() as u32;
            let progress = pb::Progress {
                models_total: total,
                models_finished: finished,
            };
            (
                results,
                item.pipeline.unwrap_or_default(),
                Some(progress),
                item.retries,
            )
        }
        None => (Vec::new(), String::new(), None, 0),
    };
    pb::Task {
        id,
        state: state as i32,
        error,
        results,
        pipeline,
        metadata,
        request_id: request_id.unwrap_or_default(),
        created_at: timestamp(created_at),
        updated_at: timestamp(updated_at),
        finished_at: finished_at.and_then(timestamp),
        progress,
        retries,
    }
}

fn timestamp(unix_secs: i64) -> Option<pb::Timestamp> {
    (unix_secs > 0).then_some(pb::Timestamp {
        seconds: unix_secs,
        nanos: 0,
    })
}

fn task_state_to_proto(s: dom::TaskState) -> pb::TaskState {
    match s {
        dom::TaskState::Queued => pb::TaskState::Queued,
        dom::TaskState::Processing => pb::TaskState::Processing,
        dom::TaskState::Completed => pb::TaskState::Completed,
        dom::TaskState::Failed => pb::TaskState::Failed,
        dom::TaskState::Cancelled => pb::TaskState::Cancelled,
    }
}

/// One model's result. In a cancelled task, a model that had not finished is
/// reported `CANCELLED`.
fn model_to_proto(
    model: String,
    m: dom::ModelResult,
    task_cancelled: bool,
    view: View,
) -> pb::ModelResult {
    use pb::model_result::Output;
    let state = match m.state {
        dom::ModelState::Queued | dom::ModelState::Processing if task_cancelled => {
            pb::ModelState::Cancelled
        }
        dom::ModelState::Queued => pb::ModelState::Queued,
        dom::ModelState::Processing => pb::ModelState::Processing,
        dom::ModelState::Done => pb::ModelState::Completed,
        dom::ModelState::Failed => pb::ModelState::Failed,
        dom::ModelState::Skipped => pb::ModelState::Skipped,
    };
    let output = m.output.map(|output| match output {
        dom::ModelOutput::Classification(c) => Output::Classification(classification_to_proto(c)),
        dom::ModelOutput::Scan(s) => Output::Scan(scan_to_proto(s, view)),
        dom::ModelOutput::Text(t) => Output::Text(text_to_proto(t)),
    });
    pb::ModelResult {
        model,
        state: state as i32,
        error: m.error.map(error_to_proto),
        output,
    }
}

fn classification_to_proto(c: dom::Classification) -> pb::Classification {
    pb::Classification {
        predictions: c
            .predictions
            .into_iter()
            .map(|p| pb::Prediction {
                label: p.label,
                name: p.name,
                score: p.score,
            })
            .collect(),
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

fn scan_to_proto(s: dom::Scan, view: View) -> pb::Scan {
    let sample_count = s.samples.len() as u32;
    let samples = match view {
        View::Full => s.samples.into_iter().map(sample_to_proto).collect(),
        View::Summary => Vec::new(),
    };
    pb::Scan {
        aggregated: Some(classification_to_proto(s.aggregated)),
        segments: s.segments.into_iter().map(segment_to_proto).collect(),
        samples,
        sample_count,
    }
}

fn text_to_proto(t: dom::TextOutput) -> pb::TextOutput {
    pb::TextOutput {
        text: t.text,
        language: t.language,
        parts: t
            .parts
            .into_iter()
            .map(|p| pb::TimedText {
                start: p.start,
                end: p.end,
                text: p.text,
            })
            .collect(),
    }
}

fn sample_to_proto(s: dom::Sample) -> pb::Sample {
    pb::Sample {
        index: s.index,
        start: s.start,
        end: s.end(),
        classification: Some(classification_to_proto(s.classification)),
    }
}

fn segment_to_proto(s: dom::Segment) -> pb::Segment {
    pb::Segment {
        label: s.label,
        name: s.name,
        start: s.start,
        end: s.end,
        peak: s.peak,
        peak_at: s.peak_at,
        samples: s.samples,
        bound_start: s.bound_start,
        bound_end: s.bound_end,
    }
}

/// `ListModels`.
pub(crate) fn catalog_to_proto(catalog: Catalog) -> pb::ListModelsResponse {
    let trigger = |t: apollo_engine::Trigger| pb::Trigger {
        labels: t.labels,
        threshold: t.threshold,
    };
    let models = catalog
        .models
        .into_iter()
        .map(|m| pb::ModelInfo {
            name: m.name,
            architecture: m.architecture.as_str().to_string(),
            repo: m.repo,
            accepts: m
                .accepts
                .into_iter()
                .map(|k| media_kind_to_proto(k) as i32)
                .collect(),
            enabled: m.enabled,
            loaded: m.loaded,
            labels: m
                .labels
                .into_iter()
                .map(|l| pb::Label {
                    id: l.id,
                    name: l.name,
                })
                .collect(),
            trigger: m.trigger.map(trigger),
            video_strategy: m.video_strategy.unwrap_or_default(),
            output: match m.output {
                dom::OutputKind::Classification => pb::ModelOutputKind::Classification,
                dom::OutputKind::Text => pb::ModelOutputKind::Text,
            } as i32,
        })
        .collect();
    let pipelines = catalog
        .pipelines
        .into_iter()
        .map(|p| pb::PipelineInfo {
            name: p.name,
            steps: p
                .steps
                .into_iter()
                .map(|s| pb::PipelineStepInfo {
                    models: s.models,
                    stop_if: s.stop_if.map(|g| pb::StopIf {
                        model: g.model,
                        trigger: Some(trigger(g.trigger)),
                    }),
                    input: s.input.unwrap_or_default(),
                })
                .collect(),
        })
        .collect();
    pb::ListModelsResponse { models, pipelines }
}

fn media_kind_to_proto(k: dom::MediaKind) -> pb::MediaKind {
    match k {
        dom::MediaKind::Image => pb::MediaKind::Image,
        dom::MediaKind::Video => pb::MediaKind::Video,
        dom::MediaKind::Audio => pb::MediaKind::Audio,
        dom::MediaKind::Text => pb::MediaKind::Text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn media(kind: pb::MediaKind, source: pb::media::Source, fallback: &str) -> Option<pb::Media> {
        Some(pb::Media {
            kind: kind as i32,
            source: Some(source),
            fallback_url: fallback.into(),
        })
    }

    #[test]
    fn media_urls_map_to_inputs_by_kind() {
        use pb::media::Source as S;
        let image = media_from_proto(media(
            pb::MediaKind::Image,
            S::Url("http://x/cat.jpg".into()),
            "file:///cat.jpg",
        ))
        .unwrap();
        assert_eq!(
            image,
            Source::Input(dom::Input::Image(dom::Url {
                main: "http://x/cat.jpg".into(),
                fallback: Some("file:///cat.jpg".into()),
            }))
        );
        let video = media_from_proto(media(pb::MediaKind::Video, S::Url("v".into()), "")).unwrap();
        assert!(matches!(
            video,
            Source::Input(dom::Input::Video(dom::Url { fallback: None, .. }))
        ));
        let data = media_from_proto(media(pb::MediaKind::Video, S::Data(vec![1, 2]), "")).unwrap();
        assert_eq!(
            data,
            Source::Data {
                kind: dom::MediaKind::Video,
                bytes: vec![1, 2]
            }
        );
    }

    #[test]
    fn text_comes_inline_or_as_data_but_not_from_a_url() {
        use pb::classify_request::Input;
        use pb::media::Source as S;
        assert_eq!(
            input_from_proto(Some(Input::Text("hello".into()))),
            Ok(Source::Input(dom::Input::Text("hello".into())))
        );
        assert!(input_from_proto(Some(Input::Text(String::new()))).is_err());
        assert!(input_from_proto(None).is_err());
        let data =
            media_from_proto(media(pb::MediaKind::Text, S::Data(b"hi".to_vec()), "")).unwrap();
        assert_eq!(
            data,
            Source::Data {
                kind: dom::MediaKind::Text,
                bytes: b"hi".to_vec()
            }
        );
        let err = media_from_proto(media(
            pb::MediaKind::Text,
            S::Url("https://x/a.txt".into()),
            "",
        ))
        .unwrap_err();
        assert!(err.contains("cannot come from a `url`"), "{err}");
    }

    #[test]
    fn incomplete_media_is_rejected() {
        use pb::media::Source as S;
        assert!(media_from_proto(None).is_err());
        assert!(
            media_from_proto(media(pb::MediaKind::Unspecified, S::Url("u".into()), "")).is_err()
        );
        assert!(media_from_proto(media(pb::MediaKind::Image, S::Url(String::new()), "")).is_err());
        assert!(media_from_proto(media(pb::MediaKind::Image, S::Data(Vec::new()), "")).is_err());
        assert!(media_from_proto(media(pb::MediaKind::Image, S::Data(vec![1]), "x")).is_err());
    }

    #[test]
    fn run_needs_models_or_a_pipeline() {
        use pb::classify_request::Run as R;
        assert_eq!(
            classify_run(Some(R::Models(pb::ModelList {
                names: vec!["a".into(), "b".into()]
            }))),
            Ok(Run::Models(vec!["a".into(), "b".into()]))
        );
        assert_eq!(
            classify_run(Some(R::Pipeline("p".into()))),
            Ok(Run::Pipeline("p".into()))
        );
        assert!(classify_run(None).is_err());
        assert!(classify_run(Some(R::Models(pb::ModelList::default()))).is_err());
        assert!(classify_run(Some(R::Pipeline(String::new()))).is_err());
    }

    #[test]
    fn metadata_limits_are_enforced() {
        let ok: BTreeMap<String, String> = [("ref".to_string(), "42".to_string())].into();
        assert!(check_metadata(&ok).is_ok());
        let many: BTreeMap<String, String> =
            (0..17).map(|i| (format!("k{i}"), String::new())).collect();
        assert!(check_metadata(&many).is_err());
        let long_key: BTreeMap<String, String> = [("k".repeat(65), String::new())].into();
        assert!(check_metadata(&long_key).is_err());
        let long_value: BTreeMap<String, String> = [("k".into(), "v".repeat(513))].into();
        assert!(check_metadata(&long_value).is_err());
        assert!(wait_seconds(300).is_ok() && wait_seconds(301).is_err());
        assert_eq!(request_id(String::new()), Ok(None));
        assert!(request_id("r".repeat(129)).is_err());
    }

    #[test]
    fn page_tokens_round_trip() {
        let cursor = TaskCursor {
            created_at: 1_760_000_000,
            id: "6d1c-..".into(),
        };
        let query = task_query(
            pb::ListTasksRequest {
                page_token: page_token(&cursor),
                page_size: 9999,
                states: vec![pb::TaskState::Failed as i32],
                ..Default::default()
            },
            Some(Some("alice".into())),
        )
        .unwrap();
        assert_eq!(query.after, Some(cursor));
        assert_eq!(query.limit, MAX_PAGE_SIZE);
        assert_eq!(query.states, [dom::TaskState::Failed]);
        assert_eq!(query.owner, Some(Some("alice".into())));
        let bad = pb::ListTasksRequest {
            page_token: "nope".into(),
            ..Default::default()
        };
        assert!(task_query(bad, None).is_err());
    }

    fn scan() -> dom::ModelOutput {
        let frame = |index, t, score| {
            dom::Sample::frame(
                index,
                t,
                dom::Classification {
                    predictions: vec![dom::Prediction::new(1, "nsfw", score)],
                },
            )
        };
        dom::ModelOutput::Scan(dom::Scan {
            aggregated: dom::Classification {
                predictions: vec![dom::Prediction::new(1, "nsfw", 0.9)],
            },
            segments: vec![dom::Segment {
                label: 1,
                name: "nsfw".into(),
                start: 2.0,
                end: 2.0,
                peak: 0.9,
                peak_at: 2.0,
                samples: 1,
                bound_start: 1.0,
                bound_end: 3.0,
            }],
            samples: vec![frame(0, 1.0, 0.1), frame(1, 2.0, 0.9), frame(2, 3.0, 0.2)],
        })
    }

    fn task(state: dom::TaskState, results: Vec<(&str, dom::ModelResult)>) -> dom::Task {
        let mut item = dom::Item::new(
            dom::Input::Image(dom::Url {
                main: "u".into(),
                fallback: None,
            }),
            vec!["nsfw".into(), "general".into(), "extra".into()],
            Some("moderation".into()),
        );
        item.results = results
            .into_iter()
            .map(|(m, r)| (m.to_string(), r))
            .collect();
        let mut task = dom::Task::new("t1", vec![item], None);
        task.state = state;
        task.metadata = [("ref".to_string(), "42".to_string())].into();
        task.request_id = Some("r1".into());
        task.created_at = 100;
        task.updated_at = 105;
        task
    }

    #[test]
    fn results_come_in_run_order_with_explicit_states() {
        let t = task(
            dom::TaskState::Processing,
            vec![
                ("general", dom::ModelResult::processing()),
                ("nsfw", dom::ModelResult::done(scan())),
            ],
        );
        let pt = task_to_proto(t, View::Summary);
        assert_eq!(pt.state(), pb::TaskState::Processing);
        assert_eq!(pt.pipeline, "moderation");
        assert_eq!(pt.metadata["ref"], "42");
        assert_eq!(pt.request_id, "r1");
        assert_eq!(pt.created_at.unwrap().seconds, 100);
        assert!(pt.finished_at.is_none());
        let order: Vec<(&str, pb::ModelState)> = pt
            .results
            .iter()
            .map(|r| (r.model.as_str(), r.state()))
            .collect();
        assert_eq!(
            order,
            [
                ("nsfw", pb::ModelState::Completed),
                ("general", pb::ModelState::Processing),
                ("extra", pb::ModelState::Queued),
            ]
        );
        let progress = pt.progress.unwrap();
        assert_eq!((progress.models_total, progress.models_finished), (3, 1));

        // SUMMARY leaves the samples out but keeps their count and the segments.
        let Some(pb::model_result::Output::Scan(s)) = &pt.results[0].output else {
            panic!("expected a scan");
        };
        assert!(s.samples.is_empty());
        assert_eq!(s.sample_count, 3);
        assert_eq!(s.segments[0].name, "nsfw");
        assert_eq!(s.aggregated.as_ref().unwrap().predictions[0].name, "nsfw");
    }

    #[test]
    fn the_full_view_includes_samples() {
        let t = task(
            dom::TaskState::Completed,
            vec![("nsfw", dom::ModelResult::done(scan()))],
        );
        let pt = task_to_proto(t, View::Full);
        let Some(pb::model_result::Output::Scan(s)) = &pt.results[0].output else {
            panic!("expected a scan");
        };
        let times: Vec<(f64, f64)> = s.samples.iter().map(|x| (x.start, x.end)).collect();
        assert_eq!(times, [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]);
    }

    #[test]
    fn a_failed_task_carries_its_error() {
        let mut t = task(dom::TaskState::Failed, vec![]);
        t.items[0].state = dom::ItemState::Failed;
        t.items[0].error = Some(dom::TaskError::fetch("unreachable"));
        t.finished_at = Some(110);
        let pt = task_to_proto(t, View::Summary);
        assert_eq!(pt.state(), pb::TaskState::Failed);
        let err = pt.error.unwrap();
        assert_eq!(
            (err.kind(), err.message.as_str()),
            (pb::ErrorType::Fetch, "unreachable")
        );
        assert_eq!(pt.finished_at.unwrap().seconds, 110);
    }

    #[test]
    fn a_retrying_task_is_processing_without_an_error() {
        let mut t = task(dom::TaskState::Processing, vec![]);
        t.items[0].state = dom::ItemState::Retrying;
        t.items[0].error = Some(dom::TaskError::fetch("connection refused"));
        t.items[0].retries = 2;
        let pt = task_to_proto(t, View::Summary);
        assert_eq!(pt.state(), pb::TaskState::Processing);
        assert!(pt.error.is_none());
        assert_eq!(pt.retries, 2);
    }

    #[test]
    fn unfinished_models_of_a_cancelled_task_are_cancelled() {
        let t = task(
            dom::TaskState::Cancelled,
            vec![
                ("nsfw", dom::ModelResult::done(scan())),
                ("general", dom::ModelResult::processing()),
            ],
        );
        let states: Vec<pb::ModelState> = task_to_proto(t, View::Summary)
            .results
            .iter()
            .map(|r| r.state())
            .collect();
        assert_eq!(
            states,
            [
                pb::ModelState::Completed,
                pb::ModelState::Cancelled,
                pb::ModelState::Cancelled
            ]
        );
    }
}
