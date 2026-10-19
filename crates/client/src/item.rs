//! Ergonomic constructors for [`ClassifyRequest`](crate::ClassifyRequest).
//!
//! Each takes the content (a URL, or the bytes themselves) plus the models to
//! run, or a pipeline name. Models accept anything `Into<String>`, so string
//! literals work: `item::image("cat.jpg", ["resnet", "vit"])`. Optional
//! request fields are set with [`ClassifyRequestExt`]:
//!
//! ```
//! use apollo_client::item::{self, ClassifyRequestExt};
//! let req = item::video("https://example.com/clip.mp4", ["nsfw"])
//!     .metadata("ref", "upload-42")
//!     .request_id("upload-42")
//!     .wait_seconds(30);
//! ```

use apollo_proto::classify_request::{Input, Run};
use apollo_proto::{ClassifyRequest, Media, MediaKind, ModelList, ResultView, media};

fn models(models: impl IntoIterator<Item = impl Into<String>>) -> Option<Run> {
    Some(Run::Models(ModelList {
        names: models.into_iter().map(Into::into).collect(),
    }))
}

fn request(kind: MediaKind, source: media::Source, run: Option<Run>) -> ClassifyRequest {
    ClassifyRequest {
        input: Some(Input::Media(Media {
            kind: kind as i32,
            source: Some(source),
            fallback_url: String::new(),
        })),
        run,
        ..Default::default()
    }
}

fn url(kind: MediaKind, url: impl Into<String>, run: Option<Run>) -> ClassifyRequest {
    request(kind, media::Source::Url(url.into()), run)
}

/// An image fetched from `url`, classified by `models` concurrently.
pub fn image(
    url: impl Into<String>,
    models: impl IntoIterator<Item = impl Into<String>>,
) -> ClassifyRequest {
    self::url(MediaKind::Image, url, self::models(models))
}

/// A video fetched from `url`. Each model (an image classifier with a
/// `video_strategy`) scans its sampled frames.
pub fn video(
    url: impl Into<String>,
    models: impl IntoIterator<Item = impl Into<String>>,
) -> ClassifyRequest {
    self::url(MediaKind::Video, url, self::models(models))
}

/// An image fetched from `url`, run through the named `[pipelines.<name>]`.
pub fn image_pipeline(url: impl Into<String>, pipeline: impl Into<String>) -> ClassifyRequest {
    self::url(MediaKind::Image, url, Some(Run::Pipeline(pipeline.into())))
}

/// A video fetched from `url`, run through the named `[pipelines.<name>]`.
pub fn video_pipeline(url: impl Into<String>, pipeline: impl Into<String>) -> ClassifyRequest {
    self::url(MediaKind::Video, url, Some(Run::Pipeline(pipeline.into())))
}

/// An image sent inline, classified by `models`. Bounded by the gRPC message
/// size (4 MiB by default); stream larger files with
/// [`Client::classify_stream`](crate::Client::classify_stream).
pub fn image_data(
    data: impl Into<Vec<u8>>,
    models: impl IntoIterator<Item = impl Into<String>>,
) -> ClassifyRequest {
    request(
        MediaKind::Image,
        media::Source::Data(data.into()),
        self::models(models),
    )
}

/// A video sent inline (see [`image_data`] for the size bound).
pub fn video_data(
    data: impl Into<Vec<u8>>,
    models: impl IntoIterator<Item = impl Into<String>>,
) -> ClassifyRequest {
    request(
        MediaKind::Video,
        media::Source::Data(data.into()),
        self::models(models),
    )
}

/// The optional fields of a [`ClassifyRequest`], as chainable setters.
pub trait ClassifyRequestExt: Sized {
    /// Try `url` if the main URL cannot be fetched or decoded.
    fn fallback(self, url: impl Into<String>) -> Self;
    /// Add a metadata entry, echoed back on the task and in webhooks.
    fn metadata(self, key: impl Into<String>, value: impl Into<String>) -> Self;
    /// Idempotency key: resubmitting with it returns the existing task.
    fn request_id(self, id: impl Into<String>) -> Self;
    /// Wait up to `secs` (at most 300) for the task to finish before returning.
    fn wait_seconds(self, secs: u32) -> Self;
    /// Result detail of the returned task (named apart from the generated
    /// `view()` getter).
    fn with_view(self, view: ResultView) -> Self;
}

impl ClassifyRequestExt for ClassifyRequest {
    fn fallback(mut self, url: impl Into<String>) -> Self {
        if let Some(Input::Media(m)) = &mut self.input {
            m.fallback_url = url.into();
        }
        self
    }

    fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    fn request_id(mut self, id: impl Into<String>) -> Self {
        self.request_id = id.into();
        self
    }

    fn wait_seconds(mut self, secs: u32) -> Self {
        self.wait_seconds = secs;
        self
    }

    fn with_view(mut self, view: ResultView) -> Self {
        self.view = view as i32;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn media(req: &ClassifyRequest) -> &Media {
        match &req.input {
            Some(Input::Media(m)) => m,
            None => panic!("no media"),
        }
    }

    #[test]
    fn image_sets_models_and_url() {
        let req = image("cat.jpg", ["resnet", "vit"]).fallback("file:///cat.jpg");
        let m = media(&req);
        assert_eq!(m.kind(), MediaKind::Image);
        assert_eq!(m.source, Some(media::Source::Url("cat.jpg".into())));
        assert_eq!(m.fallback_url, "file:///cat.jpg");
        assert_eq!(
            req.run,
            Some(Run::Models(ModelList {
                names: vec!["resnet".into(), "vit".into()]
            }))
        );
    }

    #[test]
    fn kinds_sources_and_pipelines() {
        assert_eq!(media(&video("v", ["m"])).kind(), MediaKind::Video);
        let data = video_data(vec![1, 2, 3], ["m"]);
        assert_eq!(
            media(&data).source,
            Some(media::Source::Data(vec![1, 2, 3]))
        );
        assert_eq!(media(&image_data(vec![1], ["m"])).kind(), MediaKind::Image);
        let p = image_pipeline("a.png", "moderation");
        assert_eq!(p.run, Some(Run::Pipeline("moderation".into())));
        assert_eq!(
            media(&video_pipeline("v.mp4", "p")).kind(),
            MediaKind::Video
        );
    }

    #[test]
    fn optional_fields_chain() {
        let req = image("a", ["m"])
            .metadata("ref", "1")
            .metadata("batch", "b")
            .request_id("r")
            .wait_seconds(30)
            .with_view(ResultView::Full);
        assert_eq!(req.metadata.len(), 2);
        assert_eq!(req.request_id, "r");
        assert_eq!(req.wait_seconds, 30);
        assert_eq!(req.view(), ResultView::Full);
    }
}
