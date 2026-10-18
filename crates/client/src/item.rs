//! Ergonomic constructors for [`InputItem`](crate::InputItem).
//!
//! Each takes the content reference plus the model labels to run. Models accept
//! anything `Into<String>`, so string literals work:
//! `item::image("cat.jpg", ["resnet", "vit"])`.

use apollo_proto::input_item::Input;
use apollo_proto::{InputItem, Url};

fn models_of(models: impl IntoIterator<Item = impl Into<String>>) -> Vec<String> {
    models.into_iter().map(Into::into).collect()
}

fn url(main: impl Into<String>, fallback: Option<String>) -> Url {
    Url {
        main: main.into(),
        fallback,
    }
}

/// An image fetched from `url`, classified by `models`.
pub fn image(
    url: impl Into<String>,
    models: impl IntoIterator<Item = impl Into<String>>,
) -> InputItem {
    InputItem {
        models: models_of(models),
        pipeline: None,
        input: Some(Input::ImageUrl(self::url(url, None))),
    }
}

/// An image with a primary URL and a fallback tried only if `main` fails.
pub fn image_with_fallback(
    main: impl Into<String>,
    fallback: impl Into<String>,
    models: impl IntoIterator<Item = impl Into<String>>,
) -> InputItem {
    InputItem {
        models: models_of(models),
        pipeline: None,
        input: Some(Input::ImageUrl(url(main, Some(fallback.into())))),
    }
}

/// A video fetched from `url`. Each model (an image classifier with a
/// `video_strategy`) runs over sampled frames.
pub fn video(
    url: impl Into<String>,
    models: impl IntoIterator<Item = impl Into<String>>,
) -> InputItem {
    InputItem {
        models: models_of(models),
        pipeline: None,
        input: Some(Input::VideoUrl(self::url(url, None))),
    }
}

/// A video with a primary URL and a fallback.
pub fn video_with_fallback(
    main: impl Into<String>,
    fallback: impl Into<String>,
    models: impl IntoIterator<Item = impl Into<String>>,
) -> InputItem {
    InputItem {
        models: models_of(models),
        pipeline: None,
        input: Some(Input::VideoUrl(url(main, Some(fallback.into())))),
    }
}

/// An image fetched from `url`, run through the named `[pipelines.<name>]`
/// instead of a parallel set of models.
pub fn image_pipeline(url: impl Into<String>, pipeline: impl Into<String>) -> InputItem {
    InputItem {
        models: Vec::new(),
        pipeline: Some(pipeline.into()),
        input: Some(Input::ImageUrl(self::url(url, None))),
    }
}

/// A video fetched from `url`, run through the named `[pipelines.<name>]`.
pub fn video_pipeline(url: impl Into<String>, pipeline: impl Into<String>) -> InputItem {
    InputItem {
        models: Vec::new(),
        pipeline: Some(pipeline.into()),
        input: Some(Input::VideoUrl(self::url(url, None))),
    }
}

/// Inline text content (future model kinds).
pub fn text(
    content: impl Into<String>,
    models: impl IntoIterator<Item = impl Into<String>>,
) -> InputItem {
    InputItem {
        models: models_of(models),
        pipeline: None,
        input: Some(Input::Text(content.into())),
    }
}

/// Audio fetched from `url` (future model kinds).
pub fn audio(
    url: impl Into<String>,
    models: impl IntoIterator<Item = impl Into<String>>,
) -> InputItem {
    InputItem {
        models: models_of(models),
        pipeline: None,
        input: Some(Input::AudioUrl(self::url(url, None))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apollo_proto::input_item::Input;

    #[test]
    fn image_sets_models_and_url() {
        let it = image("cat.jpg", ["resnet", "vit"]);
        assert_eq!(it.models, vec!["resnet".to_string(), "vit".to_string()]);
        match it.input {
            Some(Input::ImageUrl(u)) => {
                assert_eq!(u.main, "cat.jpg");
                assert!(u.fallback.is_none());
            }
            _ => panic!("expected an image url"),
        }
    }

    #[test]
    fn image_with_fallback_sets_both() {
        match image_with_fallback("a", "b", ["m"]).input {
            Some(Input::ImageUrl(u)) => {
                assert_eq!(u.main, "a");
                assert_eq!(u.fallback.as_deref(), Some("b"));
            }
            _ => panic!("expected an image url"),
        }
    }

    #[test]
    fn video_and_audio_variants() {
        assert!(matches!(video("v", ["m"]).input, Some(Input::VideoUrl(_))));
        assert!(matches!(audio("a", ["m"]).input, Some(Input::AudioUrl(_))));
    }

    #[test]
    fn pipeline_helpers_set_pipeline_not_models() {
        let it = image_pipeline("a.png", "moderation");
        assert!(it.models.is_empty());
        assert_eq!(it.pipeline.as_deref(), Some("moderation"));
        assert!(matches!(it.input, Some(Input::ImageUrl(_))));
        assert!(matches!(
            video_pipeline("v.mp4", "p").input,
            Some(Input::VideoUrl(_))
        ));
    }

    #[test]
    fn text_passes_content_through() {
        match text("hi", ["m"]).input {
            Some(Input::Text(s)) => assert_eq!(s, "hi"),
            _ => panic!("expected text"),
        }
    }
}
