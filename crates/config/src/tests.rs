//! Unit tests. `cargo test -p apollo-config` verifies parsing, validation, and
//! the edit round-trip against the committed example config.

use toml_edit::DocumentMut;

use crate::schema::{Backend, SamplingKind};
use crate::{edit, load};

const EXAMPLE: &str = include_str!("../../../config.example.toml");

#[test]
fn parses_and_validates_example() {
    let cfg = load::from_str(EXAMPLE).expect("example config should parse");
    cfg.validate().expect("example config should be valid");

    assert!(cfg.has_models());
    assert_eq!(cfg.database.backend, Backend::Sqlite);

    let nsfw = cfg.models.get("nsfw").expect("nsfw model");
    assert_eq!(nsfw.video_strategy.as_deref(), Some("progressive_scan"));
    let ee = nsfw.early_exit.as_ref().expect("nsfw early_exit");
    assert_eq!(ee.labels, vec!["nsfw".to_string()]);

    let scan = cfg.strategies.get("progressive_scan").expect("strategy");
    assert_eq!(scan.sampling[0].method, SamplingKind::Iframes);
    assert!(scan.early_exit);
}

#[test]
fn defaults_apply_to_minimal_model() {
    let cfg = load::from_str(
        r#"
        [models.m]
        architecture = "vit"
        repo = "a/b"
        "#,
    )
    .unwrap();
    let m = cfg.models.get("m").unwrap();
    assert_eq!(m.revision, "main");
    assert!(m.enabled);
    assert_eq!(m.max_concurrent, 8);
    assert_eq!(m.timeout, 30);
    assert_eq!(cfg.app.port, 8080);
}

#[test]
fn rejects_unknown_strategy_reference() {
    let cfg = load::from_str(
        r#"
        [models.x]
        architecture = "vit"
        repo = "a/b"
        video_strategy = "missing"
        "#,
    )
    .unwrap();
    assert!(cfg.validate().is_err());
}

#[test]
fn rejects_sampling_missing_required_param() {
    let cfg = load::from_str(
        r#"
        [strategies.s]
        [[strategies.s.sampling]]
        step = 1
        method = "fps"
        "#,
    )
    .unwrap();
    assert!(cfg.validate().is_err());
}

#[test]
fn set_get_remove_roundtrip() {
    let mut doc: DocumentMut = "[app]\nport = 8080\n".parse().unwrap();

    edit::set(&mut doc, "app.port", "9090").unwrap();
    assert_eq!(edit::get(&doc, "app.port").as_deref(), Some("9090"));

    edit::set(
        &mut doc,
        "models.nsfw.repo",
        "Falconsai/nsfw_image_detection",
    )
    .unwrap();
    assert_eq!(
        edit::get(&doc, "models.nsfw.repo").as_deref(),
        Some("\"Falconsai/nsfw_image_detection\"")
    );

    assert!(edit::remove(&mut doc, "app.port").unwrap());
    assert!(edit::get(&doc, "app.port").is_none());
}
