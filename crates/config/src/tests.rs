//! Unit tests. `cargo test -p apollo-config` verifies parsing, validation, and
//! the edit round-trip against the committed example config.

use toml_edit::DocumentMut;

use crate::schema::{Backend, SamplingKind};
use crate::{edit, load};

#[test]
fn parses_and_validates_example() {
    let cfg =
        load::load_from("../../config.example.toml".as_ref()).expect("example config should parse");
    cfg.validate().expect("example config should be valid");

    assert!(cfg.has_models());
    assert_eq!(cfg.database.backend, Backend::Sqlite);

    let nsfw = cfg.models.get("nsfw").expect("nsfw model");
    assert_eq!(nsfw.video_strategy.as_deref(), Some("progressive_scan"));
    let ee = nsfw.early_exit.as_ref().expect("nsfw early_exit");
    assert_eq!(ee.labels, vec![1u32]);

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

#[test]
fn webhook_url_override_creates_section() {
    let mut cfg = load::from_str("[models.m]\narchitecture = \"vit\"\nrepo = \"a/b\"\n").unwrap();
    assert!(cfg.webhook.is_none());
    cfg.apply_overrides(&crate::Overrides {
        webhook_url: Some("http://127.0.0.1:9090".into()),
        ..Default::default()
    });
    let w = cfg.webhook.expect("override should enable the webhook");
    assert_eq!(w.url, "http://127.0.0.1:9090");
    assert!(w.secret.is_none());
    assert_eq!(w.redelivery_secs, 60);
}

#[test]
fn rejects_typos_in_security_sections() {
    // A misspelt key must fail loudly rather than silently keep the default.
    assert!(load::from_str("[limits]\nallow_local_file = true\n").is_err());
    assert!(load::from_str("[cache]\nttl = 60\n").is_err());
    assert!(load::from_str("[auth]\npublic_key = \"k\"\nextra = 1\n").is_err());
    assert!(load::from_str("[limits]\nallow_local_files = true\n").is_ok());
}

#[test]
fn auth_is_on_by_default() {
    let cfg = load::from_str("").unwrap();
    assert!(cfg.auth.enabled);
    assert!(cfg.auth.public_key.is_none());

    let cfg = load::from_str("[auth]\npublic_key = \"k4.public.x\"\n").unwrap();
    assert!(cfg.auth.enabled);
    assert_eq!(cfg.auth.public_key.as_deref(), Some("k4.public.x"));

    let cfg = load::from_str("[auth]\nenabled = false\n").unwrap();
    assert!(!cfg.auth.enabled);
}

#[test]
fn tls_and_auth_paths_resolve_from_the_config_dir() {
    let dir = std::env::temp_dir().join(format!("apollo-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for f in ["cert.pem", "key.pem"] {
        std::fs::write(dir.join(f), "x").unwrap();
    }
    let path = dir.join("config.toml");
    std::fs::write(
        &path,
        "[tls]\ncert_file = \"cert.pem\"\nkey_file = \"key.pem\"\n\n\
         [auth]\nadmin_subjects = [\"ops\"]\nrevocation_file = \"revoked\"\n",
    )
    .unwrap();
    let cfg = load::load_from(&path).unwrap();
    cfg.validate().unwrap();
    let tls = cfg.tls.as_ref().unwrap();
    assert_eq!(std::path::Path::new(&tls.cert_file), dir.join("cert.pem"));
    assert!(tls.client_ca_file.is_none());
    assert_eq!(cfg.auth.admin_subjects, vec!["ops".to_string()]);
    assert_eq!(
        std::path::Path::new(cfg.auth.revocation_file.as_deref().unwrap()),
        dir.join("revoked")
    );

    // A missing certificate is a validation error, not a startup surprise.
    let cfg =
        load::from_str("[tls]\ncert_file = \"/nope/c.pem\"\nkey_file = \"/nope/k.pem\"\n").unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    assert!(
        err.contains("tls.cert_file") && err.contains("tls.key_file"),
        "{err}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
