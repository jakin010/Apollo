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
    assert_eq!(nsfw.video_strategy.as_deref(), Some("coverage_scan"));
    let ee = nsfw.early_exit.as_ref().expect("nsfw early_exit");
    assert_eq!(ee.labels, vec![1u32]);

    let coverage = cfg.strategies.get("coverage_scan").expect("strategy");
    assert!(coverage.early_exit);
    assert_eq!(
        coverage.sampling.last().map(|s| s.method),
        Some(SamplingKind::Refine)
    );
    // Every scene cut: a min_gap skips about half the inserts shorter than it.
    let scene = coverage
        .sampling
        .iter()
        .find(|s| s.method == SamplingKind::Scene)
        .expect("scene step");
    assert_eq!(scene.min_gap.unwrap_or(0.0), 0.0);
    let scan = cfg.strategies.get("progressive_scan").expect("strategy");
    assert_eq!(scan.sampling[0].method, SamplingKind::Iframes);
    assert!(scan.early_exit);

    let moderation = cfg.pipelines.get("moderation").expect("pipeline");
    assert_eq!(moderation.models(), ["nsfw", "category", "general"]);
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
fn device_defaults_to_auto_and_parses_overrides() {
    use crate::DevicePreference;

    assert_eq!(
        load::from_str("").unwrap().app.device,
        DevicePreference::Auto
    );
    let cfg = load::from_str("[app]\ndevice = \"cpu\"\n").unwrap();
    assert_eq!(cfg.app.device, DevicePreference::Cpu);
    assert!(load::from_str("[app]\ndevice = \"tpu\"\n").is_err());
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

/// A coverage-scan strategy with a scene `min_gap` and a final `refine` step,
/// used by `model` with an early-exit trigger at 0.85. `extra` is appended.
fn coverage_scan(extra: &str) -> String {
    format!(
        r#"
        [strategies.cs]
        early_exit = true
        [[strategies.cs.sampling]]
        step = 1
        method = "uniform"
        count = 8
        [[strategies.cs.sampling]]
        step = 2
        method = "scene"
        threshold = 0.3
        min_gap = 1.0
        [[strategies.cs.sampling]]
        step = 3
        method = "refine"
        min_score = 0.1
        window = 2.0
        fps = 5
        [models.m]
        architecture = "vit"
        repo = "a/b"
        video_strategy = "cs"
        {extra}
        "#
    )
}

#[test]
fn accepts_scene_min_gap_and_a_final_refine_step() {
    let cfg = load::from_str(&coverage_scan(
        "[models.m.early_exit]\nlabels = [1]\nthreshold = 0.85",
    ))
    .unwrap();
    cfg.validate().unwrap();
    let steps = &cfg.strategies["cs"].sampling;
    assert_eq!(steps[1].min_gap, Some(1.0));
    assert_eq!(steps[2].method, SamplingKind::Refine);
}

#[test]
fn refine_needs_the_models_trigger_below_its_min_score() {
    // No early_exit on the model: refine has nothing to look closer at.
    let err = load::from_str(&coverage_scan(""))
        .unwrap()
        .validate()
        .unwrap_err()
        .to_string();
    assert!(err.contains("needs the model's"), "{err}");

    // min_score at or above the trigger threshold can never be ambiguous.
    let err = load::from_str(&coverage_scan(
        "[models.m.early_exit]\nlabels = [1]\nthreshold = 0.05",
    ))
    .unwrap()
    .validate()
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("must be below the early_exit threshold"),
        "{err}"
    );
}

#[test]
fn rejects_misplaced_or_incomplete_refine_and_min_gap() {
    let cfg = load::from_str(
        r#"
        [strategies.s]
        [[strategies.s.sampling]]
        step = 1
        method = "refine"
        min_score = 0.1
        [[strategies.s.sampling]]
        step = 2
        method = "fps"
        fps = 1
        min_gap = 2.0
        [[strategies.s.sampling]]
        step = 3
        method = "scene"
        threshold = 0.3
        min_gap = -1.0
        "#,
    )
    .unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    for expected in [
        "requires 'window'",
        "requires 'fps'",
        "must come after all of them",
        "'min_gap' only applies to method 'scene'",
        "min_gap must be a finite number",
    ] {
        assert!(err.contains(expected), "missing {expected:?} in {err}");
    }
}

#[test]
fn confirm_parses_and_validates() {
    let base = |strategy_extra: &str, model_extra: &str| {
        format!(
            r#"
            [strategies.s]
            early_exit = true
            {strategy_extra}
            [[strategies.s.sampling]]
            step = 1
            method = "uniform"
            count = 8
            [models.m]
            architecture = "vit"
            repo = "a/b"
            video_strategy = "s"
            {model_extra}
            "#
        )
    };
    let trigger = "[models.m.early_exit]\nlabels = [1]";

    let cfg = load::from_str(&base("confirm = 1", trigger)).unwrap();
    cfg.validate().unwrap();
    assert_eq!(cfg.strategies["s"].confirm, 1);
    assert!((cfg.strategies["s"].confirm_window - 0.25).abs() < 1e-9);
    // Off by default.
    assert_eq!(
        load::from_str(&base("", trigger)).unwrap().strategies["s"].confirm,
        0
    );

    let err = |strategy_extra: &str, model_extra: &str| {
        load::from_str(&base(strategy_extra, model_extra))
            .unwrap()
            .validate()
            .unwrap_err()
            .to_string()
    };
    assert!(err("confirm = 1", "").contains("sets confirm, which needs"));
    assert!(err("confirm = 9", trigger).contains("confirm must be at most 8"));
    assert!(err("confirm = 1\nconfirm_window = 0.01", trigger).contains("confirm_window"));
}

#[test]
fn max_frames_parses_and_rejects_zero() {
    let cfg = |extra: &str| {
        load::from_str(&format!(
            "[strategies.s]\n{extra}\n[[strategies.s.sampling]]\nstep = 1\nmethod = \"uniform\"\ncount = 8\n"
        ))
        .unwrap()
    };
    assert_eq!(cfg("").strategies["s"].max_frames, None);
    let c = cfg("max_frames = 40");
    c.validate().unwrap();
    assert_eq!(c.strategies["s"].max_frames, Some(40));
    let err = cfg("max_frames = 0").validate().unwrap_err().to_string();
    assert!(err.contains("max_frames must be at least 1"), "{err}");
}

fn pipeline(steps: &str) -> String {
    format!(
        r#"
        [models.nsfw]
        architecture = "vit"
        repo = "a/b"
        [models.category]
        architecture = "vit"
        repo = "a/c"
        [models.general]
        architecture = "vit"
        repo = "a/d"

        [pipelines.moderation]
        steps = [{steps}]
        "#
    )
}

#[test]
fn a_pipeline_step_takes_one_model_or_a_list() {
    let cfg = load::from_str(&pipeline(
        r#"
        { model = ["category", "general"], order = 2 },
        { model = "nsfw", order = 1, stop_if = { labels = [1], threshold = 0.85 } },
        "#,
    ))
    .unwrap();
    cfg.validate().expect("valid");
    let p = cfg.pipelines.get("moderation").unwrap();
    assert_eq!(p.models(), ["nsfw", "category", "general"]);
    let steps = p.ordered_steps();
    assert_eq!(steps[0].gate_model(), Some("nsfw"));
    assert_eq!(steps[1].models(), ["category", "general"]);
    assert_eq!(steps[1].gate_model(), None);
}

#[test]
fn a_multi_model_gate_names_its_model() {
    let ok = load::from_str(&pipeline(
        r#"{ model = ["nsfw", "category"], order = 1, stop_if = { model = "nsfw", labels = [1] } },
           { model = "general", order = 2 }"#,
    ))
    .unwrap();
    ok.validate().expect("valid");
    let step = ok.pipelines["moderation"].ordered_steps()[0].clone();
    assert_eq!(step.gate_model(), Some("nsfw"));

    for (steps, needle) in [
        (
            r#"{ model = ["nsfw", "category"], order = 1, stop_if = { labels = [1] } }"#,
            "stop_if.model must name",
        ),
        (
            r#"{ model = ["nsfw", "category"], order = 1, stop_if = { model = "general", labels = [1] } }"#,
            "is not one of the step's models",
        ),
        (
            r#"{ model = "nsfw", order = 1 }, { model = ["nsfw", "general"], order = 2 }"#,
            "appears in more than one step",
        ),
        (r#"{ model = [], order = 1 }"#, "lists no models"),
        (r#"{ model = ["nope"], order = 1 }"#, "unknown model 'nope'"),
    ] {
        let cfg = load::from_str(&pipeline(steps)).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains(needle), "{steps}: {err}");
    }
}

#[test]
fn set_reaches_into_inline_tables_and_keeps_comments() {
    let text = r#"
[models.nsfw]
repo = "a/b"
early_exit = { labels = [1], threshold = 0.85 }

[models.general.early_exit]
labels    = [3]
threshold = 0.5   # tuned by hand
"#;
    let mut doc: DocumentMut = text.parse().unwrap();
    edit::set_value(&mut doc, "models.nsfw.early_exit.threshold", 0.7.into()).unwrap();
    edit::set_value(&mut doc, "models.general.early_exit.threshold", 0.65.into()).unwrap();
    let out = doc.to_string();
    assert!(
        out.contains("early_exit = { labels = [1], threshold = 0.7 }"),
        "{out}"
    );
    assert!(out.contains("threshold = 0.65   # tuned by hand"), "{out}");
}

#[test]
fn pdq_models_need_a_list_and_categories() {
    let dir = tempfile::tempdir().unwrap();
    let list = dir.path().join("blocklist.txt");
    std::fs::write(&list, "").unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        r#"
        [models.known]
        architecture = "pdq"
        hash_list    = "blocklist.txt"
        categories   = { 1 = "known", 7 = "spam" }
        max_distance = 40
        "#,
    )
    .unwrap();
    let cfg = load::load_from(&config).unwrap();
    cfg.validate().expect("valid");
    let m = &cfg.models["known"];
    // Resolved from the config file's directory.
    assert_eq!(
        m.hash_list.as_deref(),
        Some(list.to_string_lossy().as_ref())
    );
    assert_eq!(m.categories.len(), 2);
    assert!(m.repo.is_empty());

    for (toml, needle) in [
        (
            "[models.k]\narchitecture = \"pdq\"\ncategories = { 1 = \"a\" }\n",
            "need a `hash_list`",
        ),
        (
            "[models.k]\narchitecture = \"pdq\"\nhash_list = \"/no/such/list\"\ncategories = { 1 = \"a\" }\n",
            "does not exist",
        ),
        (
            &format!(
                "[models.k]\narchitecture = \"pdq\"\nhash_list = \"{}\"\n",
                list.display()
            ),
            "need `categories`",
        ),
        (
            &format!(
                "[models.k]\narchitecture = \"pdq\"\nhash_list = \"{}\"\ncategories = {{ x = \"a\" }}\nmax_distance = 300\n",
                list.display()
            ),
            "max_distance is at most 256",
        ),
        (
            &format!(
                "[models.k]\narchitecture = \"pdq\"\nhash_list = \"{}\"\ncategories = {{ 1 = \"a\" }}\nlabels = [\"x\"]\n",
                list.display()
            ),
            "`labels` does not apply",
        ),
        (
            "[models.k]\narchitecture = \"vit\"\nrepo = \"a/b\"\nmax_distance = 3\n",
            "only applies to architecture \"pdq\"",
        ),
        ("[models.k]\narchitecture = \"vit\"\n", "`repo` is required"),
    ] {
        let err = load::from_str(toml)
            .unwrap()
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.contains(needle), "{toml}: {err}");
    }

    // The list's lines are checked too.
    let good = format!("{} 7  # spam\n", "ab".repeat(32));
    for (content, needle) in [
        (format!("{good}zzz 1\n"), "line 2: 'zzz' is not a PDQ hash"),
        (
            format!("{} 9\n", "ab".repeat(32)),
            "category '9' is not one of",
        ),
        (
            format!("{}\n", "ab".repeat(32)),
            "expected `<hash> <category id>`",
        ),
    ] {
        std::fs::write(&list, content).unwrap();
        let err = load::load_from(&config)
            .unwrap()
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.contains(needle), "{err}");
    }
    std::fs::write(&list, good).unwrap();
    load::load_from(&config).unwrap().validate().unwrap();
}
