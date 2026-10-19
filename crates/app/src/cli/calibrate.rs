//! `calibrate`: measure a model's precision and recall on labelled examples
//! over a range of thresholds, recommend one, and optionally write it back.
//!
//! The examples run through an in-process engine with a throwaway database,
//! so images and videos are classified exactly as the server would (a video
//! through the model's strategy, its score the scan's aggregated result). A
//! few settings change for the run so every score is seen:
//! - the strategy's `early_exit` is off, so a scan does not stop at the
//!   current threshold;
//! - SigLIP models keep every label (`score_threshold = 0`, no `max_results`),
//!   and a taxonomy's per-category thresholds are left out;
//! - only this model runs, with the cache and webhook off.
//!
//! A label a model does not return for an example (a ViT label outside its top
//! five) counts as scoring 0.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use clap::Args;
use serde::Serialize;

use apollo_config::{Config, edit, load};
use apollo_domain::{Architecture, Input, ItemState, Label, ModelOutput, ModelState, Url};
use apollo_engine::{Engine, Run, Submission};

#[derive(Args)]
pub struct CalibrateArgs {
    /// The model to calibrate (its `[models.<name>]` name).
    #[arg(long)]
    model: String,
    /// Labelled examples: a directory with a subdirectory per label (named
    /// after the label or its id; `negative` for examples with none of them),
    /// or a JSONL manifest of `{"path": "…", "labels": ["nsfw"]}` lines.
    #[arg(long)]
    data: PathBuf,
    /// Config file (default: the per-user config).
    #[arg(long)]
    config: Option<PathBuf>,
    /// A label to report (name or id; repeatable). Default: every label in the
    /// data, else the model's `early_exit` labels.
    #[arg(long = "label")]
    labels: Vec<String>,
    /// The precision the recommended threshold must reach.
    #[arg(long, default_value_t = 0.95, value_parser = parse_fraction)]
    target_precision: f64,
    /// Print the results as JSON.
    #[arg(long)]
    json: bool,
    /// Write the recommended thresholds: the model's `early_exit.threshold`
    /// (for its trigger labels), and per-category thresholds in its taxonomy
    /// file. Comments and formatting are kept.
    #[arg(long)]
    write: bool,
}

fn parse_fraction(s: &str) -> Result<f64, String> {
    match s.parse::<f64>() {
        Ok(v) if (0.0..=1.0).contains(&v) => Ok(v),
        _ => Err(format!("'{s}' is not a number from 0 to 1")),
    }
}

pub fn run(args: CalibrateArgs) -> anyhow::Result<()> {
    init_logging();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the Tokio runtime")?;
    runtime.block_on(calibrate(args))
}

fn init_logging() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

// --------------------------------- dataset ----------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Image,
    Video,
}

/// One labelled example: a file and the labels (as written) it shows.
#[derive(Debug, Clone, PartialEq)]
struct Example {
    path: PathBuf,
    kind: Kind,
    labels: Vec<String>,
}

fn kind_of(path: &Path) -> Option<Kind> {
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    match ext.as_str() {
        "jpg" | "jpeg" | "png" | "webp" | "gif" | "bmp" | "tif" | "tiff" => Some(Kind::Image),
        "mp4" | "m4v" | "mov" | "mkv" | "webm" | "avi" => Some(Kind::Video),
        _ => None,
    }
}

/// Subdirectory names that mean "none of the labels".
const NEGATIVE_DIRS: &[&str] = &["negative", "negatives", "none", "clean"];

/// Read the examples, and how many files were skipped (not an image or video).
fn load_dataset(data: &Path) -> anyhow::Result<(Vec<Example>, usize)> {
    if data.is_dir() {
        load_directory(data)
    } else {
        load_manifest(data)
    }
}

fn load_directory(root: &Path) -> anyhow::Result<(Vec<Example>, usize)> {
    let mut examples = Vec::new();
    let mut skipped = 0;
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(root)
        .with_context(|| format!("reading {}", root.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    dirs.sort();
    for dir in dirs {
        if !dir.is_dir() {
            skipped += 1;
            continue;
        }
        let name = dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("{} is not a UTF-8 name", dir.display()))?
            .to_string();
        let labels = if NEGATIVE_DIRS.contains(&name.to_ascii_lowercase().as_str()) {
            Vec::new()
        } else {
            vec![name]
        };
        let mut files = Vec::new();
        collect_files(&dir, &mut files)?;
        files.sort();
        for path in files {
            match kind_of(&path) {
                Some(kind) => examples.push(Example {
                    path,
                    kind,
                    labels: labels.clone(),
                }),
                None => skipped += 1,
            }
        }
    }
    Ok((examples, skipped))
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, out)?;
        } else {
            out.push(path);
        }
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct ManifestLine {
    path: PathBuf,
    #[serde(default)]
    labels: Vec<serde_json::Value>,
}

fn load_manifest(path: &Path) -> anyhow::Result<(Vec<Example>, usize)> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let base = path.parent().unwrap_or(Path::new("."));
    let mut examples = Vec::new();
    let mut skipped = 0;
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let entry: ManifestLine = serde_json::from_str(line)
            .with_context(|| format!("{}:{}: not a manifest line", path.display(), n + 1))?;
        let file = if entry.path.is_relative() {
            base.join(&entry.path)
        } else {
            entry.path
        };
        let labels = entry
            .labels
            .iter()
            .map(|v| match v {
                serde_json::Value::String(s) => Ok(s.clone()),
                serde_json::Value::Number(n) => Ok(n.to_string()),
                other => Err(anyhow!(
                    "{}:{}: a label must be a name or an id, not {other}",
                    path.display(),
                    n + 1
                )),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        match kind_of(&file) {
            Some(kind) => examples.push(Example {
                path: file,
                kind,
                labels,
            }),
            None => skipped += 1,
        }
    }
    Ok((examples, skipped))
}

/// A label as written (a name, or an id) to its id.
fn resolve_label(raw: &str, labels: &[Label]) -> anyhow::Result<u32> {
    if let Some(l) = labels.iter().find(|l| l.name == raw) {
        return Ok(l.id);
    }
    if let Ok(id) = raw.parse::<u32>()
        && (labels.is_empty() || labels.iter().any(|l| l.id == id))
    {
        return Ok(id);
    }
    let known: Vec<String> = labels.iter().take(12).map(|l| l.name.clone()).collect();
    Err(anyhow!(
        "unknown label '{raw}' (the model's labels include: {}{})",
        known.join(", "),
        if labels.len() > 12 { ", …" } else { "" }
    ))
}

// ---------------------------------- scoring ----------------------------------

/// A throwaway directory, removed when dropped.
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The config the calibration engine runs: only `model`, nothing that stops
/// a scan or hides a score early, and a database in `scratch`.
fn calibration_config(
    config: &Config,
    model: &str,
    scratch: &Path,
    roots: Vec<String>,
) -> anyhow::Result<Config> {
    let mut cfg = config.clone();
    let mut m = cfg
        .models
        .get(model)
        .cloned()
        .ok_or_else(|| anyhow!("no model '{model}' in the config"))?;
    m.enabled = true;
    if m.architecture == Architecture::Siglip {
        m.score_threshold = Some(0.0);
        m.max_results = None;
        if let Some(file) = &m.taxonomy_file {
            let copy = scratch.join("taxonomy.toml");
            std::fs::write(&copy, without_thresholds(Path::new(file))?)
                .context("writing the calibration taxonomy")?;
            m.taxonomy_file = Some(copy.to_string_lossy().into_owned());
        }
    }
    if let Some(name) = &m.video_strategy
        && let Some(strategy) = cfg.strategies.get_mut(name)
    {
        strategy.early_exit = false;
    }
    cfg.models = BTreeMap::from([(model.to_string(), m)]);
    cfg.pipelines.clear();
    cfg.webhook = None;
    cfg.cache = None;
    cfg.metrics.enabled = false;
    cfg.app.max_pending = 0;
    cfg.database = apollo_config::DatabaseConfig {
        sqlite: Some(apollo_config::SqliteConfig {
            path: scratch.join("calibrate.db").to_string_lossy().into_owned(),
            ..Default::default()
        }),
        ..Default::default()
    };
    cfg.limits.allow_local_files = true;
    cfg.limits.local_roots = roots;
    cfg.validate()
        .context("the calibration config is invalid")?;
    Ok(cfg)
}

/// A taxonomy file's text with every category `threshold` removed.
fn without_thresholds(path: &Path) -> anyhow::Result<String> {
    let mut doc = edit::load_document(path)?;
    for (_, parent) in doc.as_table_mut().iter_mut() {
        if let Some(parent) = parent.as_table_like_mut() {
            for (_, child) in parent.iter_mut() {
                if let Some(child) = child.as_table_like_mut() {
                    child.remove("threshold");
                }
            }
        }
    }
    Ok(doc.to_string())
}

/// Each example's scores by label id; `Err` with why when it failed.
type Scores = Result<BTreeMap<u32, f32>, String>;

async fn score_all(
    engine: &Engine,
    model: &str,
    examples: &[Example],
) -> anyhow::Result<Vec<Scores>> {
    let mut ids = Vec::with_capacity(examples.len());
    for ex in examples {
        let url = Url {
            main: ex.path.to_string_lossy().into_owned(),
            fallback: None,
        };
        let input = match ex.kind {
            Kind::Image => Input::Image(url),
            Kind::Video => Input::Video(url),
        };
        let submitted = engine
            .submit(
                Submission {
                    input,
                    run: Run::Models(vec![model.to_string()]),
                    metadata: Default::default(),
                    request_id: None,
                },
                None,
            )
            .await
            .with_context(|| format!("submitting {}", ex.path.display()))?;
        ids.push(submitted.id);
    }

    let mut out = Vec::with_capacity(ids.len());
    for (n, id) in ids.iter().enumerate() {
        let task = engine
            .wait_for(id, Duration::from_secs(24 * 3600))
            .await?
            .ok_or_else(|| anyhow!("task {id} disappeared"))?;
        eprint!("\rscored {}/{}", n + 1, ids.len());
        let _ = std::io::stderr().flush();
        let item = task.items.into_iter().next();
        out.push(match item {
            Some(item) if item.state == ItemState::Failed => {
                Err(item.error.map_or_else(|| "failed".into(), |e| e.message))
            }
            Some(mut item) => match item.results.remove(model) {
                Some(r) if r.state == ModelState::Done => {
                    let class = match r.output {
                        Some(ModelOutput::Classification(c)) => c,
                        Some(ModelOutput::Scan(s)) => s.aggregated,
                        None => Default::default(),
                    };
                    Ok(class
                        .predictions
                        .into_iter()
                        .map(|p| (p.label, p.score))
                        .collect())
                }
                Some(r) => Err(r
                    .error
                    .map_or_else(|| "not classified".into(), |e| e.message)),
                None => Err("not classified".into()),
            },
            None => Err("no input".into()),
        });
    }
    if !ids.is_empty() {
        eprintln!();
    }
    Ok(out)
}

// ---------------------------------- metrics ----------------------------------

/// The confusion counts and rates at one threshold.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct Point {
    /// Compared as f32, as the models compare scores.
    threshold: f32,
    tp: usize,
    fp: usize,
    #[serde(rename = "fn")]
    fn_: usize,
    tn: usize,
    precision: Option<f64>,
    recall: Option<f64>,
    f1: Option<f64>,
    fpr: Option<f64>,
}

/// Low thresholds, for labels scored far below 0.05 (SigLIP's sigmoid scores
/// often are).
const FINE: [f32; 8] = [0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02];

/// The thresholds the table shows: 0.05, 0.10, … 0.95, preceded by [`FINE`]
/// when a positive example scores below 0.05.
fn grid(scored: &[(f32, bool)]) -> Vec<f32> {
    let low = scored.iter().any(|&(s, p)| p && s > 0.0 && s < 0.05);
    let mut out = if low { FINE.to_vec() } else { Vec::new() };
    out.extend((1..=19).map(|i| (i * 5) as f32 / 100.0));
    out
}

/// The thresholds searched for the best F1 and the recommendation: the grid
/// plus every example's own score, so the exact cut-off is found.
fn candidates(scored: &[(f32, bool)]) -> Vec<f32> {
    let mut out = grid(scored);
    out.extend(
        scored
            .iter()
            .map(|&(s, _)| s)
            .filter(|s| *s > 0.0 && s.is_finite()),
    );
    out.sort_by(f32::total_cmp);
    out.dedup();
    out
}

/// (score, positive) pairs to a point per threshold.
fn curve(scored: &[(f32, bool)], thresholds: &[f32]) -> Vec<Point> {
    let ratio = |a: usize, b: usize| (b > 0).then(|| a as f64 / b as f64);
    thresholds
        .iter()
        .map(|&t| {
            let (mut tp, mut fp, mut fn_, mut tn) = (0, 0, 0, 0);
            for &(score, positive) in scored {
                match (score >= t, positive) {
                    (true, true) => tp += 1,
                    (true, false) => fp += 1,
                    (false, true) => fn_ += 1,
                    (false, false) => tn += 1,
                }
            }
            let precision = ratio(tp, tp + fp);
            let recall = ratio(tp, tp + fn_);
            let f1 = match (precision, recall) {
                (Some(p), Some(r)) if p + r > 0.0 => Some(2.0 * p * r / (p + r)),
                (Some(_), Some(_)) => Some(0.0),
                _ => None,
            };
            Point {
                threshold: t,
                tp,
                fp,
                fn_,
                tn,
                precision,
                recall,
                f1,
                fpr: ratio(fp, fp + tn),
            }
        })
        .collect()
}

/// The point with the highest F1 (the lowest threshold among equals).
fn best_f1(points: &[Point]) -> Option<&Point> {
    points
        .iter()
        .filter(|p| p.f1.is_some())
        .fold(None, |best: Option<&Point>, p| match best {
            Some(b) if b.f1 >= p.f1 => Some(b),
            _ => Some(p),
        })
}

/// The lowest threshold whose precision reaches `target` (with at least one
/// true positive): the most recall for that precision.
fn recommend(points: &[Point], target: f64) -> Option<&Point> {
    points
        .iter()
        .find(|p| p.tp > 0 && p.precision.is_some_and(|v| v >= target - 1e-9))
}

/// A threshold as a TOML float that reads back as the same f32 (its shortest
/// decimal form, e.g. `0.0061234` rather than `0.006123399920761585`).
fn toml_threshold(t: f32) -> f64 {
    t.to_string().parse().unwrap_or(f64::from(t))
}

#[derive(Debug, Serialize)]
struct LabelReport {
    id: u32,
    name: String,
    positives: usize,
    negatives: usize,
    points: Vec<Point>,
    best_f1: Option<Point>,
    recommended: Option<Point>,
}

#[derive(Debug, Serialize)]
struct Report {
    model: String,
    examples: usize,
    failed: usize,
    skipped: usize,
    target_precision: f64,
    labels: Vec<LabelReport>,
    /// What `--write` changed.
    written: Vec<String>,
}

async fn calibrate(args: CalibrateArgs) -> anyhow::Result<()> {
    let config = load::load(args.config.as_deref()).context("loading config")?;
    config.validate().context("config is invalid")?;
    let model_cfg = config
        .models
        .get(&args.model)
        .cloned()
        .ok_or_else(|| anyhow!("no model '{}' in the config", args.model))?;

    let (examples, skipped) = load_dataset(&args.data)?;
    if examples.is_empty() {
        bail!("no images or videos found in {}", args.data.display());
    }
    let roots: BTreeSet<String> = examples
        .iter()
        .filter_map(|e| e.path.canonicalize().ok()?.parent().map(Path::to_path_buf))
        .map(|p| p.to_string_lossy().into_owned())
        .collect();

    let base = dirs::cache_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("apollo");
    let scratch = Scratch(base.join(format!("calibrate-{}", uuid::Uuid::new_v4())));
    crate::daemon::create_private_dir(&scratch.0)?;
    let cfg = calibration_config(
        &config,
        &args.model,
        &scratch.0,
        roots.into_iter().collect(),
    )?;

    let storage = apollo_storage::open(&cfg.database)
        .await
        .context("opening the calibration database")?;
    let engine = Engine::new(Arc::new(cfg), storage, None);
    eprintln!("loading {}…", args.model);
    engine
        .load_model(&args.model)
        .await
        .with_context(|| format!("loading model '{}'", args.model))?;
    let labels = engine
        .catalog()
        .await?
        .models
        .into_iter()
        .find(|m| m.name == args.model)
        .map(|m| m.labels)
        .unwrap_or_default();

    // Ground truth by id, checked before anything runs.
    let truth: Vec<BTreeSet<u32>> = examples
        .iter()
        .map(|e| {
            e.labels
                .iter()
                .map(|l| resolve_label(l, &labels).with_context(|| e.path.display().to_string()))
                .collect()
        })
        .collect::<anyhow::Result<_>>()?;
    let evaluated: BTreeSet<u32> = if !args.labels.is_empty() {
        args.labels
            .iter()
            .map(|l| resolve_label(l, &labels))
            .collect::<anyhow::Result<_>>()?
    } else {
        let seen: BTreeSet<u32> = truth.iter().flatten().copied().collect();
        if seen.is_empty() {
            model_cfg
                .early_exit
                .as_ref()
                .map(|e| e.labels.iter().copied().collect())
                .unwrap_or_default()
        } else {
            seen
        }
    };
    if evaluated.is_empty() {
        bail!("no labels to evaluate: the data has no labelled examples; pass --label");
    }

    let scores = score_all(&engine, &args.model, &examples).await?;
    let failed: Vec<(&Example, &String)> = examples
        .iter()
        .zip(&scores)
        .filter_map(|(e, s)| s.as_ref().err().map(|why| (e, why)))
        .collect();
    for (e, why) in failed.iter().take(5) {
        eprintln!("failed: {}: {why}", e.path.display());
    }

    let name_of = |id: u32| {
        labels
            .iter()
            .find(|l| l.id == id)
            .map_or_else(|| id.to_string(), |l| l.name.clone())
    };
    let mut reports = Vec::new();
    for &id in &evaluated {
        let scored: Vec<(f32, bool)> = scores
            .iter()
            .zip(&truth)
            .filter_map(|(s, t)| {
                let s = s.as_ref().ok()?;
                Some((s.get(&id).copied().unwrap_or(0.0), t.contains(&id)))
            })
            .collect();
        let searched = curve(&scored, &candidates(&scored));
        let positives = scored.iter().filter(|(_, p)| *p).count();
        reports.push(LabelReport {
            id,
            name: name_of(id),
            positives,
            negatives: scored.len() - positives,
            best_f1: best_f1(&searched).cloned(),
            recommended: recommend(&searched, args.target_precision).cloned(),
            points: curve(&scored, &grid(&scored)),
        });
    }

    let mut report = Report {
        model: args.model.clone(),
        examples: examples.len(),
        failed: failed.len(),
        skipped,
        target_precision: args.target_precision,
        labels: reports,
        written: Vec::new(),
    };
    if args.write {
        let path = args.config.clone().unwrap_or_else(load::default_path);
        report.written = write_thresholds(&path, &args.model, &model_cfg, &report.labels)?;
    }
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print!("{}", render(&report, &model_cfg));
    }
    Ok(())
}

// ---------------------------------- writing ----------------------------------

/// Write the recommended thresholds; returns what changed.
fn write_thresholds(
    config_path: &Path,
    model: &str,
    cfg: &apollo_config::ModelConfig,
    labels: &[LabelReport],
) -> anyhow::Result<Vec<String>> {
    let mut written = Vec::new();

    // The model's trigger: one threshold for all its labels, so the highest
    // recommendation among them (each keeps at least the target precision).
    if let Some(trigger) = &cfg.early_exit {
        let ours: Vec<&LabelReport> = labels
            .iter()
            .filter(|l| trigger.labels.contains(&l.id))
            .collect();
        if !ours.is_empty() {
            let mut highest = 0.0f32;
            for l in &ours {
                let p = l.recommended.as_ref().ok_or_else(|| {
                    anyhow!(
                        "no threshold reaches the target precision for '{}'; nothing written",
                        l.name
                    )
                })?;
                highest = highest.max(p.threshold);
            }
            let value = toml_threshold(highest);
            let mut doc = edit::load_document(config_path)?;
            let key = format!("models.{model}.early_exit.threshold");
            edit::set_value(&mut doc, &key, value.into())?;
            edit::save_document(config_path, &doc)?;
            written.push(format!("{key} = {value} in {}", config_path.display()));
        }
    }

    // Per-category thresholds in the taxonomy file.
    if let Some(file) = &cfg.taxonomy_file {
        let taxonomy = apollo_config::Taxonomy::load(Path::new(file))?;
        let mut doc = edit::load_document(Path::new(file))?;
        let mut changed = false;
        for l in labels {
            let (Some(p), Some(child)) = (
                &l.recommended,
                taxonomy.children.iter().find(|c| c.id == l.id),
            ) else {
                continue;
            };
            let value = toml_threshold(p.threshold);
            edit::set_value(&mut doc, &format!("{}.threshold", child.name), value.into())?;
            written.push(format!("{}.threshold = {value} in {file}", child.name));
            changed = true;
        }
        if changed {
            edit::save_document(Path::new(file), &doc)?;
        }
    }

    if written.is_empty() {
        bail!(
            "nothing to write: no evaluated label is one of '{model}'s early_exit labels or taxonomy categories with a recommended threshold"
        );
    }
    Ok(written)
}

// --------------------------------- rendering ---------------------------------

fn render(r: &Report, cfg: &apollo_config::ModelConfig) -> String {
    let mut out = format!(
        "calibrated '{}' on {} examples ({} failed, {} files skipped); target precision {}\n",
        r.model, r.examples, r.failed, r.skipped, r.target_precision
    );
    let num = |v: Option<f64>| v.map_or_else(|| "-".into(), |v| format!("{v:.3}"));
    let point = |p: &Point| {
        format!(
            "{} (precision {}, recall {}, F1 {})",
            p.threshold,
            num(p.precision),
            num(p.recall),
            num(p.f1)
        )
    };
    for l in &r.labels {
        out.push_str(&format!(
            "\nlabel {} '{}': {} positive, {} negative\n",
            l.id, l.name, l.positives, l.negatives
        ));
        out.push_str("  threshold  precision  recall  F1     FPR    TP / FP / FN / TN\n");
        for p in &l.points {
            out.push_str(&format!(
                "  {:<9}  {:<9}  {:<6}  {:<5}  {:<5}  {} / {} / {} / {}\n",
                p.threshold.to_string(),
                num(p.precision),
                num(p.recall),
                num(p.f1),
                num(p.fpr),
                p.tp,
                p.fp,
                p.fn_,
                p.tn
            ));
        }
        match &l.best_f1 {
            Some(p) => out.push_str(&format!("  best F1: {}\n", point(p))),
            None => out.push_str("  best F1: - (needs positive examples)\n"),
        }
        match &l.recommended {
            Some(p) => out.push_str(&format!(
                "  lowest threshold with precision >= {}: {}\n",
                r.target_precision,
                point(p)
            )),
            None => out.push_str(&format!(
                "  no threshold reaches precision {}\n",
                r.target_precision
            )),
        }
    }
    if let Some(trigger) = &cfg.early_exit {
        out.push_str(&format!(
            "\ncurrent early_exit: labels {:?}, threshold {}\n",
            trigger.labels, trigger.threshold
        ));
    }
    for w in &r.written {
        out.push_str(&format!("wrote {w}\n"));
    }
    if !r.written.is_empty() {
        out.push_str("run `apollo reload` to apply it to a running server\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_curve_counts_and_rates() {
        // Positives at 0.9 and 0.6; negatives at 0.7 and 0.1.
        let scored = [(0.9, true), (0.6, true), (0.7, false), (0.1, false)];
        let points = curve(&scored, &grid(&scored));
        assert_eq!(
            points.len(),
            19,
            "no fine grid: no positive scores below 0.05"
        );
        let at = |t: f32| points.iter().find(|p| p.threshold == t).unwrap();

        let p = at(0.5);
        assert_eq!((p.tp, p.fp, p.fn_, p.tn), (2, 1, 0, 1));
        assert!((p.precision.unwrap() - 2.0 / 3.0).abs() < 1e-9);
        assert_eq!(p.recall, Some(1.0));
        assert_eq!(p.fpr, Some(0.5));

        let p = at(0.8);
        assert_eq!((p.tp, p.fp, p.fn_, p.tn), (1, 0, 1, 2));
        assert_eq!(p.precision, Some(1.0));
        // Exactly at the threshold counts as positive.
        assert_eq!(at(0.6).tp, 2);
        // Nothing at or above 0.95 is predicted positive: precision is undefined.
        assert_eq!(at(0.95).precision, None);

        // The search adds the examples' own scores. 0.75 is the lowest
        // threshold that leaves out the negative at 0.7. F1 is highest (0.8)
        // from 0.15 to 0.6 (at 0.1 the negative at 0.1 still counts), and the
        // lowest of those is chosen.
        let searched = curve(&scored, &candidates(&scored));
        assert_eq!(recommend(&searched, 0.95).unwrap().threshold, 0.75);
        let best = best_f1(&searched).unwrap();
        assert!((best.f1.unwrap() - 0.8).abs() < 1e-9);
        assert_eq!(best.threshold, 0.15);
    }

    #[test]
    fn low_scores_get_a_fine_grid_and_exact_recommendations() {
        // SigLIP-like: the positive scores 0.0061, negatives 0.0009 and 0.003.
        let scored = [(0.0061, true), (0.0009, false), (0.003, false)];
        let grid = grid(&scored);
        assert_eq!(grid[0], 0.0001);
        assert_eq!(grid.len(), 8 + 19);
        let searched = curve(&scored, &candidates(&scored));
        // The lowest threshold that leaves out both negatives.
        assert_eq!(recommend(&searched, 0.95).unwrap().threshold, 0.005);
        assert_eq!(toml_threshold(0.0061234), 0.0061234);
    }

    #[test]
    fn labels_resolve_by_name_or_id() {
        let labels = vec![
            Label {
                id: 0,
                name: "normal".into(),
            },
            Label {
                id: 1,
                name: "nsfw".into(),
            },
        ];
        assert_eq!(resolve_label("nsfw", &labels).unwrap(), 1);
        assert_eq!(resolve_label("0", &labels).unwrap(), 0);
        assert!(resolve_label("7", &labels).is_err());
        assert!(resolve_label("cat", &labels).is_err());
        // Unknown label set: any id goes.
        assert_eq!(resolve_label("7", &[]).unwrap(), 7);
    }

    #[test]
    fn datasets_load_from_directories_and_manifests() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for (sub, file) in [
            ("nsfw", "a.jpg"),
            ("nsfw/more", "b.MP4"),
            ("negative", "c.png"),
            ("negative", "notes.txt"),
        ] {
            std::fs::create_dir_all(root.join(sub)).unwrap();
            std::fs::write(root.join(sub).join(file), b"x").unwrap();
        }
        let (examples, skipped) = load_dataset(root).unwrap();
        assert_eq!(skipped, 1);
        let got: Vec<(String, Kind, Vec<String>)> = examples
            .iter()
            .map(|e| {
                let name = e.path.file_name().unwrap().to_string_lossy().into_owned();
                (name, e.kind, e.labels.clone())
            })
            .collect();
        assert_eq!(
            got,
            [
                ("c.png".to_string(), Kind::Image, vec![]),
                ("a.jpg".to_string(), Kind::Image, vec!["nsfw".to_string()]),
                ("b.MP4".to_string(), Kind::Video, vec!["nsfw".to_string()]),
            ]
        );

        let manifest = root.join("set.jsonl");
        std::fs::write(
            &manifest,
            "# comment\n{\"path\": \"nsfw/a.jpg\", \"labels\": [\"nsfw\", 3]}\n\n{\"path\": \"/abs/x.webm\"}\n",
        )
        .unwrap();
        let (examples, skipped) = load_dataset(&manifest).unwrap();
        assert_eq!(skipped, 0);
        assert_eq!(examples[0].path, root.join("nsfw/a.jpg"));
        assert_eq!(examples[0].labels, ["nsfw", "3"]);
        assert_eq!(examples[1].kind, Kind::Video);
        assert!(examples[1].labels.is_empty());
    }

    #[test]
    fn thresholds_are_written_keeping_the_files_formatting() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        std::fs::write(
            &config,
            "[models.nsfw]\narchitecture = \"vit\"\nrepo = \"a/b\"\n# the trigger\nearly_exit = { labels = [1], threshold = 0.85 }\n",
        )
        .unwrap();
        let cfg = load::load_from(&config).unwrap().models["nsfw"].clone();
        let point = |t: f32| Point {
            threshold: t,
            tp: 1,
            fp: 0,
            fn_: 0,
            tn: 0,
            precision: Some(1.0),
            recall: Some(1.0),
            f1: Some(1.0),
            fpr: None,
        };
        let label = |id, t| LabelReport {
            id,
            name: format!("l{id}"),
            positives: 1,
            negatives: 0,
            points: vec![],
            best_f1: None,
            recommended: Some(point(t)),
        };
        // Label 0 is not a trigger label: only label 1's threshold counts.
        let written =
            write_thresholds(&config, "nsfw", &cfg, &[label(0, 0.3), label(1, 0.7)]).unwrap();
        assert_eq!(written.len(), 1);
        let text = std::fs::read_to_string(&config).unwrap();
        assert!(
            text.contains("# the trigger\nearly_exit = { labels = [1], threshold = 0.7 }"),
            "{text}"
        );

        // A model with neither a trigger nor a taxonomy has nothing to write.
        let mut plain = cfg.clone();
        plain.early_exit = None;
        assert!(write_thresholds(&config, "nsfw", &plain, &[label(1, 0.5)]).is_err());
    }

    #[test]
    fn taxonomy_thresholds_are_stripped_for_scoring() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("t.toml");
        std::fs::write(
            &file,
            "[p]\nid = 1\n[p.c]\nid = 10\nprompts = [\"x\"]\nthreshold = 0.4\n",
        )
        .unwrap();
        let text = without_thresholds(&file).unwrap();
        assert!(!text.contains("threshold"), "{text}");
        assert!(text.contains("prompts = [\"x\"]"), "{text}");
    }
}
