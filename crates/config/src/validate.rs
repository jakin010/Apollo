//! Structural validation. Catches reference and shape problems before startup.
//!
//! The "exit if zero models" rule is a *start* policy, not a validity error
//! (a default `[app]`-only config is valid for the `config` command); see
//! [`Config::has_models`].

use crate::error::ConfigError;
use crate::schema::{Backend, Config, ModelConfig, SamplingKind};
use apollo_domain::Architecture;
use std::collections::BTreeSet;
use std::path::Path;

/// Most frames `confirm` may ask for: twice this many are sampled around a
/// candidate, and their frame indices are derived from its index.
pub const MAX_CONFIRM: u32 = 8;

impl Config {
    /// Collect all problems; `Ok` only if there are none.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut errs: Vec<String> = Vec::new();

        for (name, strat) in &self.strategies {
            if strat.sampling.is_empty() {
                errs.push(format!(
                    "strategy '{name}': must define at least one sampling step"
                ));
            }
            let mut seen = BTreeSet::new();
            for s in &strat.sampling {
                if !seen.insert(s.step) {
                    errs.push(format!("strategy '{name}': duplicate step {}", s.step));
                }
                let required: Vec<(&str, bool)> = match s.method {
                    SamplingKind::Fps => vec![("fps", s.fps.is_some())],
                    SamplingKind::Uniform => vec![("count", s.count.is_some())],
                    SamplingKind::EveryNth => vec![("nth", s.nth.is_some())],
                    SamplingKind::Scene => vec![("threshold", s.threshold.is_some())],
                    SamplingKind::Refine => vec![
                        ("min_score", s.min_score.is_some()),
                        ("window", s.window.is_some()),
                        ("fps", s.fps.is_some()),
                    ],
                    SamplingKind::Iframes => vec![],
                };
                for (param, _) in required.iter().filter(|(_, present)| !present) {
                    errs.push(format!(
                        "strategy '{name}' step {}: method '{:?}' requires '{param}'",
                        s.step, s.method
                    ));
                }
                if s.min_gap.is_some() && s.method != SamplingKind::Scene {
                    errs.push(format!(
                        "strategy '{name}' step {}: 'min_gap' only applies to method 'scene'",
                        s.step
                    ));
                }
                if let Some(g) = s.min_gap
                    && (!g.is_finite() || g < 0.0)
                {
                    errs.push(format!(
                        "strategy '{name}' step {}: min_gap must be a finite number of seconds >= 0 (got {g})",
                        s.step
                    ));
                }
                if s.method == SamplingKind::Refine {
                    if let Some(m) = s.min_score
                        && (!m.is_finite() || !(0.0..1.0).contains(&m))
                    {
                        errs.push(format!(
                            "strategy '{name}' step {}: min_score must be within 0.0..1.0 (got {m})",
                            s.step
                        ));
                    }
                    if let Some(w) = s.window
                        && (!w.is_finite() || w <= 0.0 || w > 60.0)
                    {
                        errs.push(format!(
                            "strategy '{name}' step {}: window must be > 0 and at most 60 seconds (got {w})",
                            s.step
                        ));
                    }
                    if let Some(f) = s.fps
                        && (!f.is_finite() || f <= 0.0 || f > 60.0)
                    {
                        errs.push(format!(
                            "strategy '{name}' step {}: fps must be > 0 and at most 60 (got {f})",
                            s.step
                        ));
                    }
                }
                if let SamplingKind::Scene = s.method
                    && let Some(t) = s.threshold
                    && (!t.is_finite() || !(0.0..=1.0).contains(&t))
                {
                    errs.push(format!(
                                "strategy '{name}' step {}: scene threshold must be finite and within 0.0..=1.0 (got {t})",
                                s.step
                            ));
                }
            }
        }

        for (name, strat) in &self.strategies {
            if strat.max_frames == Some(0) {
                errs.push(format!(
                    "strategy '{name}': max_frames must be at least 1 (omit it for no limit)"
                ));
            }
            if strat.confirm > MAX_CONFIRM {
                errs.push(format!(
                    "strategy '{name}': confirm must be at most {MAX_CONFIRM} (got {})",
                    strat.confirm
                ));
            }
            let w = strat.confirm_window;
            if strat.confirm > 0 && (!w.is_finite() || w <= 0.05 || w > 5.0) {
                errs.push(format!(
                    "strategy '{name}': confirm_window must be above 0.05 and at most 5 seconds (got {w})"
                ));
            }
            let last_planned = strat
                .sampling
                .iter()
                .filter(|s| s.method != SamplingKind::Refine)
                .map(|s| s.step)
                .max();
            for s in strat
                .sampling
                .iter()
                .filter(|s| s.method == SamplingKind::Refine)
            {
                if last_planned.is_some_and(|last| s.step < last) {
                    errs.push(format!(
                        "strategy '{name}' step {}: a 'refine' step looks at the scores of the other steps, so it must come after all of them",
                        s.step
                    ));
                }
            }
        }

        for (label, model) in &self.models {
            if let Some(vs) = &model.video_strategy
                && !self.strategies.contains_key(vs)
            {
                errs.push(format!(
                    "model '{label}': video_strategy '{vs}' is not defined"
                ));
            }
            // A refine step looks closer at frames scoring just below the
            // model's trigger, so it needs the model to define one.
            if let Some(strat) = model
                .video_strategy
                .as_ref()
                .and_then(|vs| self.strategies.get(vs))
            {
                if strat.confirm > 0 && model.early_exit.is_none() {
                    errs.push(format!(
                        "model '{label}': its strategy sets confirm, which needs the model's [models.{label}.early_exit] labels and threshold"
                    ));
                }
                for s in strat
                    .sampling
                    .iter()
                    .filter(|s| s.method == SamplingKind::Refine)
                {
                    match &model.early_exit {
                        None => errs.push(format!(
                            "model '{label}': strategy step {} is 'refine', which needs the model's [models.{label}.early_exit] labels and threshold",
                            s.step
                        )),
                        Some(ee) => {
                            if let Some(m) = s.min_score
                                && m >= ee.threshold
                            {
                                errs.push(format!(
                                    "model '{label}': refine min_score ({m}) must be below the early_exit threshold ({})",
                                    ee.threshold
                                ));
                            }
                        }
                    }
                }
            }
            if let Some(ee) = &model.early_exit {
                if ee.labels.is_empty() {
                    errs.push(format!(
                        "model '{label}': early_exit must list at least one label"
                    ));
                }
                if model.video_strategy.is_none() {
                    errs.push(format!(
                        "model '{label}': early_exit has no effect without a video_strategy"
                    ));
                }
            }
            if !model.labels.is_empty() && model.taxonomy_file.is_some() {
                errs.push(format!(
                    "model '{label}': set either `labels` or `taxonomy_file`, not both"
                ));
            }
            if let Some(tf) = &model.taxonomy_file
                && !Path::new(tf).exists()
            {
                errs.push(format!(
                    "model '{label}': taxonomy file `{}` does not exist",
                    tf
                ));
            }
            validate_architecture(label, model, &mut errs);
        }

        for (name, pipeline) in &self.pipelines {
            if pipeline.steps.is_empty() {
                errs.push(format!("pipeline '{name}': has no steps"));
            }
            let mut orders = BTreeSet::new();
            let mut seen = BTreeSet::new();
            for step in &pipeline.steps {
                let models = step.models();
                if models.is_empty() {
                    errs.push(format!(
                        "pipeline '{name}': step {} lists no models",
                        step.order
                    ));
                }
                for model in models {
                    if !self.models.contains_key(model) {
                        errs.push(format!(
                            "pipeline '{name}': step references unknown model '{model}'"
                        ));
                    }
                    if !seen.insert(model.as_str()) {
                        errs.push(format!(
                            "pipeline '{name}': model '{model}' appears in more than one step"
                        ));
                    }
                }
                if !orders.insert(step.order) {
                    errs.push(format!(
                        "pipeline '{name}': duplicate step order {}",
                        step.order
                    ));
                }
                if let Some(stop) = &step.stop_if {
                    if stop.labels.is_empty() {
                        errs.push(format!(
                            "pipeline '{name}': step {} stop_if needs at least one label id",
                            step.order
                        ));
                    }
                    match &stop.model {
                        Some(m) if !models.contains(m) => errs.push(format!(
                            "pipeline '{name}': step {} stop_if.model '{m}' is not one of the step's models",
                            step.order
                        )),
                        None if models.len() > 1 => errs.push(format!(
                            "pipeline '{name}': step {} runs several models, so stop_if.model must name the one to check",
                            step.order
                        )),
                        _ => {}
                    }
                }
            }
        }

        if let Some(tls) = &self.tls {
            let files = [
                ("cert_file", Some(&tls.cert_file)),
                ("key_file", Some(&tls.key_file)),
                ("client_ca_file", tls.client_ca_file.as_ref()),
            ];
            for (field, path) in files {
                if let Some(path) = path
                    && !Path::new(path).is_file()
                {
                    errs.push(format!("tls.{field} `{path}` does not exist"));
                }
            }
        }

        match self.database.backend {
            Backend::Postgres if self.database.postgres.is_none() => {
                errs.push("database.backend = 'postgres' but [database.postgres] is missing".into())
            }
            Backend::Surrealdb if self.database.surrealdb.is_none() => errs
                .push("database.backend = 'surrealdb' but [database.surrealdb] is missing".into()),
            _ => {}
        }

        if errs.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Validation(errs))
        }
    }

    /// Number of models defined.
    pub fn model_count(&self) -> usize {
        self.models.len()
    }

    /// Whether any model is defined. The daemon exits at startup if this is false.
    pub fn has_models(&self) -> bool {
        !self.models.is_empty()
    }
}

/// A SigLIP score threshold: from 0 to 1, with at most two decimals (scores
/// are calibrated so that hundredths are fine enough). Tolerates f32 rounding.
pub(crate) fn is_score_threshold(t: f64) -> bool {
    (0.0..=1.0).contains(&t) && (t * 100.0 - (t * 100.0).round()).abs() < 1e-4
}

/// The fields each architecture needs, and those it cannot use.
fn validate_architecture(label: &str, model: &ModelConfig, errs: &mut Vec<String>) {
    use apollo_domain::ModelInput;
    let arch = model.architecture.as_str();
    // Fields set where they have no meaning, collected apart from `errs` so
    // the arms below can add to it too.
    let mut misplaced = Vec::new();
    let mut not_for = |field: &str, set: bool, why: &str| {
        if set {
            misplaced.push(format!(
                "model '{label}': `{field}` {why} (architecture \"{arch}\")"
            ));
        }
    };
    let pdq_fields = [
        ("hash_list", model.hash_list.is_some()),
        ("categories", !model.categories.is_empty()),
        ("max_distance", model.max_distance.is_some()),
        ("min_quality", model.min_quality.is_some()),
        ("match_rotations", model.match_rotations.is_some()),
    ];
    let siglip_fields = [
        ("labels", !model.labels.is_empty()),
        ("taxonomy_file", model.taxonomy_file.is_some()),
        ("prompt_template", model.prompt_template.is_some()),
        ("score_threshold", model.score_threshold.is_some()),
        ("max_results", model.max_results.is_some()),
    ];
    let text_fields = [
        ("max_tokens", model.max_tokens.is_some()),
        ("multi_label", model.multi_label.is_some()),
    ];
    if model.architecture != Architecture::Pdq {
        for (field, set) in pdq_fields {
            not_for(field, set, "only applies to architecture \"pdq\"");
        }
    }
    if model.architecture.input() != ModelInput::Text {
        for (field, set) in text_fields {
            not_for(field, set, "only applies to text models");
        }
    }
    match model.architecture {
        Architecture::Vit | Architecture::Siglip => {
            if model.repo.is_empty() {
                errs.push(format!("model '{label}': `repo` is required"));
            }
            if let Some(t) = model.score_threshold
                && !is_score_threshold(f64::from(t))
            {
                errs.push(format!(
                    "model '{label}': score_threshold must be from 0 to 1 with at most 2 decimals (got {t})"
                ));
            }
        }
        Architecture::Pdq => {
            match &model.hash_list {
                None => errs.push(format!("model '{label}': pdq models need a `hash_list`")),
                Some(list) if !Path::new(list).is_file() => errs.push(format!(
                    "model '{label}': hash list `{list}` does not exist"
                )),
                Some(list) => {
                    if let Err(e) = check_hash_list(Path::new(list), &model.categories) {
                        errs.push(format!("model '{label}': hash list `{list}`: {e}"));
                    }
                }
            }
            if model.categories.is_empty() {
                errs.push(format!(
                    "model '{label}': pdq models need `categories`, e.g. categories = {{ 1 = \"known\" }}"
                ));
            }
            for id in model.categories.keys() {
                if id.parse::<u32>().is_err() {
                    errs.push(format!(
                        "model '{label}': category id '{id}' is not a number"
                    ));
                }
            }
            if model.max_distance.is_some_and(|d| d > 256) {
                errs.push(format!(
                    "model '{label}': max_distance is at most 256 (the hash's bits)"
                ));
            }
            if model.min_quality.is_some_and(|q| q > 100) {
                errs.push(format!("model '{label}': min_quality is at most 100"));
            }
            for (field, set) in siglip_fields {
                not_for(field, set, "does not apply");
            }
        }
        Architecture::Bert
        | Architecture::Distilbert
        | Architecture::XlmRoberta
        | Architecture::Modernbert
        | Architecture::DebertaV2 => {
            if model.repo.is_empty() {
                errs.push(format!("model '{label}': `repo` is required"));
            }
            if model.max_tokens.is_some_and(|t| t < 16) {
                errs.push(format!("model '{label}': max_tokens must be at least 16"));
            }
            for (field, set) in siglip_fields {
                not_for(field, set, "does not apply");
            }
            not_for(
                "video_strategy",
                model.video_strategy.is_some(),
                "does not apply to a text model",
            );
            not_for(
                "early_exit",
                model.early_exit.is_some(),
                "only applies to video scans, which text models do not run",
            );
        }
    }
    errs.extend(misplaced);
}

/// Check a pdq hash list's lines: `<64 hex digits> <category id>`, the id one
/// of `categories`; blank lines and `#` comments allowed. Mirrors the parser
/// in `apollo-inference`, so a broken list is rejected before it is used.
fn check_hash_list(
    path: &Path,
    categories: &std::collections::BTreeMap<String, String>,
) -> Result<(), String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    for (n, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        let [hash, category] = fields[..] else {
            return Err(format!("line {}: expected `<hash> <category id>`", n + 1));
        };
        if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!(
                "line {}: '{hash}' is not a PDQ hash (64 hex digits)",
                n + 1
            ));
        }
        let known = category
            .parse::<u32>()
            .ok()
            .is_some_and(|id| categories.keys().any(|k| k.parse::<u32>() == Ok(id)));
        if !known {
            return Err(format!(
                "line {}: category '{category}' is not one of the model's `categories`",
                n + 1
            ));
        }
    }
    Ok(())
}
