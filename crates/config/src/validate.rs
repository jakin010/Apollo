//! Structural validation. Catches reference and shape problems before startup.
//!
//! The "exit if zero models" rule is a *start* policy, not a validity error
//! (a default `[app]`-only config is valid for the `config` command); see
//! [`Config::has_models`].

use crate::error::ConfigError;
use crate::schema::{Backend, Config, SamplingKind};
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
        }

        for (name, pipeline) in &self.pipelines {
            if pipeline.steps.is_empty() {
                errs.push(format!("pipeline '{name}': has no steps"));
            }
            let mut orders = BTreeSet::new();
            for step in &pipeline.steps {
                if !self.models.contains_key(&step.model) {
                    errs.push(format!(
                        "pipeline '{name}': step references unknown model '{}'",
                        step.model
                    ));
                }
                if !orders.insert(step.order) {
                    errs.push(format!(
                        "pipeline '{name}': duplicate step order {}",
                        step.order
                    ));
                }
                if let Some(stop) = &step.stop_if
                    && stop.labels.is_empty()
                {
                    errs.push(format!(
                        "pipeline '{name}': step '{}' stop_if needs at least one label id",
                        step.model
                    ));
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
