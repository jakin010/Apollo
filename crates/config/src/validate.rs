//! Structural validation. Catches reference and shape problems before startup.
//!
//! The "exit if zero models" rule is a *start* policy, not a validity error
//! (a default `[app]`-only config is valid for the `config` command); see
//! [`Config::has_models`].

use crate::error::ConfigError;
use crate::schema::{Backend, Config, SamplingKind};
use std::collections::BTreeSet;
use std::path::Path;

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
                let missing = match s.method {
                    SamplingKind::Fps => s.fps.is_none().then_some("fps"),
                    SamplingKind::Uniform => s.count.is_none().then_some("count"),
                    SamplingKind::EveryNth => s.nth.is_none().then_some("nth"),
                    SamplingKind::Scene => s.threshold.is_none().then_some("threshold"),
                    SamplingKind::Iframes => None,
                };
                if let Some(param) = missing {
                    errs.push(format!(
                        "strategy '{name}' step {}: method '{:?}' requires '{param}'",
                        s.step, s.method
                    ));
                }
            }
        }

        for (label, model) in &self.models {
            if let Some(vs) = &model.video_strategy
                && !self.strategies.contains_key(vs) {
                    errs.push(format!(
                        "model '{label}': video_strategy '{vs}' is not defined"
                    ));
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
                    && stop.labels.is_empty() {
                        errs.push(format!(
                            "pipeline '{name}': step '{}' stop_if needs at least one label id",
                            step.model
                        ));
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
