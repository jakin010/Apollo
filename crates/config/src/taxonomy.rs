//! Parsing for SigLIP taxonomy files.
//!
//! A taxonomy is a two-level tree expressed in TOML: top-level tables are parent
//! categories (each with an integer `id`), and their nested tables are child
//! categories (each with an `id`, a list of `prompts`, an optional
//! `aggregation` — `mean`/`average`/`max`, default `mean` — controlling how the
//! child's per-prompt scores combine into its score, and an optional
//! `threshold` that replaces the model's `score_threshold` for that child). It
//! is flattened here for the inference arm: one ordered prompt list, plus per
//! child its id, parent id, aggregation, threshold, and the slice of prompts it
//! owns.

use crate::error::ConfigError;
use crate::schema::Aggregation;

/// A flattened taxonomy ready for scoring.
#[derive(Debug, Clone, PartialEq)]
pub struct Taxonomy {
    /// Every prompt across all children, in child order.
    pub prompts: Vec<String>,
    /// One entry per child category.
    pub children: Vec<TaxonChild>,
}

/// A child category: its id, its parent's id, how to combine its prompt scores,
/// and the half-open `[prompt_start, prompt_start + prompt_len)` slice of
/// [`Taxonomy::prompts`] that belongs to it.
#[derive(Debug, Clone, PartialEq)]
pub struct TaxonChild {
    pub id: u32,
    /// `<parent>.<child>`, as written in the file (e.g. `food_and_drink.drinks`).
    pub name: String,
    pub parent_id: u32,
    pub aggregation: Aggregation,
    /// The child's own `threshold`, used instead of the model's
    /// `score_threshold` (set by `apollo calibrate --write`, or by hand).
    pub threshold: Option<f32>,
    pub prompt_start: usize,
    pub prompt_len: usize,
}

impl Taxonomy {
    /// Read and parse a taxonomy file.
    pub fn load(path: &std::path::Path) -> Result<Self, ConfigError> {
        let text =
            std::fs::read_to_string(path).map_err(|e| ConfigError::Io(path.to_path_buf(), e))?;
        Self::parse(&text)
    }

    /// Parse a taxonomy from TOML text.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let table: toml::Table =
            toml::from_str(text).map_err(|e| ConfigError::Parse(format!("taxonomy: {e}")))?;

        let mut prompts: Vec<String> = Vec::new();
        let mut children: Vec<TaxonChild> = Vec::new();
        let mut parent_ids = std::collections::BTreeSet::new();
        let mut child_ids = std::collections::BTreeSet::new();

        for (parent_name, parent_val) in &table {
            let parent_tbl = parent_val.as_table().ok_or_else(|| {
                ConfigError::Parse(format!("taxonomy: '{parent_name}' must be a table"))
            })?;
            let parent_id = read_id(parent_tbl, parent_name)?;
            if !parent_ids.insert(parent_id) {
                return Err(ConfigError::Parse(format!(
                    "taxonomy: duplicate parent id {parent_id}"
                )));
            }

            for (child_name, child_val) in parent_tbl {
                // A parent table holds its own scalar fields (`id`) alongside its
                // child tables; only the tables are children.
                let Some(child_tbl) = child_val.as_table() else {
                    continue;
                };
                let qualified = format!("{parent_name}.{child_name}");
                let id = read_id(child_tbl, &qualified)?;
                if !child_ids.insert(id) {
                    return Err(ConfigError::Parse(format!(
                        "taxonomy: duplicate child id {id} (in '{qualified}')"
                    )));
                }

                let list = child_tbl
                    .get("prompts")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| {
                        ConfigError::Parse(format!(
                            "taxonomy: '{qualified}' needs a `prompts` array"
                        ))
                    })?;
                let start = prompts.len();
                for pr in list {
                    let s = pr.as_str().ok_or_else(|| {
                        ConfigError::Parse(format!(
                            "taxonomy: '{qualified}' prompts must be strings"
                        ))
                    })?;
                    prompts.push(s.to_string());
                }
                let len = prompts.len() - start;
                if len == 0 {
                    return Err(ConfigError::Parse(format!(
                        "taxonomy: '{qualified}' has an empty `prompts` list"
                    )));
                }

                let aggregation = match child_tbl.get("aggregation").and_then(|v| v.as_str()) {
                    None | Some("mean") | Some("average") => Aggregation::Mean,
                    Some("max") => Aggregation::Max,
                    Some(other) => {
                        return Err(ConfigError::Parse(format!(
                            "taxonomy: '{qualified}' has unknown aggregation '{other}' \
                             (use mean/average/max)"
                        )));
                    }
                };

                let threshold = match child_tbl.get("threshold") {
                    None => None,
                    Some(v) => {
                        let t = v
                            .as_float()
                            .or_else(|| v.as_integer().map(|i| i as f64))
                            .filter(|&t| crate::validate::is_score_threshold(t))
                            .ok_or_else(|| {
                                ConfigError::Parse(format!(
                                    "taxonomy: '{qualified}' threshold must be a number from 0 to 1 \
                                     with at most 2 decimals"
                                ))
                            })?;
                        Some(t as f32)
                    }
                };

                children.push(TaxonChild {
                    id,
                    name: qualified,
                    parent_id,
                    aggregation,
                    threshold,
                    prompt_start: start,
                    prompt_len: len,
                });
            }
        }

        if children.is_empty() {
            return Err(ConfigError::Parse(
                "taxonomy: no child categories with prompts were found".into(),
            ));
        }
        Ok(Taxonomy { prompts, children })
    }
}

/// Read a required integer `id` from a table, validating it fits in `u32`.
fn read_id(tbl: &toml::Table, ctx: &str) -> Result<u32, ConfigError> {
    let id = tbl
        .get("id")
        .and_then(|v| v.as_integer())
        .ok_or_else(|| ConfigError::Parse(format!("taxonomy: '{ctx}' needs an integer `id`")))?;
    u32::try_from(id)
        .map_err(|_| ConfigError::Parse(format!("taxonomy: '{ctx}' id {id} out of range")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[food_and_drink]
id = 1
[food_and_drink.meals]
id = 1001
prompts = ["a plate of food", "a restaurant dish"]
aggregation = "mean"
[food_and_drink.drinks]
id = 1002
prompts = ["a cup of coffee"]
aggregation = "max"
"#;

    #[test]
    fn parses_and_flattens() {
        let t = Taxonomy::parse(SAMPLE).unwrap();
        assert_eq!(t.prompts.len(), 3);
        assert_eq!(t.children.len(), 2);
        let meals = t.children.iter().find(|c| c.id == 1001).unwrap();
        assert_eq!(meals.parent_id, 1);
        assert_eq!(meals.prompt_len, 2);
        assert_eq!(meals.aggregation, Aggregation::Mean);
        let drinks = t.children.iter().find(|c| c.id == 1002).unwrap();
        assert_eq!(drinks.aggregation, Aggregation::Max);
        // "average" is accepted as a synonym for mean.
        assert_eq!(
            Taxonomy::parse(&SAMPLE.replace("\"mean\"", "\"average\""))
                .unwrap()
                .children
                .iter()
                .find(|c| c.id == 1001)
                .unwrap()
                .aggregation,
            Aggregation::Mean
        );
    }

    #[test]
    fn aggregation_defaults_to_mean() {
        let t = Taxonomy::parse("[p]\nid = 1\n[p.c]\nid = 10\nprompts = [\"x\"]\n").unwrap();
        assert_eq!(t.children[0].aggregation, Aggregation::Mean);
    }

    #[test]
    fn a_child_may_set_its_own_threshold() {
        let t = Taxonomy::parse(&SAMPLE.replace(
            "aggregation = \"max\"",
            "aggregation = \"max\"\nthreshold = 0.25",
        ))
        .unwrap();
        let drinks = t.children.iter().find(|c| c.id == 1002).unwrap();
        assert_eq!(drinks.threshold, Some(0.25));
        let meals = t.children.iter().find(|c| c.id == 1001).unwrap();
        assert_eq!(meals.threshold, None);
        assert!(
            Taxonomy::parse(&SAMPLE.replace("id = 1002", "id = 1002\nthreshold = 2.0")).is_err()
        );
        assert!(
            Taxonomy::parse(&SAMPLE.replace("id = 1002", "id = 1002\nthreshold = 0.255")).is_err()
        );
        assert!(
            Taxonomy::parse(&SAMPLE.replace("id = 1002", "id = 1002\nthreshold = \"x\"")).is_err()
        );
    }

    #[test]
    fn rejects_duplicate_child_id() {
        let dup = SAMPLE.replace("id = 1002", "id = 1001");
        assert!(Taxonomy::parse(&dup).is_err());
    }
}
