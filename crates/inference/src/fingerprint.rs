//! A model's output fingerprint: what its results depend on, as the key its
//! results are cached under. Two models with the same fingerprint give the same
//! output for the same bytes, so they may share cached results (across
//! restarts, and across instances sharing a database).

use sha2::{Digest, Sha256};

use apollo_config::{ModelConfig, StrategyConfig};
use apollo_domain::Architecture;

use crate::{DeviceKind, ModelFiles};

/// The version of Apollo's own output, part of every fingerprint. Bump it when
/// a code change alters what a successful run outputs for the same bytes and
/// config (preprocessing, scan planning or aggregation, result shapes), so
/// results cached before it are no longer served.
pub const OUTPUT_VERSION: u32 = 1;

/// The fingerprint of `cfg` with its `files` (as the worker loads them), its
/// video `strategy`, on `device`: `fp<OUTPUT_VERSION>:<sha-256 hex>`. Built
/// from the settings that change outputs only; scheduling settings (`timeout`,
/// `priority`, ...), file paths and the strategy's name are left out. `None`
/// when a file it names couldn't be read: the model can't load.
pub fn output_fingerprint(
    cfg: &ModelConfig,
    files: &ModelFiles,
    strategy: Option<&StrategyConfig>,
    device: DeviceKind,
) -> Option<String> {
    // Every field, so a new one can't be added without deciding whether it
    // changes outputs.
    let ModelConfig {
        architecture,
        repo,
        revision,
        enabled: _,
        keep_in_memory: _,
        max_concurrent: _,
        timeout: _,
        max_pending: _,
        priority: _,
        video_strategy: _,
        early_exit,
        labels,
        prompt_template,
        score_threshold,
        max_results,
        taxonomy_file,
        hash_list,
        categories,
        max_distance,
        min_quality,
        match_rotations,
        max_tokens,
        multi_label,
        max_pixels,
        min_confidence,
    } = cfg;
    // A file by its contents, not its path.
    let contents = |path: &Option<String>, text: &Option<std::sync::Arc<str>>| match (path, text) {
        (None, _) => Some(None),
        (Some(_), Some(text)) => Some(Some(sha256_hex(text.as_bytes()))),
        (Some(_), None) => None,
    };
    let taxonomy = contents(taxonomy_file, &files.taxonomy)?;
    let hash_list = contents(hash_list, &files.hash_list)?;
    let mut v = serde_json::json!({
        "architecture": architecture,
        "repo": repo,
        "revision": revision,
        "early_exit": early_exit,
        "labels": labels,
        "prompt_template": prompt_template,
        "score_threshold": score_threshold,
        "max_results": max_results,
        "taxonomy": taxonomy,
        "hash_list": hash_list,
        "categories": categories,
        "max_distance": max_distance,
        "min_quality": min_quality,
        "match_rotations": match_rotations,
        "max_tokens": max_tokens,
        "multi_label": multi_label,
        "max_pixels": max_pixels,
        "min_confidence": min_confidence,
        "strategy": strategy,
        "numerics": numerics(*architecture, device),
    });
    // Unset settings leave no trace, so one added later keeps every
    // fingerprint while it is unset.
    prune(&mut v);
    // serde_json's map is ordered by key, so this is canonical.
    let bytes = serde_json::to_vec(&v).expect("a JSON value serializes");
    Some(format!("fp{OUTPUT_VERSION}:{}", sha256_hex(&bytes)))
}

/// What computes the numbers: the device, and on the CPU its architecture
/// and the speed features compiled in (they move scores in the last digits).
/// `None` for `pdq`, plain integer code whatever it runs on.
fn numerics(architecture: Architecture, device: DeviceKind) -> Option<String> {
    if architecture == Architecture::Pdq {
        return None;
    }
    let DeviceKind::Cpu = device else {
        return Some(device.to_string());
    };
    let mut cpu = format!("cpu-{}", std::env::consts::ARCH);
    for (on, name) in [
        (cfg!(feature = "accelerate"), "accelerate"),
        (cfg!(feature = "avx512"), "avx512"),
        (cfg!(feature = "fast-gelu"), "fast-gelu"),
    ] {
        if on {
            cpu.push('+');
            cpu.push_str(name);
        }
    }
    Some(cpu)
}

/// Drop object members that are null, or empty arrays or objects, at any
/// depth (`false` and `0` stay: they are settings).
fn prune(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            for value in map.values_mut() {
                prune(value);
            }
            map.retain(|_, value| !is_empty(value));
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(prune),
        _ => {}
    }
}

fn is_empty(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Null => true,
        serde_json::Value::Array(items) => items.is_empty(),
        serde_json::Value::Object(map) => map.is_empty(),
        _ => false,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn model(toml: &str) -> ModelConfig {
        apollo_config::load::from_str(&format!("[models.m]\n{toml}"))
            .unwrap()
            .models
            .remove("m")
            .unwrap()
    }

    fn strategy(toml: &str) -> StrategyConfig {
        apollo_config::load::from_str(&format!(
            "[models.m]\narchitecture = \"vit\"\nrepo = \"r\"\n[strategies.s]\n{toml}"
        ))
        .unwrap()
        .strategies
        .remove("s")
        .unwrap()
    }

    const PDQ: &str =
        "architecture = \"pdq\"\nhash_list = \"/a/list.txt\"\ncategories = { 1 = \"known\" }\n";
    const VIT: &str = "architecture = \"vit\"\nrepo = \"org/vit\"\n";
    const SAMPLING: &str = "[[strategies.s.sampling]]\nstep = 1\nmethod = \"fps\"\nfps = 1.0\n";

    fn list(text: &str) -> ModelFiles {
        ModelFiles {
            taxonomy: None,
            hash_list: Some(Arc::from(text)),
        }
    }

    fn fp(cfg: &ModelConfig, files: &ModelFiles, s: Option<&StrategyConfig>) -> String {
        output_fingerprint(cfg, files, s, DeviceKind::Cpu).unwrap()
    }

    #[test]
    fn a_fingerprint_is_stable() {
        // pdq has no numerics, so this holds in every build. A change here
        // changes every model's cache key: bump OUTPUT_VERSION only when
        // outputs change, and expect this value to move with it.
        assert_eq!(
            fp(&model(PDQ), &list("ab 1\n"), None),
            "fp1:85f5dc07819903bd61c25fb511605e2da0955254859ef0af3864585e87c70360"
        );
    }

    #[test]
    fn output_settings_change_the_fingerprint() {
        let base_vit = fp(&model(VIT), &ModelFiles::default(), None);
        for change in [
            "architecture = \"siglip\"\nrepo = \"org/vit\"\n",
            "architecture = \"vit\"\nrepo = \"org/other\"\n",
            "revision = \"0123abc\"\n",
            "labels = [\"a\"]\n",
            "prompt_template = \"a photo of {}\"\n",
            "score_threshold = 0.4\n",
            "max_results = 3\n",
            "max_tokens = 64\n",
            "multi_label = false\n",
            "max_pixels = 1000\n",
            "min_confidence = 0.3\n",
            "early_exit = { labels = [1] }\n",
            "early_exit = { labels = [1], threshold = 0.9 }\n",
        ] {
            let toml = if change.starts_with("architecture") {
                change.to_string()
            } else {
                format!("{VIT}{change}")
            };
            assert_ne!(
                fp(&model(&toml), &ModelFiles::default(), None),
                base_vit,
                "{change}"
            );
        }
        // Each early_exit field on its own.
        assert_ne!(
            fp(
                &model(&format!("{VIT}early_exit = {{ labels = [1] }}\n")),
                &ModelFiles::default(),
                None
            ),
            fp(
                &model(&format!("{VIT}early_exit = {{ labels = [2] }}\n")),
                &ModelFiles::default(),
                None
            ),
        );

        let files = list("ab 1\n");
        let base_pdq = fp(&model(PDQ), &files, None);
        for change in [
            "max_distance = 10\n",
            "min_quality = 10\n",
            "match_rotations = true\n",
            "match_rotations = false\n",
        ] {
            assert_ne!(
                fp(&model(&format!("{PDQ}{change}")), &files, None),
                base_pdq,
                "{change}"
            );
        }
        // An explicit default isn't taken for an unset one.
        assert_ne!(
            fp(
                &model(&format!("{PDQ}match_rotations = true\n")),
                &files,
                None
            ),
            fp(
                &model(&format!("{PDQ}match_rotations = false\n")),
                &files,
                None
            ),
        );
        let other_categories = PDQ.replace("1 = \"known\"", "1 = \"other\"");
        assert_ne!(fp(&model(&other_categories), &files, None), base_pdq);
        // The files' contents.
        assert_ne!(fp(&model(PDQ), &list("ab 2\n"), None), base_pdq);
        let siglip = "architecture = \"siglip\"\nrepo = \"org/s\"\ntaxonomy_file = \"/t.toml\"\n";
        let taxonomy = |text: &str| ModelFiles {
            taxonomy: Some(Arc::from(text)),
            hash_list: None,
        };
        assert_ne!(
            fp(&model(siglip), &taxonomy("a"), None),
            fp(&model(siglip), &taxonomy("b"), None)
        );

        // The strategy's contents.
        let s = strategy(SAMPLING);
        let with = fp(&model(VIT), &ModelFiles::default(), Some(&s));
        assert_ne!(with, base_vit);
        for change in [
            "aggregation = \"max\"\n",
            "early_exit = true\n",
            "confirm = 2\n",
            "confirm_window = 5.0\n",
            "max_frames = 10\n",
        ] {
            let s = strategy(&format!("{change}{SAMPLING}"));
            assert_ne!(
                fp(&model(VIT), &ModelFiles::default(), Some(&s)),
                with,
                "{change}"
            );
        }
        for sampling in [
            SAMPLING.replace("fps = 1.0", "fps = 2.0"),
            SAMPLING.replace("fps = 1.0", "count = 8"),
            format!("{SAMPLING}random = true\n"),
        ] {
            let s = strategy(&sampling);
            assert_ne!(
                fp(&model(VIT), &ModelFiles::default(), Some(&s)),
                with,
                "{sampling}"
            );
        }

        // The device, for a model the device computes.
        assert_ne!(
            output_fingerprint(&model(VIT), &ModelFiles::default(), None, DeviceKind::Cuda),
            Some(base_vit)
        );
    }

    #[test]
    fn other_settings_keep_the_fingerprint() {
        let base = fp(&model(VIT), &ModelFiles::default(), None);
        for change in [
            "enabled = true\n",
            "keep_in_memory = true\n",
            "max_concurrent = 7\n",
            "timeout = 5\n",
            "max_pending = 3\n",
            "priority = 9\n",
            "video_strategy = \"s\"\n",
        ] {
            assert_eq!(
                fp(
                    &model(&format!("{VIT}{change}")),
                    &ModelFiles::default(),
                    None
                ),
                base,
                "{change}"
            );
        }
        // A file at another path, with the same contents.
        let files = list("ab 1\n");
        assert_eq!(
            fp(
                &model(&PDQ.replace("/a/list.txt", "/b/other.txt")),
                &files,
                None
            ),
            fp(&model(PDQ), &files, None)
        );
        // pdq computes the same on any device.
        assert_eq!(
            output_fingerprint(&model(PDQ), &files, None, DeviceKind::Cuda),
            output_fingerprint(&model(PDQ), &files, None, DeviceKind::Cpu)
        );
    }

    #[test]
    fn an_unread_file_gives_no_fingerprint() {
        assert_eq!(
            output_fingerprint(&model(PDQ), &ModelFiles::default(), None, DeviceKind::Cpu),
            None
        );
    }
}
