//! `hash`: print the PDQ hashes of images, to build `pdq` block lists.
//!
//! With `--category`, each line is a ready list entry (`<hash> <category>`)
//! with the file name and quality in a trailing comment, so the output can be
//! appended to a `hash_list` as it is.
//!
//! Images are hashed as stored, without applying their EXIF orientation,
//! which is what Meta's PDQ tools do, so the hashes match theirs. A `pdq`
//! model compares rotations too (`match_rotations`), so a list made this way
//! still matches photos the server decodes upright.

use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use clap::Args;

#[derive(Args)]
pub struct HashArgs {
    /// Images, or directories to hash every image in (recursively).
    #[arg(required = true)]
    paths: Vec<PathBuf>,
    /// Print list entries for this category id: `<hash> <category>  # file
    /// (quality N)`.
    #[arg(long)]
    category: Option<u32>,
    /// Leave out images whose hash quality (0–100) is below this; they would
    /// match unreliably.
    #[arg(long, default_value_t = apollo_inference::PDQ_DEFAULT_MIN_QUALITY)]
    min_quality: u32,
}

const IMAGE_EXTENSIONS: &[&str] = &["jpg", "jpeg", "png", "webp", "gif", "bmp", "tif", "tiff"];

pub fn run(args: HashArgs) -> anyhow::Result<()> {
    let mut files = Vec::new();
    for path in &args.paths {
        if path.is_dir() {
            collect(path, &mut files)?;
        } else if path.is_file() {
            files.push(path.clone());
        } else {
            return Err(anyhow!("{} does not exist", path.display()));
        }
    }
    let (mut hashed, mut skipped) = (0, 0);
    for file in files {
        let bytes = std::fs::read(&file).with_context(|| format!("reading {}", file.display()))?;
        let image = match apollo_media::decode_image_as_stored(&bytes, None) {
            Ok(img) => img,
            Err(e) => {
                eprintln!("skipped {}: {e}", file.display());
                skipped += 1;
                continue;
            }
        };
        let (hash, quality) = apollo_inference::pdq_hash(&image);
        if quality < args.min_quality {
            eprintln!(
                "skipped {}: quality {quality} is below {} (a flat or blank image matches unreliably)",
                file.display(),
                args.min_quality
            );
            skipped += 1;
            continue;
        }
        println!("{}", line(&hash.to_string(), quality, &file, args.category));
        hashed += 1;
    }
    if skipped > 0 {
        eprintln!("hashed {hashed} images, skipped {skipped}");
    }
    Ok(())
}

fn line(hash: &str, quality: u32, file: &Path, category: Option<u32>) -> String {
    let name = file.file_name().map_or_else(
        || file.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    match category {
        Some(c) => format!("{hash} {c}  # {name} (quality {quality})"),
        None => format!("{hash}  quality {quality}  {}", file.display()),
    }
}

/// Every image under `dir`, sorted.
fn collect(dir: &Path, out: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect(&path, out)?;
        } else if path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| IMAGE_EXTENSIONS.contains(&e.to_ascii_lowercase().as_str()))
        {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_are_list_entries_with_a_category() {
        let h = "a".repeat(64);
        let entry = line(&h, 87, Path::new("/x/cat.jpg"), Some(3));
        assert_eq!(entry, format!("{h} 3  # cat.jpg (quality 87)"));
        let list = apollo_inference::parse_pdq_hash_list(&entry).unwrap();
        assert_eq!(list[0].category, 3);
        assert_eq!(
            line(&h, 87, Path::new("/x/cat.jpg"), None),
            format!("{h}  quality 87  /x/cat.jpg")
        );
    }
}
