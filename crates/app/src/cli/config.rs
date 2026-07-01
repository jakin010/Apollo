//! `config get|set|remove` over dotted keys, format-preserving. A default
//! `[app]`-only file is created on first `set` if none exists.

use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::Subcommand;

use apollo_config::{edit, load};

#[derive(Subcommand)]
pub enum ConfigCmd {
    /// Print the value at a dotted key (e.g. `models.nsfw.repo`).
    Get {
        key: String,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Set a dotted key to a value, creating tables (and the file) as needed.
    Set {
        key: String,
        value: String,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Remove the value at a dotted key.
    Remove {
        key: String,
        #[arg(long)]
        config: Option<PathBuf>,
    },
}

pub fn run(cmd: ConfigCmd) -> anyhow::Result<()> {
    match cmd {
        ConfigCmd::Get { key, config } => {
            let path = config.unwrap_or_else(load::default_path);
            let doc = edit::load_document(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            match edit::get(&doc, &key) {
                Some(v) => println!("{v}"),
                None => return Err(anyhow!("key '{key}' not set")),
            }
        }
        ConfigCmd::Set { key, value, config } => {
            let path = config.unwrap_or_else(load::default_path);
            edit::create_default_if_missing(&path)
                .with_context(|| format!("creating {}", path.display()))?;
            let mut doc = edit::load_document(&path)?;
            edit::set(&mut doc, &key, &value)?;
            edit::save_document(&path, &doc)
                .with_context(|| format!("writing {}", path.display()))?;
            println!("{key} = {value}");
        }
        ConfigCmd::Remove { key, config } => {
            let path = config.unwrap_or_else(load::default_path);
            let mut doc = edit::load_document(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            if edit::remove(&mut doc, &key)? {
                edit::save_document(&path, &doc)
                    .with_context(|| format!("writing {}", path.display()))?;
                println!("removed {key}");
            } else {
                return Err(anyhow!("key '{key}' not set"));
            }
        }
    }
    Ok(())
}
