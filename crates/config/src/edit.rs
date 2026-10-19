//! Format-preserving `get` / `set` / `remove` over dotted keys, plus creation of
//! a default `[app]`-only config. Backs the `config` CLI subcommand.

use std::path::Path;

use toml_edit::{DocumentMut, Item, Table, TableLike, Value};

use crate::error::ConfigError;

/// Load a config file as an editable, format-preserving document.
pub fn load_document(path: &Path) -> Result<DocumentMut, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|e| ConfigError::Io(path.to_path_buf(), e))?;
    text.parse::<DocumentMut>()
        .map_err(|e| ConfigError::Edit(e.to_string()))
}

/// Write a document back to disk.
pub fn save_document(path: &Path, doc: &DocumentMut) -> Result<(), ConfigError> {
    std::fs::write(path, doc.to_string()).map_err(|e| ConfigError::Io(path.to_path_buf(), e))
}

/// Create a default config (only `[app]`, with defaults) if the file does not
/// exist. Returns `true` if it was created.
pub fn create_default_if_missing(path: &Path) -> Result<bool, ConfigError> {
    if path.exists() {
        return Ok(false);
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| ConfigError::Io(parent.to_path_buf(), e))?;
    }
    std::fs::write(path, default_app_toml()).map_err(|e| ConfigError::Io(path.to_path_buf(), e))?;
    Ok(true)
}

fn default_app_toml() -> String {
    let app = crate::schema::AppConfig::default();
    let mut s = String::new();
    s.push_str("[app]\n");
    s.push_str(&format!("endpoint       = \"{}\"\n", app.endpoint));
    s.push_str(&format!("port           = {}\n", app.port));
    s.push_str(&format!("max_concurrent = {}\n", app.max_concurrent));
    s.push_str(&format!("idle_timeout   = {}\n", app.idle_timeout));
    s.push_str(&format!("log_level      = \"{}\"\n", app.log_level));
    s
}

/// Get the value at a dotted key (e.g. `models.nsfw.repo`) as TOML text.
pub fn get(doc: &DocumentMut, key: &str) -> Option<String> {
    let mut segs = key.split('.');
    let first = segs.next()?;
    let mut item = doc.as_table().get(first)?;
    for seg in segs {
        item = item.get(seg)?;
    }
    Some(render_item(item))
}

/// Set the value at a dotted key, creating intermediate tables as needed.
/// Keys may lead through inline tables (`early_exit = { threshold = 0.85 }`).
pub fn set(doc: &mut DocumentMut, key: &str, value: &str) -> Result<(), ConfigError> {
    set_value(doc, key, parse_value(value))
}

/// [`set`] with a value already typed (e.g. a float that must stay a float).
pub fn set_value(doc: &mut DocumentMut, key: &str, value: Value) -> Result<(), ConfigError> {
    let segs: Vec<&str> = key.split('.').collect();
    let (last, parents) = segs
        .split_last()
        .ok_or_else(|| ConfigError::EditOp("empty key".into()))?;

    let mut table: &mut dyn TableLike = doc.as_table_mut();
    for &seg in parents {
        if table.get(seg).is_none() {
            table.insert(seg, Item::Table(Table::new()));
        }
        table = table
            .get_mut(seg)
            .and_then(Item::as_table_like_mut)
            .ok_or_else(|| ConfigError::EditOp(format!("'{seg}' is not a table")))?;
    }
    match table.get_mut(last) {
        // Keep the existing value's comments and spacing.
        Some(Item::Value(old)) => {
            let decor = old.decor().clone();
            *old = value;
            *old.decor_mut() = decor;
        }
        _ => {
            table.insert(last, Item::Value(value));
        }
    }
    Ok(())
}

/// Remove the value at a dotted key. Returns `true` if something was removed.
pub fn remove(doc: &mut DocumentMut, key: &str) -> Result<bool, ConfigError> {
    let segs: Vec<&str> = key.split('.').collect();
    let (last, parents) = segs
        .split_last()
        .ok_or_else(|| ConfigError::EditOp("empty key".into()))?;

    let mut table: &mut Table = doc.as_table_mut();
    for &seg in parents {
        match table.get_mut(seg).and_then(Item::as_table_mut) {
            Some(t) => table = t,
            None => return Ok(false),
        }
    }
    Ok(table.remove(last).is_some())
}

/// Render an item: scalars as their TOML text, tables/arrays via debug.
fn render_item(item: &Item) -> String {
    match item.as_value() {
        Some(v) => v.to_string().trim().to_string(),
        None => format!("{item:?}"),
    }
}

/// Interpret a CLI string as a TOML scalar: bool, integer, float, else string.
fn parse_value(s: &str) -> Value {
    if let Ok(b) = s.parse::<bool>() {
        Value::from(b)
    } else if let Ok(i) = s.parse::<i64>() {
        Value::from(i)
    } else if let Ok(f) = s.parse::<f64>() {
        Value::from(f)
    } else {
        Value::from(s)
    }
}
