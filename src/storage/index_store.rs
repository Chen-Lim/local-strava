use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use crate::domain::activity::ActivityRecord;

pub fn ensure_store_file(path: &Path) -> Result<()> {
    if !path.exists() {
        fs::write(path, b"{}\n")
            .with_context(|| format!("failed to create index file {}", path.display()))?;
    }
    Ok(())
}

pub fn load(path: &Path) -> Result<HashMap<String, ActivityRecord>> {
    ensure_store_file(path)?;
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read index file {}", path.display()))?;

    if content.trim().is_empty() {
        return Ok(HashMap::new());
    }

    let index = serde_json::from_str::<HashMap<String, ActivityRecord>>(&content)
        .with_context(|| format!("failed to parse index file {}", path.display()))?;
    Ok(index)
}

pub fn save(path: &Path, index: &HashMap<String, ActivityRecord>) -> Result<()> {
    let json = serde_json::to_string_pretty(index).context("failed to serialize index")?;
    fs::write(path, format!("{json}\n"))
        .with_context(|| format!("failed to write index file {}", path.display()))?;
    Ok(())
}

pub fn is_effectively_empty(path: &Path) -> Result<bool> {
    let index = load(path)?;
    Ok(index.is_empty())
}
