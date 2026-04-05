use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

pub fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create directory {}", path.display()))?;
    Ok(())
}

pub fn infer_output_extension(source_basename: &str) -> Option<&'static str> {
    if source_basename.ends_with(".fit.gz") {
        Some(".fit")
    } else if source_basename.ends_with(".tcx.gz") {
        Some(".tcx")
    } else if source_basename.ends_with(".gpx.gz") {
        Some(".gpx")
    } else {
        None
    }
}

pub fn unique_semantic_path(dir: &Path, sanitized_name: &str, extension: &str) -> Result<PathBuf> {
    let base_name = sanitized_name.trim();
    let mut candidate = dir.join(format!("{base_name}{extension}"));
    if !candidate.exists() {
        return Ok(candidate);
    }

    let mut counter = 1usize;
    loop {
        candidate = dir.join(format!("{base_name}_{counter}{extension}"));
        if !candidate.exists() {
            return Ok(candidate);
        }
        counter += 1;
    }
}

pub fn timestamp_string() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    secs.to_string()
}
