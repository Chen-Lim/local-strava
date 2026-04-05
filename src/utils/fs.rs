use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;

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

pub fn deterministic_activity_path(
    dir: &Path,
    activity_id: &str,
    sanitized_name: &str,
    extension: &str,
) -> PathBuf {
    let file_stem = if sanitized_name.trim().is_empty() {
        format!("{activity_id}__untitled")
    } else {
        format!("{activity_id}__{}", sanitized_name.trim())
    };

    dir.join(format!("{file_stem}{extension}"))
}

pub fn copy_file_overwrite(src: &Path, dest: &Path) -> Result<()> {
    if dest.exists() {
        fs::remove_file(dest)
            .with_context(|| format!("failed to remove existing file {}", dest.display()))?;
    }

    fs::copy(src, dest).with_context(|| {
        format!(
            "failed to copy file from {} to {}",
            src.display(),
            dest.display()
        )
    })?;
    Ok(())
}

pub fn timestamp_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
