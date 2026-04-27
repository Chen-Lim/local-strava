use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::storage::export_log;
use crate::utils::fs;

pub fn sqlite_path(cwd: &Path) -> PathBuf {
    cwd.join("strata.sqlite")
}

pub fn duckdb_path(cwd: &Path) -> PathBuf {
    cwd.join("strata.duckdb")
}

pub fn exports_log_path(cwd: &Path) -> PathBuf {
    cwd.join(".strata/exports.jsonl")
}

pub fn library_dir_for_extension(cwd: &Path, extension: &str) -> PathBuf {
    match extension {
        ".fit" => cwd.join("library/activities/fit"),
        ".tcx" => cwd.join("library/activities/tcx"),
        ".gpx" => cwd.join("library/activities/gpx"),
        _ => cwd.join("library/activities"),
    }
}

pub fn staging_dir(cwd: &Path) -> PathBuf {
    cwd.join("workspace/staging")
}

pub fn new_dir(cwd: &Path) -> PathBuf {
    cwd.join("new")
}

/// Create everything `sync` / `reingest` needs. Idempotent.
pub fn ensure_sync_layout(cwd: &Path) -> Result<()> {
    for dir in [
        cwd.join("library/activities/fit"),
        cwd.join("library/activities/tcx"),
        cwd.join("library/activities/gpx"),
        cwd.join("archive"),
        cwd.join("workspace/staging"),
        cwd.join("workspace/failed"),
        cwd.join("workspace/tmp"),
        cwd.join(".strata"),
    ] {
        fs::ensure_dir(&dir)?;
    }
    export_log::ensure_log_file(&exports_log_path(cwd))?;
    Ok(())
}

/// Create only the `new/` output directory for `export-new`.
pub fn ensure_export_layout(cwd: &Path) -> Result<()> {
    fs::ensure_dir(&new_dir(cwd))
}

/// Warn (once) if a legacy `state/` directory from the old layout is present.
pub fn warn_if_legacy_state(cwd: &Path) {
    let legacy = cwd.join("state");
    if legacy.is_dir() {
        eprintln!(
            "warning: found legacy state/ directory in {} — strata now uses strata.sqlite/strata.duckdb in the cwd. Please move them up one level manually.",
            cwd.display()
        );
    }
}
