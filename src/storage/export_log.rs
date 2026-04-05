use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};

use crate::domain::export_batch::ExportLogEntry;

pub fn ensure_log_file(path: &Path) -> Result<()> {
    if !path.exists() {
        fs::write(path, b"")
            .with_context(|| format!("failed to create log file {}", path.display()))?;
    }
    Ok(())
}

pub fn append(path: &Path, entry: &ExportLogEntry) -> Result<()> {
    ensure_log_file(path)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open export log {}", path.display()))?;

    let line = serde_json::to_string(entry).context("failed to serialize export log entry")?;
    writeln!(file, "{line}")
        .with_context(|| format!("failed to append export log {}", path.display()))?;
    Ok(())
}
