use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use csv::StringRecord;

use crate::domain::activity::ActivityCsvRow;
use crate::domain::export_batch::StravaExportBatch;

const COL_ACTIVITY_ID: &[&str] = &["活动 ID"];
const COL_ACTIVITY_NAME: &[&str] = &["活动名称"];
const COL_ACTIVITY_DATE: &[&str] = &["活动日期"];
const COL_SOURCE_FILE: &[&str] = &["文件名"];

/// An entry discovered in the inbox directory.
#[derive(Debug)]
pub enum InboxEntry {
    Dir(StravaExportBatch),
    Zip {
        batch_name: String,
        zip_path: PathBuf,
    },
}

impl InboxEntry {
    pub fn batch_name(&self) -> &str {
        match self {
            InboxEntry::Dir(batch) => &batch.batch_name,
            InboxEntry::Zip { batch_name, .. } => batch_name,
        }
    }
}

/// Discover all valid inbox entries (both directories and zip archives).
///
/// When the same `batch_name` exists as both a directory and a zip, the
/// directory takes priority (the zip is ignored with an info message).
pub fn discover_inbox(inbox_dir: &Path) -> Result<Vec<InboxEntry>> {
    if !inbox_dir.exists() {
        return Ok(Vec::new());
    }

    let dir_entries = discover_dir_entries(inbox_dir)?;
    let zip_entries = discover_zip_entries(inbox_dir)?;

    // Collect dir batch names for conflict resolution (owned to avoid borrow conflict).
    let dir_names: HashSet<String> = dir_entries
        .iter()
        .map(|e| e.batch_name().to_string())
        .collect();

    let mut entries: Vec<InboxEntry> = dir_entries;

    for zip_entry in zip_entries {
        let name = zip_entry.batch_name().to_string();
        if dir_names.contains(name.as_str()) {
            eprintln!(
                "info: ignoring zip for batch '{}' — directory entry takes priority",
                name
            );
        } else {
            entries.push(zip_entry);
        }
    }

    entries.sort_by(|a, b| a.batch_name().cmp(b.batch_name()));
    Ok(entries)
}

/// Backward-compatible: discover only directory-based batches.
#[allow(dead_code)]
pub fn discover_batches(inbox_dir: &Path) -> Result<Vec<StravaExportBatch>> {
    let entries = discover_inbox(inbox_dir)?;
    let batches = entries
        .into_iter()
        .filter_map(|e| match e {
            InboxEntry::Dir(batch) => Some(batch),
            _ => None,
        })
        .collect();
    Ok(batches)
}

/// Scan for directory-based Strava export batches.
fn discover_dir_entries(inbox_dir: &Path) -> Result<Vec<InboxEntry>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(inbox_dir).context("failed to read inbox directory")? {
        let entry = entry.context("failed to read inbox entry")?;
        let path = entry.path();

        if !path.is_dir() || is_hidden_path(path.as_path()) {
            continue;
        }

        let csv_path = path.join("activities.csv");
        let activities_dir = path.join("activities");
        if csv_path.is_file() && activities_dir.is_dir() {
            entries.push(InboxEntry::Dir(StravaExportBatch {
                batch_name: entry.file_name().to_string_lossy().to_string(),
                root: path,
                csv_path,
                activities_dir,
            }));
        }
    }
    Ok(entries)
}

/// Scan for zip-based Strava export archives with lightweight validation.
fn discover_zip_entries(inbox_dir: &Path) -> Result<Vec<InboxEntry>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(inbox_dir).context("failed to read inbox directory")? {
        let entry = entry.context("failed to read inbox entry")?;
        let path = entry.path();

        if !path.is_file() || is_hidden_path(path.as_path()) {
            continue;
        }

        let name_lower = entry
            .file_name()
            .to_string_lossy()
            .to_lowercase();
        if !name_lower.ends_with(".zip") {
            continue;
        }

        let batch_name = Path::new(&entry.file_name())
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        // Lightweight validation: open the zip and check for required entries.
        match validate_strava_zip(&path) {
            Ok(true) => {
                entries.push(InboxEntry::Zip {
                    batch_name,
                    zip_path: path,
                });
            }
            Ok(false) => {
                eprintln!(
                    "warn: skipping {}: missing activities.csv or activities/ in zip",
                    path.display()
                );
            }
            Err(err) => {
                eprintln!(
                    "warn: skipping {}: failed to read zip: {err:#}",
                    path.display()
                );
            }
        }
    }
    Ok(entries)
}

/// Validate that a zip archive contains the minimum required Strava export structure:
/// - `activities.csv` (or `*/activities.csv` for wrapped archives)
/// - At least one entry under `activities/` (or `*/activities/`)
fn validate_strava_zip(zip_path: &Path) -> Result<bool> {
    let file = fs::File::open(zip_path)
        .with_context(|| format!("failed to open zip {}", zip_path.display()))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("failed to read zip {}", zip_path.display()))?;

    let mut has_csv = false;
    let mut has_activities = false;

    for i in 0..archive.len() {
        let entry = archive.by_index_raw(i)?;
        let name = entry.name();

        // Direct: activities.csv or wrapped: something/activities.csv
        if name == "activities.csv" || name.ends_with("/activities.csv") {
            // For wrapped case, ensure it's at most one level deep.
            let parts: Vec<&str> = name.split('/').collect();
            if parts.len() <= 2 {
                has_csv = true;
            }
        }

        // Direct: activities/... or wrapped: something/activities/...
        if name.starts_with("activities/") || name.contains("/activities/") {
            has_activities = true;
        }

        if has_csv && has_activities {
            return Ok(true);
        }
    }

    Ok(has_csv && has_activities)
}

pub fn read_activities(csv_path: &Path) -> Result<Vec<ActivityCsvRow>> {
    let mut reader = csv::Reader::from_path(csv_path)
        .with_context(|| format!("failed to open csv file {}", csv_path.display()))?;

    let headers = reader
        .headers()
        .context("failed to read csv headers")?
        .clone();
    let id_idx = header_index(&headers, COL_ACTIVITY_ID)?;
    let name_idx = header_index(&headers, COL_ACTIVITY_NAME)?;
    let date_idx = header_index(&headers, COL_ACTIVITY_DATE)?;
    let file_idx = header_index(&headers, COL_SOURCE_FILE)?;

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record.context("failed to read csv record")?;
        let activity_id = record.get(id_idx).unwrap_or("").trim().to_string();
        let activity_name = record.get(name_idx).unwrap_or("").trim().to_string();
        let activity_date = record.get(date_idx).unwrap_or("").trim().to_string();
        let source_file = record.get(file_idx).unwrap_or("").trim().to_string();

        if activity_id.is_empty() || activity_name.is_empty() || source_file.is_empty() {
            continue;
        }

        let source_basename = Path::new(&source_file)
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_default();

        rows.push(ActivityCsvRow {
            activity_id,
            activity_name,
            activity_date,
            source_file,
            source_basename,
        });
    }

    Ok(rows)
}

fn header_index(headers: &StringRecord, expected_aliases: &[&str]) -> Result<usize> {
    headers
        .iter()
        .position(|header| expected_aliases.iter().any(|alias| header == *alias))
        .with_context(|| {
            format!(
                "missing required csv column: {}",
                expected_aliases.join(", ")
            )
        })
}

fn is_hidden_path(path: &Path) -> bool {
    path.file_name()
        .map(|name| name.to_string_lossy().starts_with('.'))
        .unwrap_or(false)
}
