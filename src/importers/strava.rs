use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use csv::StringRecord;

use crate::domain::activity::ActivityCsvRow;
use crate::domain::export_batch::StravaExportBatch;

const COL_ACTIVITY_ID: &str = "活动 ID";
const COL_ACTIVITY_NAME: &str = "活动名称";
const COL_ACTIVITY_DATE: &str = "活动日期";
const COL_SOURCE_FILE: &str = "文件名";

pub fn discover_batches(inbox_dir: &Path) -> Result<Vec<StravaExportBatch>> {
    if !inbox_dir.exists() {
        return Ok(Vec::new());
    }

    let mut batches = Vec::new();
    for entry in fs::read_dir(inbox_dir).context("failed to read inbox directory")? {
        let entry = entry.context("failed to read inbox entry")?;
        let path = entry.path();

        if !path.is_dir() || is_hidden_path(path.as_path()) {
            continue;
        }

        let csv_path = path.join("activities.csv");
        let activities_dir = path.join("activities");
        if csv_path.is_file() && activities_dir.is_dir() {
            batches.push(StravaExportBatch {
                batch_name: entry.file_name().to_string_lossy().to_string(),
                root: path,
                csv_path,
                activities_dir,
            });
        }
    }

    batches.sort_by(|a, b| a.batch_name.cmp(&b.batch_name));
    Ok(batches)
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

fn header_index(headers: &StringRecord, expected: &str) -> Result<usize> {
    headers
        .iter()
        .position(|header| header == expected)
        .with_context(|| format!("missing required csv column: {expected}"))
}

fn is_hidden_path(path: &Path) -> bool {
    path.file_name()
        .map(|name| name.to_string_lossy().starts_with('.'))
        .unwrap_or(false)
}
