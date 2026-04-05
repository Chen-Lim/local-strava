use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use rayon::prelude::*;

use crate::domain::activity::{ActivityRecord, ActivityStatus};
use crate::domain::export_batch::{ExportLogEntry, StravaExportBatch};
use crate::importers::strava;
use crate::services::{consistency, decompress, incremental, naming};
use crate::storage::{export_log, index_store};
use crate::utils::fs;

pub fn run_sync(project_root: &Path, batch_name: Option<&str>) -> Result<()> {
    consistency::ensure_project_layout(project_root)?;

    let index_path = project_root.join("state/activity_index.json");
    if index_store::is_effectively_empty(&index_path)?
        && project_root.join("activities.csv").is_file()
    {
        consistency::bootstrap_legacy_index(project_root)?;
    }

    let mut index = index_store::load(&index_path)?;
    let batches = resolve_batches(project_root, batch_name)?;

    if batches.is_empty() {
        println!("No valid Strava export batches found in inbox/");
        return Ok(());
    }

    for batch in batches {
        // Scheme A: Parallel processing of files, separate aggregation phase for index updates
        let (summary, new_records) = process_batch(project_root, &batch, &index)?;

        // Unify and commit index updates sequentially
        for record in new_records {
            index.insert(record.activity_id.clone(), record);
        }

        let batch_name = summary.batch_name.clone();
        export_log::append(
            &project_root.join("state/exports.jsonl"),
            &ExportLogEntry {
                batch_name,
                batch_path: summary.batch_path,
                processed_at: fs::timestamp_string(),
                total_rows: summary.total_rows,
                new_activities: summary.new_activities,
                skipped_activities: summary.skipped_activities,
                failed_activities: summary.failed_activities,
            },
        )?;

        index_store::save(&index_path, &index)?;
        println!(
            "Batch {}: total={}, new={}, skipped={}, failed={}",
            summary.batch_name,
            summary.total_rows,
            summary.new_activities,
            summary.skipped_activities,
            summary.failed_activities
        );
    }

    Ok(())
}

struct BatchSummary {
    batch_name: String,
    batch_path: String,
    total_rows: usize,
    new_activities: usize,
    skipped_activities: usize,
    failed_activities: usize,
}

fn resolve_batches(
    project_root: &Path,
    requested_batch: Option<&str>,
) -> Result<Vec<StravaExportBatch>> {
    let mut batches = strava::discover_batches(&project_root.join("inbox"))?;
    if let Some(requested) = requested_batch {
        batches.retain(|batch| batch.batch_name == requested);
    }
    Ok(batches)
}

fn process_batch(
    project_root: &Path,
    batch: &StravaExportBatch,
    index: &HashMap<String, ActivityRecord>,
) -> Result<(BatchSummary, Vec<ActivityRecord>)> {
    let rows = strava::read_activities(&batch.csv_path)?;
    let pending = incremental::pending_rows(&rows, index);
    let total_rows = rows.len();
    let skipped_activities = total_rows.saturating_sub(pending.len());

    // Process all pending rows in parallel using rayon
    let results: Vec<Result<Option<ActivityRecord>, String>> = pending
        .into_par_iter()
        .map(|row| {
            let src_path = batch.activities_dir.join(&row.source_basename);
            if !src_path.is_file() {
                return Err(format!(
                    "Missing source file for activity {}: {}",
                    row.activity_id,
                    src_path.display()
                ));
            }

            let extension = match fs::infer_output_extension(&row.source_basename) {
                Some(ext) => ext,
                None => return Err(format!("Unsupported export file: {}", row.source_basename)),
            };

            let dest_dir = library_dir_for_extension(project_root, extension);
            if let Err(e) = fs::ensure_dir(&dest_dir) {
                return Err(format!(
                    "Failed to create directory {}: {}",
                    dest_dir.display(),
                    e
                ));
            }

            let sanitized_name = naming::sanitize_filename(&row.activity_name);
            let dest_path = match fs::unique_semantic_path(&dest_dir, &sanitized_name, extension) {
                Ok(path) => path,
                Err(e) => return Err(format!("Failed to get unique path: {}", e)),
            };

            match decompress::decompress_gzip(&src_path, &dest_path) {
                Ok(()) => {
                    if let Err(e) = copy_to_new_folder(project_root, extension, &dest_path) {
                        return Err(format!("Failed to copy to new folder: {}", e));
                    }

                    Ok(Some(ActivityRecord {
                        activity_id: row.activity_id.clone(),
                        activity_name_raw: row.activity_name.clone(),
                        activity_name_sanitized: sanitized_name,
                        activity_date: row.activity_date.clone(),
                        source_batch: batch.batch_name.clone(),
                        source_file: row.source_file.clone(),
                        source_basename: row.source_basename.clone(),
                        file_format: extension.trim_start_matches('.').to_string(),
                        library_path: Some(relative_to_root(project_root, &dest_path)),
                        processed_at: fs::timestamp_string(),
                        status: ActivityStatus::Ready,
                    }))
                }
                Err(err) => Err(format!(
                    "Failed to process activity {} from {}: {err:#}",
                    row.activity_id, row.source_basename
                )),
            }
        })
        .collect();

    let mut new_activities = 0usize;
    let mut failed_activities = 0usize;
    let mut new_records = Vec::new();

    for res in results {
        match res {
            Ok(Some(record)) => {
                new_activities += 1;
                new_records.push(record);
            }
            Ok(None) => {}
            Err(err) => {
                failed_activities += 1;
                println!("{}", err);
            }
        }
    }

    let summary = BatchSummary {
        batch_name: batch.batch_name.clone(),
        batch_path: relative_to_root(project_root, &batch.root),
        total_rows,
        new_activities,
        skipped_activities,
        failed_activities,
    };

    Ok((summary, new_records))
}

fn library_dir_for_extension(project_root: &Path, extension: &str) -> PathBuf {
    match extension {
        ".fit" => project_root.join("library/activities/fit"),
        ".tcx" => project_root.join("library/activities/tcx"),
        ".gpx" => project_root.join("library/activities/gpx"),
        _ => project_root.join("library/activities"),
    }
}

fn relative_to_root(project_root: &Path, path: &Path) -> String {
    path.strip_prefix(project_root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

fn copy_to_new_folder(project_root: &Path, extension: &str, source_path: &Path) -> Result<()> {
    let new_dir = project_root.join("new");
    fs::ensure_dir(&new_dir)?;

    let file_name = match source_path.file_name() {
        Some(name) => name,
        None => return Ok(()),
    };

    let dest_path = new_dir.join(file_name);
    // Be careful with concurrency here. Ignore errors if it's not a file etc.
    // If it's already there and being written, we don't strictly care as long as we don't panic.
    if dest_path.exists() {
        let _ = std::fs::remove_file(&dest_path);
    }

    if extension == ".fit" {
        let _ = std::fs::copy(source_path, &dest_path);
    }

    Ok(())
}
