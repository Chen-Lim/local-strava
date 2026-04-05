use std::path::{Path, PathBuf};

use anyhow::Result;
use rayon::prelude::*;

use crate::db::models::{ActivityRecord, SyncRun};
use crate::db::sqlite::SqliteActivityStore;
use crate::domain::activity::ActivityCsvRow;
use crate::domain::export_batch::{ExportLogEntry, StravaExportBatch};
use crate::importers::strava;
use crate::services::{consistency, decompress, naming};
use crate::storage::export_log;
use crate::utils::fs;

pub fn run_sync(project_root: &Path, batch_name: Option<&str>) -> Result<()> {
    consistency::ensure_project_layout(project_root)?;
    let db_path = project_root.join("state/strava.db");
    let mut store = SqliteActivityStore::open(&db_path)?;
    let batches = resolve_batches(project_root, batch_name)?;

    if batches.is_empty() {
        println!("No valid Strava export batches found in inbox/");
        return Ok(());
    }

    let sync_run = SyncRun {
        run_id: format!("sync_{}", fs::timestamp_rfc3339().replace(':', "-")),
        started_at: fs::timestamp_rfc3339(),
        completed_at: None,
    };
    store.begin_sync_run(&sync_run)?;

    for batch in batches {
        let (summary, new_records) = process_batch(project_root, &batch, &store, &sync_run.run_id)?;
        let inserted = store.insert_activities(&new_records)?;

        let batch_name = summary.batch_name.clone();
        export_log::append(
            &project_root.join("state/exports.jsonl"),
            &ExportLogEntry {
                run_id: sync_run.run_id.clone(),
                batch_name,
                batch_path: summary.batch_path,
                processed_at: fs::timestamp_rfc3339(),
                total_rows: summary.total_rows,
                new_activities: inserted,
                skipped_activities: summary.skipped_activities,
                failed_activities: summary.failed_activities,
            },
        )?;
        println!(
            "Batch {}: total={}, new={}, skipped={}, failed={}",
            summary.batch_name,
            summary.total_rows,
            inserted,
            summary.skipped_activities,
            summary.failed_activities
        );
    }

    store.complete_sync_run(&sync_run.run_id, &fs::timestamp_rfc3339())?;
    Ok(())
}

struct BatchSummary {
    batch_name: String,
    batch_path: String,
    total_rows: usize,
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
    store: &SqliteActivityStore,
    run_id: &str,
) -> Result<(BatchSummary, Vec<ActivityRecord>)> {
    let rows = strava::read_activities(&batch.csv_path)?;
    let total_rows = rows.len();
    let existing_ids = store.existing_ids(rows.iter().map(|row| row.activity_id.as_str()))?;
    let pending: Vec<ActivityCsvRow> = rows
        .into_iter()
        .filter(|row| !existing_ids.contains(&row.activity_id))
        .collect();
    let skipped_activities = total_rows.saturating_sub(pending.len());

    ensure_library_dirs(project_root)?;

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
            let sanitized_name = naming::sanitize_filename(&row.activity_name);
            let dest_path = fs::deterministic_activity_path(
                &dest_dir,
                &row.activity_id,
                &sanitized_name,
                extension,
            );

            match decompress::decompress_gzip(&src_path, &dest_path) {
                Ok(()) => Ok(Some(ActivityRecord {
                    activity_id: row.activity_id.clone(),
                    activity_name_raw: row.activity_name.clone(),
                    activity_name_sanitized: if sanitized_name.trim().is_empty() {
                        "untitled".to_string()
                    } else {
                        sanitized_name
                    },
                    activity_date: row.activity_date.clone(),
                    source_batch: batch.batch_name.clone(),
                    source_file: row.source_file.clone(),
                    source_basename: row.source_basename.clone(),
                    file_format: extension.trim_start_matches('.').to_string(),
                    library_path: relative_to_root(project_root, &dest_path),
                    import_run_id: run_id.to_string(),
                    imported_at: fs::timestamp_rfc3339(),
                })),
                Err(err) => Err(format!(
                    "Failed to process activity {} from {}: {err:#}",
                    row.activity_id, row.source_basename
                )),
            }
        })
        .collect();

    let mut failed_activities = 0usize;
    let mut new_records = Vec::new();

    for res in results {
        match res {
            Ok(Some(record)) => {
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
        skipped_activities,
        failed_activities,
    };

    Ok((summary, new_records))
}

fn ensure_library_dirs(project_root: &Path) -> Result<()> {
    for dir in [
        project_root.join("library/activities/fit"),
        project_root.join("library/activities/tcx"),
        project_root.join("library/activities/gpx"),
    ] {
        fs::ensure_dir(&dir)?;
    }
    Ok(())
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
