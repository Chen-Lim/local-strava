use std::path::Path;

use anyhow::Result;
use rayon::prelude::*;

use crate::db::duckdb_store::DuckActivityStore;
use crate::db::models::{ActivityRecord, SyncRun};
use crate::db::sqlite::SqliteActivityStore;
use crate::domain::activity::ActivityCsvRow;
use crate::domain::export_batch::{ExportLogEntry, StravaExportBatch};
use crate::importers::strava;
use crate::services::{decompress, fit_ingest, layout, naming};
use crate::storage::export_log;
use crate::utils::fs;

pub fn run_sync(project_root: &Path, batch_name: Option<&str>) -> Result<()> {
    layout::warn_if_legacy_state(project_root);
    layout::ensure_sync_layout(project_root)?;
    let db_path = layout::sqlite_path(project_root);
    let mut store = SqliteActivityStore::open(&db_path)?;
    let batches = resolve_batches(project_root, batch_name)?;

    if batches.is_empty() {
        println!(
            "No valid Strava export batches (export_*.zip or export_*/) found in {}",
            project_root.display()
        );
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
            &layout::exports_log_path(project_root),
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

    // Phase 2: parse newly-imported FIT files into DuckDB. Runs after every
    // batch so a partial sync still leaves the analytical DB consistent for
    // whatever made it through. Failures are logged per activity, never abort.
    run_fit_ingest_pass(project_root, &mut store)?;

    store.complete_sync_run(&sync_run.run_id, &fs::timestamp_rfc3339())?;
    Ok(())
}

/// Walk every `pending_fit_ingest()` activity and persist it to the DuckDB
/// analytical store. Logs per-activity failures to stderr but always returns
/// `Ok(())` — fit ingestion is opportunistic and shouldn't fail the sync.
pub fn run_fit_ingest_pass(project_root: &Path, store: &mut SqliteActivityStore) -> Result<()> {
    let pending = store.pending_fit_ingest()?;
    if pending.is_empty() {
        return Ok(());
    }

    let duck_path = layout::duckdb_path(project_root);
    let mut duck = DuckActivityStore::open(&duck_path)?;
    let conn = duck.conn_mut();

    let total = pending.len();
    let mut ok = 0usize;
    let mut failed = 0usize;
    println!("FIT ingest: {total} activities pending");

    for record in pending {
        let fit_path = project_root.join(&record.library_path);
        match fit_ingest::ingest_activity(conn, &record.activity_id, &fit_path) {
            Ok(()) => {
                store.mark_fit_ingested(&record.activity_id, &fs::timestamp_rfc3339())?;
                ok += 1;
            }
            Err(err) => {
                failed += 1;
                eprintln!(
                    "FIT ingest failed for activity {} ({}): {err:#}",
                    record.activity_id,
                    fit_path.display()
                );
            }
        }
    }

    println!("FIT ingest: ok={ok} failed={failed} total={total}");
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
    use crate::importers::strava::InboxEntry;

    let staging_dir = layout::staging_dir(project_root);
    let entries = strava::discover_exports(project_root)?;

    let mut batches = Vec::new();
    for entry in entries {
        match entry {
            InboxEntry::Dir(batch) => batches.push(batch),
            InboxEntry::Zip {
                batch_name,
                zip_path,
            } => {
                let dest = staging_dir.join(&batch_name);
                decompress::unzip_strava_archive(&zip_path, &dest)?;
                batches.push(StravaExportBatch {
                    batch_name,
                    root: dest.clone(),
                    csv_path: dest.join("activities.csv"),
                    activities_dir: dest.join("activities"),
                });
            }
        }
    }

    if let Some(requested) = requested_batch {
        batches.retain(|b| b.batch_name == requested);
    }
    batches.sort_by(|a, b| a.batch_name.cmp(&b.batch_name));
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

            let dest_dir = layout::library_dir_for_extension(project_root, extension);
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

fn relative_to_root(project_root: &Path, path: &Path) -> String {
    path.strip_prefix(project_root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}
