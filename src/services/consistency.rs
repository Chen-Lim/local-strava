use std::path::Path;

use anyhow::{Context, Result};

use crate::domain::activity::{ActivityRecord, ActivityStatus};
use crate::importers::strava;
use crate::services::naming;
use crate::storage::{export_log, index_store};
use crate::utils::fs;

pub fn ensure_project_layout(project_root: &Path) -> Result<()> {
    let dirs = [
        project_root.join("inbox"),
        project_root.join("archive"),
        project_root.join("new"),
        project_root.join("library"),
        project_root.join("library/activities"),
        project_root.join("library/activities/fit"),
        project_root.join("library/activities/tcx"),
        project_root.join("library/activities/gpx"),
        project_root.join("state"),
        project_root.join("state/logs"),
        project_root.join("workspace"),
        project_root.join("workspace/staging"),
        project_root.join("workspace/failed"),
        project_root.join("workspace/tmp"),
    ];

    for dir in dirs {
        fs::ensure_dir(&dir)?;
    }

    index_store::ensure_store_file(&project_root.join("state/activity_index.json"))?;
    export_log::ensure_log_file(&project_root.join("state/exports.jsonl"))?;
    Ok(())
}

pub fn bootstrap_legacy_index(project_root: &Path) -> Result<()> {
    ensure_project_layout(project_root)?;

    let index_path = project_root.join("state/activity_index.json");
    let legacy_csv_path = project_root.join("activities.csv");
    if !legacy_csv_path.is_file() {
        println!("No root activities.csv found, skip legacy bootstrap.");
        return Ok(());
    }

    let mut index = index_store::load(&index_path)?;
    let rows = strava::read_activities(&legacy_csv_path)
        .with_context(|| format!("failed to load legacy csv {}", legacy_csv_path.display()))?;

    let mut inserted = 0usize;
    for row in rows {
        if index.contains_key(&row.activity_id) {
            continue;
        }

        let format = fs::infer_output_extension(row.source_basename.as_str())
            .unwrap_or("fit")
            .trim_start_matches('.')
            .to_string();

        index.insert(
            row.activity_id.clone(),
            ActivityRecord {
                activity_id: row.activity_id,
                activity_name_raw: row.activity_name.clone(),
                activity_name_sanitized: naming::sanitize_filename(&row.activity_name),
                activity_date: row.activity_date,
                source_batch: "legacy_root_csv".to_string(),
                source_file: row.source_file,
                source_basename: row.source_basename,
                file_format: format,
                library_path: None,
                processed_at: fs::timestamp_string(),
                status: ActivityStatus::LegacySeeded,
            },
        );
        inserted += 1;
    }

    index_store::save(&index_path, &index)?;
    println!("Seeded {inserted} legacy activities into state/activity_index.json");
    Ok(())
}
