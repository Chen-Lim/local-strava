use std::path::Path;

use anyhow::{Context, Result};
use chrono::{NaiveDate, TimeZone, Utc};

use crate::db::sqlite::SqliteActivityStore;
use crate::services::layout;
use crate::utils::fs;

pub fn export_new(project_root: &Path, since: Option<&str>) -> Result<()> {
    layout::warn_if_legacy_state(project_root);
    layout::ensure_export_layout(project_root)?;

    let db_path = layout::sqlite_path(project_root);
    let store = SqliteActivityStore::open(&db_path)?;

    let records = match since {
        Some(date) => {
            let imported_at = parse_start_date(date)?;
            store.activities_since(&imported_at)?
        }
        None => {
            let Some(run_id) = store.latest_completed_run_id()? else {
                println!("No completed sync runs found. Nothing to export.");
                return Ok(());
            };
            store.activities_for_run(&run_id)?
        }
    };

    if records.is_empty() {
        println!("No activities matched the export criteria.");
        return Ok(());
    }

    let new_dir = layout::new_dir(project_root);

    for record in &records {
        let file_name = Path::new(&record.library_path)
            .file_name()
            .context("activity record missing file name")?;
        let source_path = project_root.join(&record.library_path);
        let dest_path = new_dir.join(file_name);
        fs::copy_file_overwrite(&source_path, &dest_path)?;
    }

    println!("Exported {} activities to new/", records.len());
    Ok(())
}

fn parse_start_date(value: &str) -> Result<String> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("invalid date format: {value}, expected YYYY-MM-DD"))?;
    let dt = Utc.from_utc_datetime(
        &date
            .and_hms_opt(0, 0, 0)
            .context("failed to build UTC datetime from date")?,
    );
    Ok(dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}
