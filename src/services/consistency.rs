use std::path::Path;

use anyhow::Result;

use crate::storage::export_log;
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

    export_log::ensure_log_file(&project_root.join("state/exports.jsonl"))?;
    Ok(())
}
