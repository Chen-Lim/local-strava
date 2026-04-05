use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct StravaExportBatch {
    pub batch_name: String,
    pub root: PathBuf,
    pub csv_path: PathBuf,
    pub activities_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportLogEntry {
    pub batch_name: String,
    pub batch_path: String,
    pub processed_at: String,
    pub total_rows: usize,
    pub new_activities: usize,
    pub skipped_activities: usize,
    pub failed_activities: usize,
}
