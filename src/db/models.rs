#[derive(Debug, Clone)]
pub struct ActivityRecord {
    pub activity_id: String,
    pub activity_name_raw: String,
    pub activity_name_sanitized: String,
    pub activity_date: String,
    pub file_format: String,
    pub library_path: String,
    pub source_batch: String,
    pub source_file: String,
    pub source_basename: String,
    pub import_run_id: String,
    pub imported_at: String,
}

#[derive(Debug, Clone)]
pub struct SyncRun {
    pub run_id: String,
    pub started_at: String,
    pub completed_at: Option<String>,
}
