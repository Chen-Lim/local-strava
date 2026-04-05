use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityCsvRow {
    pub activity_id: String,
    pub activity_name: String,
    pub activity_date: String,
    pub source_file: String,
    pub source_basename: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityRecord {
    pub activity_id: String,
    pub activity_name_raw: String,
    pub activity_name_sanitized: String,
    pub activity_date: String,
    pub source_batch: String,
    pub source_file: String,
    pub source_basename: String,
    pub file_format: String,
    pub library_path: Option<String>,
    pub processed_at: String,
    pub status: ActivityStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityStatus {
    Ready,
    LegacySeeded,
}
