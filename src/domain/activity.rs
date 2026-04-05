use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityCsvRow {
    pub activity_id: String,
    pub activity_name: String,
    pub activity_date: String,
    pub source_file: String,
    pub source_basename: String,
}
