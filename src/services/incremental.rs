use crate::domain::activity::{ActivityCsvRow, ActivityRecord};

pub fn pending_rows<'a>(
    rows: &'a [ActivityCsvRow],
    index: &'a std::collections::HashMap<String, ActivityRecord>,
) -> Vec<ActivityCsvRow> {
    rows.iter()
        .filter(|row| !index.contains_key(&row.activity_id))
        .cloned()
        .collect()
}
