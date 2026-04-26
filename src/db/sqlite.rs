use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};

use crate::db::models::{ActivityRecord, SyncRun};

#[derive(Debug, Clone, Copy)]
pub struct FitIngestSummary {
    pub total_fit: usize,
    pub ingested: usize,
}

pub struct SqliteActivityStore {
    conn: Connection,
}

impl SqliteActivityStore {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open sqlite db {}", path.display()))?;
        let store = Self { conn };
        store.init()?;
        Ok(store)
    }

    fn init(&self) -> Result<()> {
        self.conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS sync_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS activities (
                activity_id TEXT PRIMARY KEY,
                activity_name_raw TEXT NOT NULL,
                activity_name_sanitized TEXT NOT NULL,
                activity_date TEXT NOT NULL,
                file_format TEXT NOT NULL,
                library_path TEXT NOT NULL,
                source_batch TEXT NOT NULL,
                source_file TEXT NOT NULL,
                source_basename TEXT NOT NULL,
                import_run_id TEXT NOT NULL,
                imported_at TEXT NOT NULL,
                FOREIGN KEY(import_run_id) REFERENCES sync_runs(run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_activities_imported_at
            ON activities(imported_at);

            CREATE INDEX IF NOT EXISTS idx_activities_file_format
            ON activities(file_format);

            CREATE INDEX IF NOT EXISTS idx_activities_import_run_id
            ON activities(import_run_id);

            CREATE INDEX IF NOT EXISTS idx_sync_runs_completed_at
            ON sync_runs(completed_at);
            ",
        )?;

        // Phase 2 / M3 migration: add fit_ingested_at column to existing DBs.
        // SQLite has no IF NOT EXISTS for ADD COLUMN, so probe the schema first.
        if !self.column_exists("activities", "fit_ingested_at")? {
            self.conn.execute_batch(
                "ALTER TABLE activities ADD COLUMN fit_ingested_at TEXT;
                 CREATE INDEX IF NOT EXISTS idx_activities_fit_ingested_at
                 ON activities(fit_ingested_at);",
            )?;
        }

        Ok(())
    }

    fn column_exists(&self, table: &str, column: &str) -> Result<bool> {
        let mut stmt = self
            .conn
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for r in rows {
            if r? == column {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn begin_sync_run(&mut self, run: &SyncRun) -> Result<()> {
        self.conn.execute(
            "INSERT INTO sync_runs (run_id, started_at, completed_at) VALUES (?1, ?2, ?3)",
            params![run.run_id, run.started_at, run.completed_at],
        )?;
        Ok(())
    }

    pub fn complete_sync_run(&mut self, run_id: &str, completed_at: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE sync_runs SET completed_at = ?1 WHERE run_id = ?2",
            params![completed_at, run_id],
        )?;
        Ok(())
    }

    pub fn existing_ids<'a, I>(&self, ids: I) -> Result<HashSet<String>>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let ids: Vec<&str> = ids.into_iter().collect();
        let mut existing = HashSet::new();

        for chunk in ids.chunks(500) {
            if chunk.is_empty() {
                continue;
            }

            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql =
                format!("SELECT activity_id FROM activities WHERE activity_id IN ({placeholders})");
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(chunk.iter().copied()), |row| row.get(0))?;
            for row in rows {
                existing.insert(row?);
            }
        }

        Ok(existing)
    }

    pub fn insert_activities(&mut self, records: &[ActivityRecord]) -> Result<usize> {
        let tx = self.conn.transaction()?;
        let mut inserted = 0usize;

        {
            let mut stmt = tx.prepare(
                "
                INSERT OR IGNORE INTO activities (
                    activity_id,
                    activity_name_raw,
                    activity_name_sanitized,
                    activity_date,
                    file_format,
                    library_path,
                    source_batch,
                    source_file,
                    source_basename,
                    import_run_id,
                    imported_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                ",
            )?;

            for record in records {
                let changed = stmt.execute(params![
                    record.activity_id,
                    record.activity_name_raw,
                    record.activity_name_sanitized,
                    record.activity_date,
                    record.file_format,
                    record.library_path,
                    record.source_batch,
                    record.source_file,
                    record.source_basename,
                    record.import_run_id,
                    record.imported_at,
                ])?;
                inserted += changed;
            }
        }

        tx.commit()?;
        Ok(inserted)
    }

    /// Activities whose FIT file has not yet been ingested into DuckDB.
    /// Filters to `file_format = 'fit'`; non-FIT activities are skipped here
    /// and remain forever NULL on `fit_ingested_at` (intentional — only FIT
    /// is parseable by our pipeline).
    pub fn pending_fit_ingest(&self) -> Result<Vec<ActivityRecord>> {
        self.activities_by_clause(
            "WHERE file_format = 'fit' AND fit_ingested_at IS NULL \
             ORDER BY imported_at ASC",
            params![],
        )
    }

    pub fn mark_fit_ingested(&mut self, activity_id: &str, ingested_at: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE activities SET fit_ingested_at = ?1 WHERE activity_id = ?2",
            params![ingested_at, activity_id],
        )?;
        Ok(())
    }

    /// Mark a single activity as needing re-ingestion. Used by `reingest <id>` (M5).
    #[allow(dead_code)]
    pub fn clear_fit_ingested(&mut self, activity_id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE activities SET fit_ingested_at = NULL WHERE activity_id = ?1",
            params![activity_id],
        )?;
        Ok(())
    }

    /// Wipe `fit_ingested_at` for every activity. Used by `reingest --all` (M5).
    #[allow(dead_code)]
    pub fn clear_all_fit_ingested(&mut self) -> Result<()> {
        self.conn
            .execute("UPDATE activities SET fit_ingested_at = NULL", [])?;
        Ok(())
    }

    /// Reserved for `db-info` CLI (M5).
    #[allow(dead_code)]
    pub fn fit_ingest_summary(&self) -> Result<FitIngestSummary> {
        let total: i64 = self.conn.query_row(
            "SELECT count(*) FROM activities WHERE file_format = 'fit'",
            [],
            |r| r.get(0),
        )?;
        let ingested: i64 = self.conn.query_row(
            "SELECT count(*) FROM activities \
             WHERE file_format = 'fit' AND fit_ingested_at IS NOT NULL",
            [],
            |r| r.get(0),
        )?;
        Ok(FitIngestSummary {
            total_fit: total as usize,
            ingested: ingested as usize,
        })
    }

    pub fn latest_completed_run_id(&self) -> Result<Option<String>> {
        self.conn
            .query_row(
                "
                SELECT run_id
                FROM sync_runs
                WHERE completed_at IS NOT NULL
                ORDER BY completed_at DESC
                LIMIT 1
                ",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn activities_for_run(&self, run_id: &str) -> Result<Vec<ActivityRecord>> {
        self.activities_by_clause(
            "WHERE import_run_id = ?1 ORDER BY imported_at ASC",
            params![run_id],
        )
    }

    pub fn activities_since(&self, imported_at: &str) -> Result<Vec<ActivityRecord>> {
        self.activities_by_clause(
            "WHERE imported_at >= ?1 ORDER BY imported_at ASC",
            params![imported_at],
        )
    }

    fn activities_by_clause<P>(&self, clause: &str, params: P) -> Result<Vec<ActivityRecord>>
    where
        P: rusqlite::Params,
    {
        let sql = format!(
            "
            SELECT
                activity_id,
                activity_name_raw,
                activity_name_sanitized,
                activity_date,
                file_format,
                library_path,
                source_batch,
                source_file,
                source_basename,
                import_run_id,
                imported_at
            FROM activities
            {clause}
            "
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params, |row| {
            Ok(ActivityRecord {
                activity_id: row.get(0)?,
                activity_name_raw: row.get(1)?,
                activity_name_sanitized: row.get(2)?,
                activity_date: row.get(3)?,
                file_format: row.get(4)?,
                library_path: row.get(5)?,
                source_batch: row.get(6)?,
                source_file: row.get(7)?,
                source_basename: row.get(8)?,
                import_run_id: row.get(9)?,
                imported_at: row.get(10)?,
            })
        })?;

        let mut records = Vec::new();
        for row in rows {
            records.push(row?);
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::models::ActivityRecord;
    use tempfile::tempdir;

    fn temp_store() -> (tempfile::TempDir, SqliteActivityStore) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let store = SqliteActivityStore::open(&path).unwrap();
        (dir, store)
    }

    fn seed_activity(store: &mut SqliteActivityStore, id: &str, file_format: &str) {
        store
            .begin_sync_run(&SyncRun {
                run_id: format!("run-{id}"),
                started_at: "2026-04-27T00:00:00Z".into(),
                completed_at: None,
            })
            .unwrap();
        store
            .insert_activities(&[ActivityRecord {
                activity_id: id.into(),
                activity_name_raw: "n".into(),
                activity_name_sanitized: "n".into(),
                activity_date: "2026-04-27".into(),
                file_format: file_format.into(),
                library_path: "x".into(),
                source_batch: "b".into(),
                source_file: "f".into(),
                source_basename: "f".into(),
                import_run_id: format!("run-{id}"),
                imported_at: "2026-04-27T00:00:00Z".into(),
            }])
            .unwrap();
    }

    #[test]
    fn fresh_db_has_fit_ingested_at_column() {
        let (_d, store) = temp_store();
        assert!(store.column_exists("activities", "fit_ingested_at").unwrap());
    }

    #[test]
    fn migration_adds_column_to_legacy_db() {
        // Simulate a pre-M3 DB by creating one with the legacy schema, then
        // re-opening it through SqliteActivityStore::open (which runs init).
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE activities (
                    activity_id TEXT PRIMARY KEY,
                    activity_name_raw TEXT NOT NULL,
                    activity_name_sanitized TEXT NOT NULL,
                    activity_date TEXT NOT NULL,
                    file_format TEXT NOT NULL,
                    library_path TEXT NOT NULL,
                    source_batch TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    source_basename TEXT NOT NULL,
                    import_run_id TEXT NOT NULL,
                    imported_at TEXT NOT NULL
                );",
            )
            .unwrap();
        }
        let store = SqliteActivityStore::open(&path).unwrap();
        assert!(store.column_exists("activities", "fit_ingested_at").unwrap());
    }

    #[test]
    fn pending_fit_ingest_filters_to_fit_and_null_only() {
        let (_d, mut store) = temp_store();
        seed_activity(&mut store, "fit-1", "fit");
        seed_activity(&mut store, "fit-2", "fit");
        seed_activity(&mut store, "tcx-1", "tcx");

        let pending = store.pending_fit_ingest().unwrap();
        let ids: Vec<&str> = pending.iter().map(|r| r.activity_id.as_str()).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"fit-1"));
        assert!(ids.contains(&"fit-2"));
        assert!(!ids.iter().any(|id| *id == "tcx-1"));

        store.mark_fit_ingested("fit-1", "2026-04-27T01:00:00Z").unwrap();
        let pending = store.pending_fit_ingest().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].activity_id, "fit-2");

        store.clear_fit_ingested("fit-1").unwrap();
        assert_eq!(store.pending_fit_ingest().unwrap().len(), 2);

        store.mark_fit_ingested("fit-1", "2026-04-27T01:00:00Z").unwrap();
        store.mark_fit_ingested("fit-2", "2026-04-27T01:00:00Z").unwrap();
        store.clear_all_fit_ingested().unwrap();
        assert_eq!(store.pending_fit_ingest().unwrap().len(), 2);
    }

    #[test]
    fn fit_ingest_summary_counts_correctly() {
        let (_d, mut store) = temp_store();
        seed_activity(&mut store, "f1", "fit");
        seed_activity(&mut store, "f2", "fit");
        seed_activity(&mut store, "g1", "gpx");

        let s = store.fit_ingest_summary().unwrap();
        assert_eq!(s.total_fit, 2);
        assert_eq!(s.ingested, 0);

        store.mark_fit_ingested("f1", "2026-04-27T01:00:00Z").unwrap();
        let s = store.fit_ingest_summary().unwrap();
        assert_eq!(s.total_fit, 2);
        assert_eq!(s.ingested, 1);
    }
}
