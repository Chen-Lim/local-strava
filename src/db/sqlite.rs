use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};

use crate::db::models::{ActivityRecord, SyncRun};

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
        Ok(())
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
