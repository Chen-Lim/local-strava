use std::path::Path;

use anyhow::{Context, Result};
use duckdb::Connection;

use crate::services::fit_schema;

/// DuckDB-backed analytical store for parsed FIT messages.
///
/// One physical file at `state/activities.duckdb`. Schema is fully derived from
/// `assets/profile_messages.json` at first open (idempotent).
pub struct DuckActivityStore {
    conn: Connection,
}

impl DuckActivityStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create duckdb parent directory {}",
                    parent.display()
                )
            })?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("failed to open duckdb {}", path.display()))?;

        fit_schema::init_schema(&conn)
            .context("failed to initialize duckdb schema from FIT profile")?;

        Ok(Self { conn })
    }

    #[allow(dead_code)]
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn conn_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }
}
