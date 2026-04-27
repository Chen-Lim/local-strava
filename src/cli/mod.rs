use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use std::env;
use std::path::Path;

use crate::db::duckdb_store::DuckActivityStore;
use crate::db::sqlite::SqliteActivityStore;
use crate::services;
use crate::services::{fit_schema, layout};

#[derive(Parser)]
#[command(
    name = "strata",
    version,
    about = "Local Strava analytics — runs in the current directory"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Process Strava export batches in the cwd and ingest new FIT files into DuckDB.
    Sync {
        /// Restrict to a specific batch name (defaults to all).
        batch: Option<String>,
    },
    /// List valid Strava export batches (export_*.zip / export_*/) in the cwd.
    Scan,
    /// Copy activities from the latest sync (or since YYYY-MM-DD) into ./new/.
    ExportNew {
        /// YYYY-MM-DD start date; defaults to the latest completed sync run.
        since: Option<String>,
    },
    /// Re-parse a single activity (or --all) into DuckDB.
    Reingest {
        /// Activity id to reingest. Omit when using --all.
        activity_id: Option<String>,
        /// Reingest every activity.
        #[arg(long)]
        all: bool,
    },
    /// Print SQLite + DuckDB schema and row-count summary.
    DbInfo,
    /// List non-empty DuckDB tables and row counts.
    Tables,
    /// Print schema (columns, types) for a specific DuckDB table.
    Schema {
        table: String,
    },
    /// Run a SQL query against DuckDB and output as CSV or JSON.
    Query {
        sql: String,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        limit: Option<usize>,
    },
}

pub fn run() -> Result<()> {
    let cwd = env::current_dir().context("failed to read current directory")?;
    let cli = Cli::parse();
    let cmd = cli.command.unwrap_or(Cmd::Sync { batch: None });

    match cmd {
        Cmd::Sync { batch } => services::sync::run_sync(&cwd, batch.as_deref()),
        Cmd::Scan => run_scan(&cwd),
        Cmd::ExportNew { since } => services::export::export_new(&cwd, since.as_deref()),
        Cmd::Reingest { activity_id, all } => run_reingest(&cwd, activity_id.as_deref(), all),
        Cmd::DbInfo => run_db_info(&cwd),
        Cmd::Tables => run_tables(&cwd),
        Cmd::Schema { table } => run_schema(&cwd, &table),
        Cmd::Query { sql, json, limit } => run_query(&cwd, &sql, json, limit),
    }
}

fn run_scan(cwd: &Path) -> Result<()> {
    use crate::importers::strava::InboxEntry;
    let entries = crate::importers::strava::discover_exports(cwd)?;
    if entries.is_empty() {
        println!(
            "No Strava export batches (export_*.zip or export_*/) found in {}",
            cwd.display()
        );
    } else {
        for entry in entries {
            match entry {
                InboxEntry::Dir(batch) => {
                    println!(
                        "{}  [dir]   {}",
                        batch.batch_name,
                        display_path(cwd, batch.root.as_path())
                    );
                }
                InboxEntry::Zip {
                    batch_name,
                    zip_path,
                } => {
                    println!(
                        "{}  [zip]   {}",
                        batch_name,
                        display_path(cwd, zip_path.as_path())
                    );
                }
            }
        }
    }
    Ok(())
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

/// Marks the given activity (or all activities) as needing FIT ingestion by
/// clearing `fit_ingested_at`, then runs the standard ingest pass. The
/// underlying `ingest_activity` is idempotent (DELETE + Appender), so this
/// is safe to retry.
fn run_reingest(cwd: &Path, activity_id: Option<&str>, all: bool) -> Result<()> {
    layout::warn_if_legacy_state(cwd);
    layout::ensure_sync_layout(cwd)?;
    let db_path = layout::sqlite_path(cwd);
    let mut store = SqliteActivityStore::open(&db_path)?;

    match (activity_id, all) {
        (None, false) => bail!("reingest requires an <activity_id> or --all"),
        (_, true) => {
            store.clear_all_fit_ingested()?;
            println!("cleared fit_ingested_at on all activities");
        }
        (Some(activity_id), false) => {
            store.clear_fit_ingested(activity_id)?;
            println!("cleared fit_ingested_at on activity {activity_id}");
        }
    }

    services::sync::run_fit_ingest_pass(cwd, &mut store)
}

/// `db-info` — quick health snapshot of the analytical pipeline. Read-only;
/// does not create directories or files.
fn run_db_info(cwd: &Path) -> Result<()> {
    let sqlite_path = layout::sqlite_path(cwd);
    if !sqlite_path.exists() {
        bail!(
            "{} not found in {} — run `strata sync` first",
            sqlite_path.file_name().unwrap().to_string_lossy(),
            cwd.display()
        );
    }
    let store = SqliteActivityStore::open(&sqlite_path)?;
    let summary = store.fit_ingest_summary()?;
    println!("SQLite ({})", display_path(cwd, &sqlite_path));
    println!(
        "  fit activities : {} total, {} ingested, {} pending",
        summary.total_fit,
        summary.ingested,
        summary.total_fit.saturating_sub(summary.ingested)
    );

    let duck_path = layout::duckdb_path(cwd);
    if !duck_path.exists() {
        println!("\nDuckDB ({}) — not yet created", display_path(cwd, &duck_path));
        return Ok(());
    }
    let mut duck = DuckActivityStore::open(&duck_path)?;
    let conn = duck.conn_mut();

    let profile_version: String = conn
        .query_row(
            "SELECT value FROM _schema_meta WHERE key = 'profile_version'",
            [],
            |r| r.get(0),
        )
        .unwrap_or_else(|_| "<unknown>".to_string());
    println!(
        "\nDuckDB ({})\n  profile_version : {profile_version}\n  built-in        : {}",
        display_path(cwd, &duck_path),
        fit_schema::PROFILE_VERSION
    );

    println!("  table row counts (non-empty only):");
    let s = fit_schema::schema();
    let mut rows: Vec<(String, i64)> = Vec::new();
    for table_name in s.tables.keys() {
        let n: i64 = conn
            .query_row(
                &format!("SELECT count(*) FROM \"{table_name}\""),
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if n > 0 {
            rows.push((table_name.clone(), n));
        }
    }
    for aux in ["unknown_fields", "developer_field", "developer_field_value"] {
        let n: i64 = conn
            .query_row(&format!("SELECT count(*) FROM {aux}"), [], |r| r.get(0))
            .unwrap_or(0);
        if n > 0 {
            rows.push((aux.to_string(), n));
        }
    }
    rows.sort_by(|a, b| b.1.cmp(&a.1));
    let widest = rows.iter().map(|(n, _)| n.len()).max().unwrap_or(0);
    for (name, n) in &rows {
        println!("    {name:<widest$}  {n}");
    }
    println!("  ({} non-empty / {} total tables)", rows.len(), s.tables.len() + 3);
    Ok(())
}

/// `tables` — list non-empty tables and their row counts.
fn run_tables(cwd: &Path) -> Result<()> {
    let duck_path = layout::duckdb_path(cwd);
    if !duck_path.exists() {
        bail!(
            "{} not found in {} — run `strata sync` first",
            duck_path.file_name().unwrap().to_string_lossy(),
            cwd.display()
        );
    }
    let mut duck = DuckActivityStore::open(&duck_path)?;
    let conn = duck.conn_mut();

    let s = fit_schema::schema();
    let mut rows: Vec<(String, i64)> = Vec::new();
    for table_name in s.tables.keys() {
        let n: i64 = conn
            .query_row(&format!("SELECT count(*) FROM \"{table_name}\""), [], |r| {
                r.get(0)
            })
            .unwrap_or(0);
        if n > 0 {
            rows.push((table_name.clone(), n));
        }
    }
    for aux in ["unknown_fields", "developer_field", "developer_field_value"] {
        let n: i64 = conn
            .query_row(&format!("SELECT count(*) FROM {aux}"), [], |r| r.get(0))
            .unwrap_or(0);
        if n > 0 {
            rows.push((aux.to_string(), n));
        }
    }
    rows.sort_by(|a, b| b.1.cmp(&a.1));
    for (name, n) in rows {
        println!("{name}\t{n}");
    }
    Ok(())
}

/// `schema <table>` — print columns, types, and flags for a specific table.
fn run_schema(cwd: &Path, table_name: &str) -> Result<()> {
    let duck_path = layout::duckdb_path(cwd);
    if !duck_path.exists() {
        bail!(
            "{} not found in {} — run `strata sync` first",
            duck_path.file_name().unwrap().to_string_lossy(),
            cwd.display()
        );
    }

    let s = fit_schema::schema();

    if let Some(table) = s.table(table_name) {
        println!("column_name,type,is_semicircle,is_array");
        println!("activity_id,VARCHAR,false,false");
        println!("mesg_index,INTEGER,false,false");
        for c in &table.columns {
            println!(
                "{},{},{},{}",
                c.name,
                c.sql_type.sql_str(),
                c.is_semicircle,
                c.is_array
            );
        }
        return Ok(());
    }

    let mut duck = DuckActivityStore::open(&duck_path)?;
    let conn = duck.conn_mut();
    let stmt = conn
        .prepare(&format!("SELECT * FROM \"{table_name}\" LIMIT 0"))
        .with_context(|| format!("Table '{table_name}' not found or invalid"))?;
    drop(stmt);

    let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{table_name}\")"))?;
    let mut rows = stmt.query([])?;
    println!("column_name,type,is_semicircle,is_array");
    while let Ok(Some(row)) = rows.next() {
        let name: String = row.get(1)?;
        let typ: String = row.get(2)?;
        println!("{},{},false,false", name, typ);
    }
    Ok(())
}

/// `query <SQL>` — run a query and output CSV or JSON.
fn run_query(cwd: &Path, sql: &str, json: bool, limit: Option<usize>) -> Result<()> {
    let duck_path = layout::duckdb_path(cwd);
    if !duck_path.exists() {
        bail!(
            "{} not found in {} — run `strata sync` first",
            duck_path.file_name().unwrap().to_string_lossy(),
            cwd.display()
        );
    }

    let mut duck = DuckActivityStore::open(&duck_path)?;
    let conn = duck.conn_mut();

    let mut sql = sql.to_string();
    if let Some(l) = limit {
        sql = format!("SELECT * FROM ({}) LIMIT {}", sql, l);
    }

    let col_names = {
        let mut stmt0 = conn.prepare(&format!("SELECT * FROM ({}) LIMIT 0", sql))?;
        stmt0.execute([])?;
        stmt0.column_names()
    };
    let col_count = col_names.len();

    let mut stmt = conn.prepare(&sql).map_err(|e| {
        eprintln!("SQL Error: {}", e);
        std::process::exit(1);
    })?;

    let mut rows = stmt.query([]).map_err(|e| {
        eprintln!("Execution Error: {}", e);
        std::process::exit(1);
    })?;

    if json {
        println!("[");
        let mut first = true;
        while let Ok(Some(row)) = rows.next() {
            if !first {
                println!(",");
            } else {
                first = false;
            }
            let mut map = serde_json::Map::new();
            for i in 0..col_count {
                let val: duckdb::types::Value = row.get(i).unwrap_or(duckdb::types::Value::Null);
                map.insert(col_names[i].clone(), duck_to_json(val));
            }
            print!("  {}", serde_json::Value::Object(map).to_string());
        }
        println!("\n]");
    } else {
        let mut wtr = csv::Writer::from_writer(std::io::stdout());
        wtr.write_record(&col_names)?;
        while let Ok(Some(row)) = rows.next() {
            let mut record = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let val: duckdb::types::Value = row.get(i).unwrap_or(duckdb::types::Value::Null);
                record.push(duck_to_string(val));
            }
            wtr.write_record(&record)?;
        }
        wtr.flush()?;
    }

    Ok(())
}

fn duck_to_string(val: duckdb::types::Value) -> String {
    use duckdb::types::Value;
    match val {
        Value::Null => String::new(),
        Value::Boolean(b) => b.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::Int(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(d) => d.to_string(),
        Value::Text(s) => s,
        Value::Timestamp(tu, v) => {
            let (secs, nanos) = match tu {
                duckdb::types::TimeUnit::Second => (v, 0),
                duckdb::types::TimeUnit::Millisecond => (v / 1000, (v % 1000) as u32 * 1_000_000),
                duckdb::types::TimeUnit::Microsecond => {
                    (v / 1_000_000, (v % 1_000_000) as u32 * 1_000)
                }
                duckdb::types::TimeUnit::Nanosecond => {
                    (v / 1_000_000_000, (v % 1_000_000_000) as u32)
                }
            };
            chrono::DateTime::from_timestamp(secs, nanos)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "Invalid Timestamp".to_string())
        }
        Value::HugeInt(v) => v.to_string(),
        Value::Blob(b) => format!("{:?}", b),
        _ => format!("{:?}", val),
    }
}

fn duck_to_json(val: duckdb::types::Value) -> serde_json::Value {
    use duckdb::types::Value;
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(b),
        Value::TinyInt(i) => serde_json::json!(i),
        Value::SmallInt(i) => serde_json::json!(i),
        Value::Int(i) => serde_json::json!(i),
        Value::BigInt(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::json!(f),
        Value::Double(d) => serde_json::json!(d),
        Value::Text(s) => serde_json::Value::String(s),
        Value::Timestamp(tu, v) => {
            let (secs, nanos) = match tu {
                duckdb::types::TimeUnit::Second => (v, 0),
                duckdb::types::TimeUnit::Millisecond => (v / 1000, (v % 1000) as u32 * 1_000_000),
                duckdb::types::TimeUnit::Microsecond => {
                    (v / 1_000_000, (v % 1_000_000) as u32 * 1_000)
                }
                duckdb::types::TimeUnit::Nanosecond => {
                    (v / 1_000_000_000, (v % 1_000_000_000) as u32)
                }
            };
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                serde_json::Value::String(dt.to_rfc3339())
            } else {
                serde_json::Value::Null
            }
        }
        Value::HugeInt(v) => serde_json::json!(v.to_string()),
        _ => serde_json::Value::String(format!("{:?}", val)),
    }
}
