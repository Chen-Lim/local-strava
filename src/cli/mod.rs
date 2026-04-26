use anyhow::{Context, Result, bail};
use std::env;

use crate::db::duckdb_store::DuckActivityStore;
use crate::db::sqlite::SqliteActivityStore;
use crate::services;
use crate::services::fit_schema;

pub fn run() -> Result<()> {
    let cwd = env::current_dir().context("failed to read current directory")?;
    let args: Vec<String> = env::args().collect();

    match args.get(1).map(String::as_str) {
        None | Some("sync") => {
            let batch = args.get(2).map(String::as_str);
            services::sync::run_sync(&cwd, batch)
        }
        Some("reingest") => run_reingest(&cwd, args.get(2).map(String::as_str)),
        Some("db-info") => run_db_info(&cwd),
        Some("scan") => {
            use crate::importers::strava::InboxEntry;
            let entries = crate::importers::strava::discover_inbox(&cwd.join("inbox"))?;
            if entries.is_empty() {
                println!("No Strava export entries found in inbox/");
            } else {
                for entry in entries {
                    match entry {
                        InboxEntry::Dir(batch) => {
                            println!(
                                "{}  [dir]   {}",
                                batch.batch_name,
                                display_path(cwd.as_path(), batch.root.as_path())
                            );
                        }
                        InboxEntry::Zip {
                            batch_name,
                            zip_path,
                        } => {
                            println!(
                                "{}  [zip]   {}",
                                batch_name,
                                display_path(cwd.as_path(), zip_path.as_path())
                            );
                        }
                    }
                }
            }
            Ok(())
        }
        Some("export-new") => {
            let since = args.get(2).map(String::as_str);
            services::export::export_new(&cwd, since)
        }
        Some("tables") => run_tables(&cwd),
        Some("schema") => {
            let table = args.get(2).map(String::as_str);
            run_schema(&cwd, table)
        }
        Some("query") => {
            let is_json = args.iter().any(|a| a == "--json");
            let limit = args
                .iter()
                .position(|a| a == "--limit")
                .and_then(|pos| args.get(pos + 1))
                .and_then(|v| v.parse::<usize>().ok());
            
            let mut sql = None;
            let mut skip_next = false;
            for arg in args.iter().skip(2) {
                if skip_next {
                    skip_next = false;
                    continue;
                }
                if arg == "--json" {
                    continue;
                }
                if arg == "--limit" {
                    skip_next = true;
                    continue;
                }
                sql = Some(arg.as_str());
                break;
            }
            run_query(&cwd, sql, is_json, limit)
        }
        Some("help") | Some("--help") | Some("-h") => {
            print_help();
            Ok(())
        }
        Some(other) => {
            bail!("unknown command: {other}")
        }
    }
}

fn display_path(root: &std::path::Path, path: &std::path::Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

fn print_help() {
    println!("strava-sync");
    println!();
    println!("Usage:");
    println!("  cargo run -- sync [batch_name]");
    println!("  cargo run -- scan");
    println!("  cargo run -- export-new [YYYY-MM-DD]");
    println!("  cargo run -- reingest [activity_id | --all]");
    println!("  cargo run -- db-info");
    println!("  cargo run -- tables");
    println!("  cargo run -- schema <table>");
    println!("  cargo run -- query <SQL> [--json] [--limit N]");
    println!();
    println!("Commands:");
    println!("  sync              Process inbox batches and ingest new FIT files into DuckDB");
    println!("  scan              List valid Strava export batches under inbox/");
    println!("  export-new        Export activities from the latest sync run or from a start date");
    println!("  reingest          Re-parse a single activity (or --all) into DuckDB");
    println!("  db-info           Print SQLite + DuckDB schema and row-count summary");
    println!("  tables            List non-empty DuckDB tables and row counts");
    println!("  schema            Print schema (columns, types) for a specific DuckDB table");
    println!("  query             Run a SQL query against DuckDB and output as CSV or JSON");
}

/// `reingest [activity_id | --all]`
///
/// Marks the given activity (or all activities) as needing FIT ingestion by
/// clearing `fit_ingested_at`, then runs the standard ingest pass. The
/// underlying `ingest_activity` is idempotent (DELETE + Appender), so this
/// is safe to retry.
fn run_reingest(cwd: &std::path::Path, arg: Option<&str>) -> Result<()> {
    let db_path = cwd.join("state/strava.db");
    let mut store = SqliteActivityStore::open(&db_path)?;

    match arg {
        None => bail!("reingest requires an <activity_id> or --all"),
        Some("--all") => {
            store.clear_all_fit_ingested()?;
            println!("cleared fit_ingested_at on all activities");
        }
        Some(activity_id) => {
            store.clear_fit_ingested(activity_id)?;
            println!("cleared fit_ingested_at on activity {activity_id}");
        }
    }

    services::sync::run_fit_ingest_pass(cwd, &mut store)
}

/// `db-info` — quick health snapshot of the analytical pipeline. Does not
/// modify any state.
fn run_db_info(cwd: &std::path::Path) -> Result<()> {
    let sqlite_path = cwd.join("state/strava.db");
    let store = SqliteActivityStore::open(&sqlite_path)?;
    let summary = store.fit_ingest_summary()?;
    println!("SQLite ({})", display_path(cwd, &sqlite_path));
    println!(
        "  fit activities : {} total, {} ingested, {} pending",
        summary.total_fit,
        summary.ingested,
        summary.total_fit.saturating_sub(summary.ingested)
    );

    let duck_path = cwd.join("state/activities.duckdb");
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
fn run_tables(cwd: &std::path::Path) -> Result<()> {
    let duck_path = cwd.join("state/activities.duckdb");
    if !duck_path.exists() {
        bail!("DuckDB not found at {}", duck_path.display());
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
fn run_schema(cwd: &std::path::Path, table_name: Option<&str>) -> Result<()> {
    let table_name = table_name.context("schema requires a table name")?;
    
    let duck_path = cwd.join("state/activities.duckdb");
    if !duck_path.exists() {
        bail!("DuckDB not found at {}", duck_path.display());
    }

    let s = fit_schema::schema();
    
    // For standard profile tables
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

    // For aux tables (query sqlite master or simply return structure)
    // To be robust, we can just query the schema from DuckDB directly
    let mut duck = DuckActivityStore::open(&duck_path)?;
    let conn = duck.conn_mut();
    let stmt = conn.prepare(&format!("SELECT * FROM \"{table_name}\" LIMIT 0"))
        .with_context(|| format!("Table '{table_name}' not found or invalid"))?;
    
    // Check if table exists and drop stmt to free locks
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
fn run_query(
    cwd: &std::path::Path,
    sql: Option<&str>,
    json: bool,
    limit: Option<usize>,
) -> Result<()> {
    let mut sql = sql.context("query requires a SQL string")?.to_string();

    let duck_path = cwd.join("state/activities.duckdb");
    if !duck_path.exists() {
        bail!("DuckDB not found at {}", duck_path.display());
    }

    let mut duck = DuckActivityStore::open(&duck_path)?;
    let conn = duck.conn_mut();

    if let Some(l) = limit {
        sql = format!("SELECT * FROM ({}) LIMIT {}", sql, l);
    }

    let col_names = {
        // Execute a LIMIT 0 wrapper to safely fetch the schema and column names
        // without keeping the statement borrowed or exhausting rows.
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
            // DuckDB timestamp is typically microseconds, but TimeUnit tells us.
            let (secs, nanos) = match tu {
                duckdb::types::TimeUnit::Second => (v, 0),
                duckdb::types::TimeUnit::Millisecond => (v / 1000, (v % 1000) as u32 * 1_000_000),
                duckdb::types::TimeUnit::Microsecond => (v / 1_000_000, (v % 1_000_000) as u32 * 1_000),
                duckdb::types::TimeUnit::Nanosecond => (v / 1_000_000_000, (v % 1_000_000_000) as u32),
            };
            chrono::DateTime::from_timestamp(secs, nanos)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "Invalid Timestamp".to_string())
        }
        Value::HugeInt(v) => v.to_string(),
        Value::Blob(b) => format!("{:?}", b), // fallback for Blob
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
                duckdb::types::TimeUnit::Microsecond => (v / 1_000_000, (v % 1_000_000) as u32 * 1_000),
                duckdb::types::TimeUnit::Nanosecond => (v / 1_000_000_000, (v % 1_000_000_000) as u32),
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
