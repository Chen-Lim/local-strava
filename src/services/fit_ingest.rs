//! Parse a single `.fit` file and write its messages into DuckDB tables
//! derived from the FIT Profile.
//!
//! Idempotency: every run starts with `DELETE FROM <table> WHERE activity_id = ?`
//! across all schema-known tables plus the auxiliary `unknown_fields` /
//! `developer_field*` tables. The whole operation runs inside a transaction.
//!
//! See [crate::services::fit_schema] for table/column derivation and
//! [docs/fit-to-duckdb.md](../../docs/fit-to-duckdb.md) for design rationale.

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{NaiveDateTime, Utc};
use duckdb::Connection;
use duckdb::types::Value as DuckValue;
use fitparser::{FitDataRecord, Value as FitValue};

use crate::services::fit_schema::{Column, SqlType, schema};

/// Ingest one FIT file into DuckDB.
///
/// Idempotency: each ingest first DELETEs any prior rows for `activity_id`
/// from every table it's about to write to (plus aux tables). DuckDB's bulk
/// `Appender` API is then used for inserts. We do **not** wrap the whole op
/// in an explicit transaction: Appender holds an exclusive borrow on the
/// connection, which is incompatible with a long-lived `Transaction`. The
/// DELETE-then-append pattern is safe across crashes because the next ingest
/// will rerun the DELETE before re-inserting.
pub fn ingest_activity(conn: &mut Connection, activity_id: &str, fit_path: &Path) -> Result<()> {
    let file = File::open(fit_path)
        .with_context(|| format!("failed to open fit file {}", fit_path.display()))?;
    let mut reader = BufReader::new(file);
    let records: Vec<FitDataRecord> = fitparser::from_reader(&mut reader)
        .with_context(|| format!("failed to parse FIT file {}", fit_path.display()))?;

    let s = schema();

    // Group by message kind, preserving in-file order so mesg_index is stable.
    let mut by_kind: indexmap::IndexMap<String, Vec<FitDataRecord>> = indexmap::IndexMap::new();
    for rec in records {
        let kind = rec.kind().to_string();
        by_kind.entry(kind).or_default().push(rec);
    }

    // 1) Wipe prior rows for this activity. Narrow scope: only tables we're
    //    about to write, plus aux tables (which may have been written by a
    //    wider previous run).
    for mesg_name in by_kind.keys() {
        if s.table(mesg_name).is_some() {
            conn.execute(
                &format!("DELETE FROM \"{mesg_name}\" WHERE activity_id = ?"),
                [activity_id],
            )?;
        }
    }
    for aux in ["unknown_fields", "developer_field", "developer_field_value"] {
        conn.execute(
            &format!("DELETE FROM {aux} WHERE activity_id = ?"),
            [activity_id],
        )?;
    }

    // 2) Insert per kind via Appender (bulk fast path).
    for (mesg_name, recs) in &by_kind {
        if let Some(table) = s.table(mesg_name) {
            append_known(conn, activity_id, table, recs)?;
        } else {
            append_unknown(conn, activity_id, mesg_name, recs)?;
        }
    }

    Ok(())
}

fn append_known(
    conn: &Connection,
    activity_id: &str,
    table: &crate::services::fit_schema::Table,
    recs: &[FitDataRecord],
) -> Result<()> {
    let mut app = conn.appender(&table.name).with_context(|| {
        format!("failed to open duckdb appender for table {}", table.name)
    })?;

    for (idx, rec) in recs.iter().enumerate() {
        let by_name: HashMap<&str, &FitValue> =
            rec.fields().iter().map(|f| (f.name(), f.value())).collect();

        let mut row: Vec<DuckValue> = Vec::with_capacity(2 + table.columns.len());
        row.push(DuckValue::Text(activity_id.to_string()));
        row.push(DuckValue::Int(idx as i32));
        for col in &table.columns {
            let v = by_name
                .get(col.fit_name.as_str())
                .map(|fv| fit_value_to_duck(fv, col))
                .unwrap_or(DuckValue::Null);
            row.push(v);
        }
        let refs: Vec<&dyn duckdb::ToSql> = row.iter().map(|p| p as &dyn duckdb::ToSql).collect();
        app.append_row(refs.as_slice())?;
    }
    app.flush()?;
    drop(app);

    // Persist any FIT fields that exist on this record but aren't in our
    // schema's column list (e.g., undocumented vendor fields).
    let known: std::collections::HashSet<&str> =
        table.columns.iter().map(|c| c.fit_name.as_str()).collect();
    let has_any_unknown = recs
        .iter()
        .any(|r| r.fields().iter().any(|f| !known.contains(f.name())));
    if !has_any_unknown {
        return Ok(());
    }

    let mut app = conn.appender("unknown_fields")?;
    for (idx, rec) in recs.iter().enumerate() {
        for f in rec.fields() {
            if !known.contains(f.name()) {
                let json = serde_json::to_string(f.value()).ok();
                app.append_row(duckdb::params![
                    activity_id,
                    idx as i32,
                    table.name,
                    f.name(),
                    f.number() as i32,
                    json,
                ])?;
            }
        }
    }
    app.flush()?;
    Ok(())
}

fn append_unknown(
    conn: &Connection,
    activity_id: &str,
    mesg_name: &str,
    recs: &[FitDataRecord],
) -> Result<()> {
    let mut app = conn.appender("unknown_fields")?;
    for (idx, rec) in recs.iter().enumerate() {
        if rec.fields().is_empty() {
            app.append_row(duckdb::params![
                activity_id,
                idx as i32,
                mesg_name,
                Option::<&str>::None,
                Option::<i32>::None,
                Option::<&str>::None,
            ])?;
            continue;
        }
        for f in rec.fields() {
            let json = serde_json::to_string(f.value()).ok();
            app.append_row(duckdb::params![
                activity_id,
                idx as i32,
                mesg_name,
                f.name(),
                f.number() as i32,
                json,
            ])?;
        }
    }
    app.flush()?;
    Ok(())
}

/// Convert a fitparser [`Value`] into a [`DuckValue`] honoring the column's
/// declared SQL type. Type mismatches fall back to `NULL`, never panic.
fn fit_value_to_duck(v: &FitValue, col: &Column) -> DuckValue {
    if matches!(v, FitValue::Invalid) {
        return DuckValue::Null;
    }

    // Semicircle → degrees: fitparser leaves these as SInt32. Apply only when
    // the schema says the column is a semicircle-derived DOUBLE.
    if col.is_semicircle {
        let raw = match v {
            FitValue::SInt32(n) => Some(*n as f64),
            FitValue::SInt64(n) => Some(*n as f64),
            FitValue::Float32(f) => Some(*f as f64),
            FitValue::Float64(f) => Some(*f),
            _ => None,
        };
        return match raw {
            Some(n) => DuckValue::Double(n * 180.0 / 2_147_483_648.0),
            None => DuckValue::Null,
        };
    }

    // Arrays → JSON string (declared as VARCHAR in schema).
    if col.is_array {
        return match v {
            FitValue::Array(items) => match serde_json::to_string(items) {
                Ok(s) => DuckValue::Text(s),
                Err(_) => DuckValue::Null,
            },
            other => match serde_json::to_string(other) {
                Ok(s) => DuckValue::Text(s),
                Err(_) => DuckValue::Null,
            },
        };
    }

    match col.sql_type {
        SqlType::Boolean => match v {
            FitValue::Enum(n) | FitValue::UInt8(n) | FitValue::Byte(n) | FitValue::UInt8z(n) => {
                DuckValue::Boolean(*n != 0)
            }
            FitValue::SInt8(n) => DuckValue::Boolean(*n != 0),
            _ => DuckValue::Null,
        },
        SqlType::Integer => coerce_integer(v).map(DuckValue::Int).unwrap_or(DuckValue::Null),
        SqlType::BigInt => coerce_bigint(v).map(DuckValue::BigInt).unwrap_or(DuckValue::Null),
        SqlType::Double => coerce_double(v).map(DuckValue::Double).unwrap_or(DuckValue::Null),
        SqlType::Timestamp => match v {
            FitValue::Timestamp(dt) => {
                let utc = dt.with_timezone(&Utc).naive_utc();
                naive_to_duck_ts(utc)
            }
            _ => DuckValue::Null,
        },
        SqlType::Varchar => DuckValue::Text(stringify(v)),
    }
}

fn coerce_integer(v: &FitValue) -> Option<i32> {
    match v {
        FitValue::Enum(n) | FitValue::UInt8(n) | FitValue::Byte(n) | FitValue::UInt8z(n) => {
            Some(*n as i32)
        }
        FitValue::SInt8(n) => Some(*n as i32),
        FitValue::SInt16(n) => Some(*n as i32),
        FitValue::UInt16(n) | FitValue::UInt16z(n) => Some(*n as i32),
        FitValue::SInt32(n) => Some(*n),
        FitValue::UInt32(n) | FitValue::UInt32z(n) => Some(*n as i32), // may wrap on huge values
        FitValue::Float32(f) => Some(*f as i32),
        FitValue::Float64(f) => Some(*f as i32),
        _ => None,
    }
}

fn coerce_bigint(v: &FitValue) -> Option<i64> {
    match v {
        FitValue::Enum(n) | FitValue::UInt8(n) | FitValue::Byte(n) | FitValue::UInt8z(n) => {
            Some(*n as i64)
        }
        FitValue::SInt8(n) => Some(*n as i64),
        FitValue::SInt16(n) => Some(*n as i64),
        FitValue::UInt16(n) | FitValue::UInt16z(n) => Some(*n as i64),
        FitValue::SInt32(n) => Some(*n as i64),
        FitValue::UInt32(n) | FitValue::UInt32z(n) => Some(*n as i64),
        FitValue::SInt64(n) => Some(*n),
        FitValue::UInt64(n) | FitValue::UInt64z(n) => Some(*n as i64),
        FitValue::Float32(f) => Some(*f as i64),
        FitValue::Float64(f) => Some(*f as i64),
        _ => None,
    }
}

fn coerce_double(v: &FitValue) -> Option<f64> {
    match v {
        FitValue::Float32(f) => Some(*f as f64),
        FitValue::Float64(f) => Some(*f),
        FitValue::SInt8(n) => Some(*n as f64),
        FitValue::UInt8(n) | FitValue::Byte(n) | FitValue::UInt8z(n) | FitValue::Enum(n) => {
            Some(*n as f64)
        }
        FitValue::SInt16(n) => Some(*n as f64),
        FitValue::UInt16(n) | FitValue::UInt16z(n) => Some(*n as f64),
        FitValue::SInt32(n) => Some(*n as f64),
        FitValue::UInt32(n) | FitValue::UInt32z(n) => Some(*n as f64),
        FitValue::SInt64(n) => Some(*n as f64),
        FitValue::UInt64(n) | FitValue::UInt64z(n) => Some(*n as f64),
        _ => None,
    }
}

fn stringify(v: &FitValue) -> String {
    match v {
        FitValue::String(s) => s.clone(),
        FitValue::Array(_) => serde_json::to_string(v).unwrap_or_default(),
        FitValue::Invalid => String::new(),
        other => other.to_string(),
    }
}

fn naive_to_duck_ts(dt: NaiveDateTime) -> DuckValue {
    // DuckDB stores TIMESTAMP as microseconds since UNIX epoch, TimeUnit::Microsecond.
    let secs = dt.and_utc().timestamp();
    let nanos = dt.and_utc().timestamp_subsec_nanos() as i64;
    let micros = secs.saturating_mul(1_000_000) + nanos / 1_000;
    DuckValue::Timestamp(duckdb::types::TimeUnit::Microsecond, micros)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::fit_schema::init_schema;
    use std::path::PathBuf;

    fn fixture_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("sample.fit")
    }

    fn open_with_schema() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    #[test]
    fn ingest_inserts_known_messages() {
        let mut conn = open_with_schema();
        ingest_activity(&mut conn, "test-act-1", &fixture_path()).expect("ingest ok");

        // file_id should have at least 1 row.
        let n: i64 = conn
            .query_row(
                "SELECT count(*) FROM file_id WHERE activity_id = 'test-act-1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(n >= 1, "expected ≥1 file_id row, got {n}");

        // Activity-level summary should exist (Apple Fitness exports include session).
        let session_n: i64 = conn
            .query_row(
                "SELECT count(*) FROM session WHERE activity_id = 'test-act-1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(session_n >= 1, "expected ≥1 session row, got {session_n}");

        // file_id.time_created should parse as a TIMESTAMP, not NULL.
        let has_ts: bool = conn
            .query_row(
                "SELECT count(*) > 0 FROM file_id \
                 WHERE activity_id = 'test-act-1' AND time_created IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(has_ts, "file_id.time_created should be non-null");
    }

    #[test]
    fn ingest_is_idempotent() {
        let mut conn = open_with_schema();
        ingest_activity(&mut conn, "act-idem", &fixture_path()).unwrap();
        let count_after_first: i64 = conn
            .query_row(
                "SELECT \
                   (SELECT count(*) FROM file_id WHERE activity_id = 'act-idem') + \
                   (SELECT count(*) FROM session WHERE activity_id = 'act-idem') + \
                   (SELECT count(*) FROM event   WHERE activity_id = 'act-idem')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        ingest_activity(&mut conn, "act-idem", &fixture_path()).unwrap();
        let count_after_second: i64 = conn
            .query_row(
                "SELECT \
                   (SELECT count(*) FROM file_id WHERE activity_id = 'act-idem') + \
                   (SELECT count(*) FROM session WHERE activity_id = 'act-idem') + \
                   (SELECT count(*) FROM event   WHERE activity_id = 'act-idem')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            count_after_first, count_after_second,
            "second ingest must replace, not duplicate"
        );
    }

    #[test]
    #[ignore]
    fn inspect_live_duckdb() {
        // run: cargo test --release inspect_live_duckdb -- --ignored --nocapture
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("state/activities.duckdb");
        if !path.exists() {
            eprintln!("no live duckdb at {}", path.display());
            return;
        }
        let conn = Connection::open(&path).unwrap();
        for table in [
            "file_id",
            "session",
            "lap",
            "record",
            "event",
            "device_info",
            "hr",
            "activity",
            "unknown_fields",
        ] {
            let n: i64 = conn
                .query_row(&format!("SELECT count(*) FROM \"{table}\""), [], |r| r.get(0))
                .unwrap_or(-1);
            println!("{table:>16} : {n}");
        }
        println!("--- top 5 sports ---");
        let mut s = conn
            .prepare(
                "SELECT sport, count(*) c FROM session GROUP BY 1 ORDER BY c DESC LIMIT 5",
            )
            .unwrap();
        let rows = s
            .query_map([], |r| {
                Ok((r.get::<_, Option<String>>(0)?, r.get::<_, i64>(1)?))
            })
            .unwrap();
        for r in rows {
            let (k, v) = r.unwrap();
            println!("  {:?} : {}", k, v);
        }
        println!("--- avg session distance (m) ---");
        let avg: Option<f64> = conn
            .query_row("SELECT AVG(total_distance) FROM session", [], |r| r.get(0))
            .unwrap_or(None);
        println!("  {avg:?}");
        println!("--- record latitude sanity (degrees) ---");
        let lat_min: Option<f64> = conn
            .query_row("SELECT MIN(position_lat) FROM record", [], |r| r.get(0))
            .unwrap_or(None);
        let lat_max: Option<f64> = conn
            .query_row("SELECT MAX(position_lat) FROM record", [], |r| r.get(0))
            .unwrap_or(None);
        println!("  min={lat_min:?}  max={lat_max:?}");
    }

    #[test]
    #[ignore] // run with `cargo test ingest_large -- --ignored` if a larger fit is available
    fn ingest_large_activity_smoke() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("library/activities/fit/17146955427__West Lake Loop Hike.fit");
        if !path.exists() {
            eprintln!("skipping: fixture not present: {}", path.display());
            return;
        }
        let mut conn = open_with_schema();
        ingest_activity(&mut conn, "big-act", &path).unwrap();
        let n_record: i64 = conn
            .query_row(
                "SELECT count(*) FROM record WHERE activity_id = 'big-act'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(n_record > 100, "expected many record rows, got {n_record}");
        // Sanity: latitude must be a finite degree value, not raw semicircles.
        let lat: Option<f64> = conn
            .query_row(
                "SELECT position_lat FROM record \
                 WHERE activity_id = 'big-act' AND position_lat IS NOT NULL LIMIT 1",
                [],
                |r| r.get(0),
            )
            .ok();
        if let Some(lat) = lat {
            assert!(
                (-90.0..=90.0).contains(&lat),
                "lat {lat} out of degree range"
            );
        }
    }

    #[test]
    fn ingest_isolates_activities() {
        let mut conn = open_with_schema();
        ingest_activity(&mut conn, "act-A", &fixture_path()).unwrap();
        let n_a_before: i64 = conn
            .query_row(
                "SELECT count(*) FROM file_id WHERE activity_id = 'act-A'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        // Re-ingest a *different* activity_id; act-A rows must persist.
        ingest_activity(&mut conn, "act-B", &fixture_path()).unwrap();
        let n_a_after: i64 = conn
            .query_row(
                "SELECT count(*) FROM file_id WHERE activity_id = 'act-A'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n_a_before, n_a_after);
        let n_b: i64 = conn
            .query_row(
                "SELECT count(*) FROM file_id WHERE activity_id = 'act-B'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(n_b >= 1);
    }
}
