//! Build the DuckDB analytical schema from the embedded Garmin FIT Profile.
//!
//! `assets/profile_messages.json` (the official Garmin FIT Profile, 21.x) is
//! the authoritative source for table names, field names, and field types.
//! `assets/profile_types.json` lets us classify enum-like types as VARCHAR.
//!
//! Conventions:
//! * One DuckDB table per FIT message kind, name = FIT message name (snake_case).
//! * First two columns are always injected: `activity_id VARCHAR`, `mesg_index INTEGER`,
//!   primary key `(activity_id, mesg_index)`.
//! * All field names are taken verbatim from the Profile (snake_case), except
//!   when they collide with an injected column — then prefixed with `fit_`.
//! * Type mapping: see `map_fit_type`.
//! * Array fields are stored as VARCHAR (JSON-encoded) for now — keeps DDL
//!   simple and SQL-compatible across DuckDB versions.
//! * Fields with units `semicircles` (lat/long) are stored as DOUBLE; ingest
//!   converts the int32 semicircle value to degrees.
//! * `_schema_meta`, `unknown_fields`, `developer_field`, and
//!   `developer_field_value` are auxiliary tables (not derived from Profile).

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use anyhow::{Context, Result};
use duckdb::Connection;
use indexmap::IndexMap;
use serde::Deserialize;

const PROFILE_MESSAGES_JSON: &str = include_str!("../../assets/profile_messages.json");
const PROFILE_TYPES_JSON: &str = include_str!("../../assets/profile_types.json");

/// Profile version recorded in `_schema_meta`. Keep in lock-step with the
/// asset file; bump when refreshing from a newer Garmin SDK release.
pub const PROFILE_VERSION: &str = "21.x (assets/profile_messages.json)";

const INJECTED_COLUMNS: &[&str] = &["activity_id", "mesg_index"];

/// DuckDB column type categories we care about during INSERT bind. The DDL
/// uses concrete SQL type strings (returned by `sql_str`); ingest uses this
/// enum to pick the right binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlType {
    Boolean,
    Integer,
    BigInt,
    Double,
    Varchar,
    Timestamp,
}

impl SqlType {
    pub fn sql_str(&self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN",
            Self::Integer => "INTEGER",
            Self::BigInt => "BIGINT",
            Self::Double => "DOUBLE",
            Self::Varchar => "VARCHAR",
            Self::Timestamp => "TIMESTAMP",
        }
    }
}

/// One column in a table (excluding the injected `activity_id` / `mesg_index`).
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name as written in DuckDB (already de-collisioned).
    pub name: String,
    /// FIT field name from the Profile (pre-rename), used to match incoming
    /// field values during ingest.
    pub fit_name: String,
    pub sql_type: SqlType,
    /// True if `units == "semicircles"` (ingest converts int32 → degrees).
    pub is_semicircle: bool,
    /// True if the Profile declares this field as an array.
    pub is_array: bool,
}

/// One DuckDB table derived from a FIT message.
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

/// The full FIT-derived schema (123 message tables).
#[derive(Debug)]
pub struct Schema {
    pub tables: IndexMap<String, Table>,
}

impl Schema {
    pub fn table(&self, mesg_name: &str) -> Option<&Table> {
        self.tables.get(mesg_name)
    }
}

#[derive(Debug, Deserialize)]
struct MessageDef {
    #[serde(default)]
    fields: Vec<FieldDef>,
}

#[derive(Debug, Deserialize)]
struct FieldDef {
    name: String,
    #[serde(rename = "type")]
    fit_type: String,
    #[serde(default)]
    array: Option<serde_json::Value>,
    #[serde(default)]
    units: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TypeDef {
    #[serde(rename = "baseType")]
    base_type: String,
}

/// Lazily-built process-wide schema. Cheap (~1 ms) but only needs to happen once.
pub fn schema() -> &'static Schema {
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA.get_or_init(build_schema)
}

fn build_schema() -> Schema {
    let messages: IndexMap<String, MessageDef> =
        serde_json::from_str(PROFILE_MESSAGES_JSON).expect("profile_messages.json is valid");
    let types: HashMap<String, TypeDef> =
        serde_json::from_str(PROFILE_TYPES_JSON).expect("profile_types.json is valid");

    let mut tables: IndexMap<String, Table> = IndexMap::with_capacity(messages.len());

    for (mesg_name, mesg) in &messages {
        let mut columns: Vec<Column> = Vec::with_capacity(mesg.fields.len());
        let mut seen: HashSet<String> = INJECTED_COLUMNS.iter().map(|s| s.to_string()).collect();

        for f in &mesg.fields {
            let mut name = f.name.clone();
            if INJECTED_COLUMNS.contains(&name.as_str()) {
                name = format!("fit_{name}");
            }
            if !seen.insert(name.clone()) {
                continue;
            }

            let is_array = f
                .array
                .as_ref()
                .map(|v| !v.is_null())
                .unwrap_or(false);
            let is_semicircle = matches!(f.units.as_deref(), Some("semicircles"));

            let sql_type = if is_array {
                SqlType::Varchar
            } else if is_semicircle {
                SqlType::Double
            } else {
                base_type_to_sql(&f.fit_type, &types)
            };

            columns.push(Column {
                name,
                fit_name: f.name.clone(),
                sql_type,
                is_semicircle,
                is_array,
            });
        }

        tables.insert(
            mesg_name.clone(),
            Table {
                name: mesg_name.clone(),
                columns,
            },
        );
    }

    Schema { tables }
}

fn base_type_to_sql(fit_type: &str, types: &HashMap<String, TypeDef>) -> SqlType {
    match fit_type {
        "bool" => SqlType::Boolean,
        "date_time" | "local_date_time" => SqlType::Timestamp,
        "float32" | "float64" => SqlType::Double,
        "sint64" | "uint64" | "uint64z" => SqlType::BigInt,
        "sint8" | "uint8" | "uint8z" | "sint16" | "uint16" | "uint16z" | "sint32" | "uint32"
        | "uint32z" | "enum" => SqlType::Integer,
        "string" => SqlType::Varchar,
        "byte" => SqlType::Varchar,
        other => match types.get(other) {
            Some(t) => match t.base_type.as_str() {
                "bool" => SqlType::Boolean,
                "float32" | "float64" => SqlType::Double,
                "sint64" | "uint64" | "uint64z" => SqlType::BigInt,
                _ => SqlType::Varchar,
            },
            None => SqlType::Varchar,
        },
    }
}

/// Idempotently install the full FIT analytical schema in `conn`.
pub fn init_schema(conn: &Connection) -> Result<()> {
    let s = schema();
    let mut ddl = String::new();
    ddl.push_str(meta_ddl());
    ddl.push_str(aux_ddl());
    for table in s.tables.values() {
        ddl.push_str(&table_ddl(table));
    }

    conn.execute_batch(&ddl)
        .context("failed to execute FIT schema DDL")?;

    conn.execute_batch(&format!(
        "INSERT INTO _schema_meta (key, value) VALUES ('profile_version', '{}') \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value;",
        PROFILE_VERSION.replace('\'', "''")
    ))
    .context("failed to record profile_version in _schema_meta")?;

    Ok(())
}

fn table_ddl(t: &Table) -> String {
    let mut cols: Vec<String> = vec![
        "    activity_id VARCHAR NOT NULL".to_string(),
        "    mesg_index  INTEGER NOT NULL".to_string(),
    ];
    for c in &t.columns {
        cols.push(format!("    \"{}\" {}", c.name, c.sql_type.sql_str()));
    }
    cols.push("    PRIMARY KEY (activity_id, mesg_index)".to_string());
    format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" (\n{}\n);\n",
        t.name,
        cols.join(",\n")
    )
}

fn meta_ddl() -> &'static str {
    "CREATE TABLE IF NOT EXISTS _schema_meta (
        key   VARCHAR PRIMARY KEY,
        value VARCHAR NOT NULL
    );\n"
}

fn aux_ddl() -> &'static str {
    "CREATE TABLE IF NOT EXISTS unknown_fields (
        activity_id     VARCHAR NOT NULL,
        mesg_index      INTEGER NOT NULL,
        mesg_name       VARCHAR NOT NULL,
        field_name      VARCHAR,
        field_def_num   INTEGER,
        value_json      VARCHAR
    );
    CREATE TABLE IF NOT EXISTS developer_field (
        activity_id          VARCHAR NOT NULL,
        developer_data_index INTEGER NOT NULL,
        field_def_num        INTEGER NOT NULL,
        field_name           VARCHAR,
        units                VARCHAR,
        fit_base_type_id     INTEGER
    );
    CREATE TABLE IF NOT EXISTS developer_field_value (
        activity_id          VARCHAR NOT NULL,
        mesg_index           INTEGER NOT NULL,
        developer_data_index INTEGER NOT NULL,
        field_def_num        INTEGER NOT NULL,
        value_json           VARCHAR
    );\n"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_loads_in_memory() {
        let conn = Connection::open_in_memory().expect("open in-memory duckdb");
        init_schema(&conn).expect("schema init should succeed");

        let n_tables: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.tables \
                 WHERE table_schema = 'main' AND table_name NOT LIKE 'sqlite_%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(n_tables >= 120, "expected >=120 tables, got {n_tables}");

        let cols: Vec<String> = conn
            .prepare(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = 'record' ORDER BY ordinal_position",
            )
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(cols[0], "activity_id");
        assert_eq!(cols[1], "mesg_index");
        assert!(cols.contains(&"timestamp".to_string()));
        assert!(cols.contains(&"heart_rate".to_string()));
        assert!(cols.contains(&"position_lat".to_string()));
        assert!(cols.contains(&"position_long".to_string()));
        assert!(cols.contains(&"enhanced_speed".to_string()));

        init_schema(&conn).expect("re-init should be idempotent");

        let v: String = conn
            .query_row(
                "SELECT value FROM _schema_meta WHERE key='profile_version'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(v, PROFILE_VERSION);
    }

    #[test]
    fn segment_leaderboard_entry_renames_activity_id() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        let cols: Vec<String> = conn
            .prepare(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name='segment_leaderboard_entry' ORDER BY ordinal_position",
            )
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(cols[0], "activity_id");
        assert!(cols.contains(&"fit_activity_id".to_string()));
        assert_eq!(
            cols.iter().filter(|c| c.as_str() == "activity_id").count(),
            1
        );
    }

    #[test]
    fn schema_record_has_semicircle_doubles() {
        let s = schema();
        let rec = s.table("record").unwrap();
        let lat = rec
            .columns
            .iter()
            .find(|c| c.name == "position_lat")
            .unwrap();
        assert_eq!(lat.sql_type, SqlType::Double);
        assert!(lat.is_semicircle);
        let ts = rec.columns.iter().find(|c| c.name == "timestamp").unwrap();
        assert_eq!(ts.sql_type, SqlType::Timestamp);
    }
}
