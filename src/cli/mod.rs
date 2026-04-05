use anyhow::{Context, Result, bail};
use std::env;

use crate::services;

pub fn run() -> Result<()> {
    let cwd = env::current_dir().context("failed to read current directory")?;
    let args: Vec<String> = env::args().collect();

    match args.get(1).map(String::as_str) {
        None | Some("sync") => {
            let batch = args.get(2).map(String::as_str);
            services::sync::run_sync(&cwd, batch)
        }
        Some("scan") => {
            let batches = crate::importers::strava::discover_batches(&cwd.join("inbox"))?;
            if batches.is_empty() {
                println!("No Strava export batches found in inbox/");
            } else {
                for batch in batches {
                    println!(
                        "{} -> {}",
                        batch.batch_name,
                        display_path(cwd.as_path(), batch.root.as_path())
                    );
                }
            }
            Ok(())
        }
        Some("bootstrap-legacy") => services::consistency::bootstrap_legacy_index(&cwd),
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
    println!("  cargo run -- bootstrap-legacy");
    println!();
    println!("Commands:");
    println!("  sync              Process one inbox batch or all batches when omitted");
    println!("  scan              List valid Strava export batches under inbox/");
    println!("  bootstrap-legacy  Seed state/activity_index.json from root activities.csv");
}
