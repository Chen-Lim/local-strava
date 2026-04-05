mod cli;
mod domain;
mod importers;
mod services;
mod storage;
mod utils;

use anyhow::Result;

fn main() -> Result<()> {
    cli::run()
}
