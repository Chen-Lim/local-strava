use std::fs::File;
use std::io;
use std::path::Path;

use anyhow::{Context, Result};
use flate2::read::GzDecoder;

pub fn decompress_gzip(src_path: &Path, dest_path: &Path) -> Result<()> {
    let src = File::open(src_path)
        .with_context(|| format!("failed to open source file {}", src_path.display()))?;
    let mut decoder = GzDecoder::new(src);
    let mut dest = File::create(dest_path)
        .with_context(|| format!("failed to create destination {}", dest_path.display()))?;

    io::copy(&mut decoder, &mut dest)
        .with_context(|| format!("failed to decompress {}", src_path.display()))?;

    Ok(())
}
