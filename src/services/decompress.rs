use std::fs::File;
use std::io;
use std::path::Path;

use anyhow::{Context, Result, bail};
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

const STAGING_SENTINEL: &str = ".staging-ok";

/// Extract a Strava export zip archive into `dest_dir`.
///
/// Features:
/// - Sentinel file (`.staging-ok`) to mark completed extractions
/// - Incomplete directory cleanup (missing sentinel → re-extract)
/// - Zip-slip defense (rejects absolute paths and `..` traversal)
/// - Top-level directory normalization (strips single wrapper dir)
pub fn unzip_strava_archive(src_path: &Path, dest_dir: &Path) -> Result<()> {
    let sentinel = dest_dir.join(STAGING_SENTINEL);

    // Already extracted successfully — skip.
    if sentinel.is_file() {
        eprintln!(
            "staging already complete: {}, skipping extraction",
            dest_dir.display()
        );
        return Ok(());
    }

    // Incomplete previous attempt — clean up.
    if dest_dir.exists() {
        eprintln!(
            "removing incomplete staging directory: {}",
            dest_dir.display()
        );
        std::fs::remove_dir_all(dest_dir).with_context(|| {
            format!(
                "failed to remove incomplete staging dir {}",
                dest_dir.display()
            )
        })?;
    }

    std::fs::create_dir_all(dest_dir)
        .with_context(|| format!("failed to create staging dir {}", dest_dir.display()))?;

    let file = File::open(src_path)
        .with_context(|| format!("failed to open zip file {}", src_path.display()))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("failed to read zip archive {}", src_path.display()))?;

    // Detect top-level directory wrapper.
    // If all entries share a single common prefix directory, we strip it so that
    // `activities.csv` and `activities/` end up directly under dest_dir.
    let strip_prefix = detect_wrapper_prefix(&mut archive);

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).with_context(|| {
            format!("failed to read zip entry #{i} in {}", src_path.display())
        })?;

        let raw_name = match entry.enclosed_name() {
            Some(name) => name.to_path_buf(),
            None => {
                // enclosed_name() returns None for entries with absolute paths
                // or path traversal — this is zip-slip defense.
                bail!(
                    "zip-slip: rejected unsafe entry name in {}",
                    src_path.display()
                );
            }
        };

        // Apply prefix stripping.
        let relative = if let Some(ref prefix) = strip_prefix {
            match raw_name.strip_prefix(prefix) {
                Ok(stripped) => stripped.to_path_buf(),
                Err(_) => raw_name.clone(),
            }
        } else {
            raw_name.clone()
        };

        // Skip empty relative paths (the wrapper directory entry itself).
        if relative.as_os_str().is_empty() {
            continue;
        }

        let out_path = dest_dir.join(&relative);

        // Secondary zip-slip defense: ensure resolved path stays within dest_dir.
        let canonical_dest = dest_dir
            .canonicalize()
            .unwrap_or_else(|_| dest_dir.to_path_buf());
        // For the output path we can't canonicalize (it doesn't exist yet),
        // so we check that it starts with dest_dir after joining.
        if !out_path.starts_with(dest_dir) {
            bail!(
                "zip-slip: entry {:?} would escape staging directory",
                raw_name
            );
        }
        // Also ensure that the canonical form doesn't escape.
        if let Ok(canonical_out) = out_path.canonicalize() {
            if !canonical_out.starts_with(&canonical_dest) {
                bail!(
                    "zip-slip: entry {:?} resolves outside staging directory",
                    raw_name
                );
            }
        }

        if entry.is_dir() {
            std::fs::create_dir_all(&out_path).with_context(|| {
                format!("failed to create directory {}", out_path.display())
            })?;
        } else {
            // Ensure parent directory exists.
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create parent directory {}", parent.display())
                })?;
            }
            let mut out_file = File::create(&out_path).with_context(|| {
                format!("failed to create file {}", out_path.display())
            })?;
            io::copy(&mut entry, &mut out_file).with_context(|| {
                format!("failed to write file {}", out_path.display())
            })?;
        }
    }

    // Write sentinel to mark successful extraction.
    File::create(&sentinel)
        .with_context(|| format!("failed to write staging sentinel {}", sentinel.display()))?;

    eprintln!(
        "extracted {} to {}",
        src_path.display(),
        dest_dir.display()
    );
    Ok(())
}

/// Detect if all zip entries share a single top-level directory prefix.
/// Returns `Some(prefix)` if so, `None` otherwise.
fn detect_wrapper_prefix(archive: &mut zip::ZipArchive<File>) -> Option<std::path::PathBuf> {
    let mut common_prefix: Option<String> = None;

    for i in 0..archive.len() {
        let entry = match archive.by_index_raw(i) {
            Ok(e) => e,
            Err(_) => return None,
        };
        let name = entry.name();

        // Split the entry name into its first component.
        let first_component = match name.split('/').next() {
            Some(c) if !c.is_empty() => c.to_string(),
            _ => return None,
        };

        match &common_prefix {
            None => common_prefix = Some(first_component),
            Some(existing) => {
                if *existing != first_component {
                    // Multiple top-level items → no wrapper directory.
                    return None;
                }
            }
        }
    }

    // Only strip if the common prefix is a directory (i.e., entries like "prefix/something").
    // If the zip only contains files at the root with the same prefix string, don't strip.
    let prefix = common_prefix?;

    // Verify that at least one entry has content beyond just the prefix.
    for i in 0..archive.len() {
        if let Ok(entry) = archive.by_index_raw(i) {
            let name = entry.name();
            if name.starts_with(&format!("{prefix}/")) && name.len() > prefix.len() + 1 {
                return Some(std::path::PathBuf::from(&prefix));
            }
        }
    }

    None
}

