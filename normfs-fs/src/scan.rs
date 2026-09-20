//! Directory scans over the 3-hex-chunk layout, on an executor thread.
//!
//! The walk itself stays in `uintn::paths`, which three crates already agree
//! on; what changes is where it runs.

use std::io;
use std::path::Path;

use uintn::UintN;
use uintn::paths::{self, PathError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scan {
    Min,
    Max,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanResult {
    /// No file with the extension under the directory, or no directory.
    None,
    One(UintN),
    All(Vec<UintN>),
}

pub(crate) fn scan_ids(dir: &Path, ext: &str, which: Scan) -> io::Result<ScanResult> {
    if !dir.is_dir() {
        return Ok(ScanResult::None);
    }
    let one = |r: Result<UintN, PathError>| match r {
        Ok(id) => Ok(ScanResult::One(id)),
        Err(PathError::NoFilesFound) => Ok(ScanResult::None),
        Err(PathError::Io(e)) => Err(e),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
    };
    match which {
        Scan::Min => one(paths::find_min_id(dir, ext)),
        Scan::Max => one(paths::find_max_id(dir, ext)),
        Scan::All => match paths::get_files_ids(dir, ext) {
            Ok(ids) if ids.is_empty() => Ok(ScanResult::None),
            Ok(ids) => Ok(ScanResult::All(ids)),
            Err(PathError::NoFilesFound) => Ok(ScanResult::None),
            Err(PathError::Io(e)) => Err(e),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        },
    }
}
