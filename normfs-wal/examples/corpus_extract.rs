//! Pulls real records out of a queue's WAL files into a flat corpus.
//!
//!   corpus_extract <queue_wal_dir> <out_file> <max_mb>
//!
//! Read-only: a benchmark fed synthetic bytes measures the wrong thing twice
//! over. Random data is incompressible, so it hides what compression saves on
//! the store pass; a repeated buffer is compressible to nothing, so it hides
//! what the disk is really asked to write. Only the station's own records have
//! the right size distribution and the right entropy.
//!
//! Output is `<u32 le len><bytes>` per record.

use std::io::Write;
use std::path::Path;

use normfs_types::DataSource;
use tokio::sync::mpsc;
use uintn::{UintN, paths};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("usage: corpus_extract <queue_wal_dir> <out_file> <max_mb>");
        std::process::exit(2);
    }
    let wal_dir = Path::new(&args[0]);
    let out_path = &args[1];
    let cap: u64 = args[2].parse::<u64>().unwrap() * 1024 * 1024;

    let ids = paths::get_files_ids(wal_dir, "wal").expect("listing wal files");
    eprintln!("{} wal file(s) under {}", ids.len(), wal_dir.display());

    let mut out = std::io::BufWriter::new(std::fs::File::create(out_path).unwrap());
    let mut written: u64 = 0;
    let mut records: u64 = 0;
    let mut smallest = usize::MAX;
    let mut largest = 0usize;

    let fs = normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap();
    for id in ids {
        if written >= cap {
            break;
        }
        let (tx, mut rx) = mpsc::channel(256);
        let dir = wal_dir.to_path_buf();
        let fid = id.clone();
        let fs = fs.clone();
        let reader = tokio::spawn(async move {
            let _ = normfs_wal::read_wal_file_range(
                &fs,
                &dir,
                &fid,
                &UintN::zero(),
                &None,
                1,
                &tx,
                DataSource::DiskWal,
            )
            .await;
        });
        while let Some(entry) = rx.recv().await {
            let len = entry.data.len();
            if len == 0 {
                continue;
            }
            out.write_all(&(len as u32).to_le_bytes()).unwrap();
            out.write_all(&entry.data).unwrap();
            written += 4 + len as u64;
            records += 1;
            smallest = smallest.min(len);
            largest = largest.max(len);
            if written >= cap {
                break;
            }
        }
        let _ = reader.await;
    }
    out.flush().unwrap();
    println!(
        "{records} records, {:.1} MiB, sizes {}..{} B, mean {} B",
        written as f64 / (1024.0 * 1024.0),
        if smallest == usize::MAX { 0 } else { smallest },
        largest,
        if records > 0 {
            (written - records * 4) / records
        } else {
            0
        }
    );
}
