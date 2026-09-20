//! Pulls real records out of a queue's *store* files into a flat corpus.
//!
//!   store_corpus <instance_root> <instance_id> <queue_path> <out_file> <max_mb>
//!
//! The WAL keeps records only until the store worker archives them, so a queue
//! that has been running a while has nothing left there. The store copy is
//! compressed and encrypted, which is why this needs the instance's seed --
//! `CryptoContext::open` reads it from the instance root.
//!
//! Read-only apart from `PersistStore::new` ensuring `tmp/` exists, which it
//! already does on a live instance. No writer is started.
//!
//! Output is `<u32 le len><bytes>` per record, matching corpus_extract.

use std::io::Write;
use std::sync::Arc;

use normfs_store::{PersistStore, StoreWriteConfig};
use normfs_types::{DataSource, QueueIdResolver};
use normfs_wal::WalStore;
use tokio::sync::mpsc;
use uintn::{UintN, paths};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 5 {
        eprintln!("usage: store_corpus <instance_root> <instance_id> <queue_path> <out> <max_mb>");
        std::process::exit(2);
    }
    let root = std::path::Path::new(&args[0]);
    let queue = QueueIdResolver::new(&args[1]).resolve(&args[2]);
    let out_path = &args[3];
    let cap: u64 = args[4].parse::<u64>().unwrap() * 1024 * 1024;

    let crypto = Arc::new(normfs_crypto::CryptoContext::open(root).expect("seed"));
    let (wtx, _wrx) = mpsc::unbounded_channel();
    let (ctx_, _crx) = mpsc::unbounded_channel();
    let wal = Arc::new(WalStore::new(root, wtx, ctx_));
    let fs = wal.fs().clone();
    let store = PersistStore::new(
        root,
        StoreWriteConfig {
            num_workers: 1,
            verify_signatures: false,
        },
        crypto,
        wal,
        mpsc::unbounded_channel().0,
    );

    let store_dir = queue.to_store_dir(root);
    let ids = paths::get_files_ids(&store_dir, "store").expect("listing store files");
    eprintln!("{} store file(s) under {}", ids.len(), store_dir.display());

    // Beside the output, not in temp_dir: on a station /tmp is a tmpfs that
    // gets swept while this runs, and a multi-hour extraction died on it.
    let stage_dir = format!("{out_path}.stage");
    std::fs::create_dir_all(&stage_dir).unwrap();
    let stage_file = format!("{stage_dir}/001.wal");

    let mut out = std::io::BufWriter::new(std::fs::File::create(out_path).unwrap());
    let (mut written, mut records) = (0u64, 0u64);
    let (mut smallest, mut largest) = (usize::MAX, 0usize);

    for id in ids {
        if written >= cap {
            break;
        }
        let Ok(Some(raw)) = store.get_store_bytes(&queue, &id).await else {
            continue;
        };
        let wal_bytes = match store.extract_wal_bytes(&queue, &id, raw, false) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("file {id}: {e:?}");
                continue;
            }
        };
        // `read_wal_bytes_range` is not exported from normfs-wal, and adding an
        // export for a benchmark would put a public API change in an unrelated
        // PR. Staging the decoded bytes as a file costs one write per store
        // file and uses the shipped reader unchanged.
        std::fs::write(&stage_file, &wal_bytes).unwrap();
        let (tx, mut rx) = mpsc::channel(256);
        let stage_dir_c = stage_dir.clone();
        let fs = fs.clone();
        let reader = tokio::spawn(async move {
            let _ = normfs_wal::read_wal_file_range(
                &fs,
                std::path::Path::new(&stage_dir_c),
                &UintN::from(1u64),
                &UintN::zero(),
                &None,
                1,
                &tx,
                DataSource::DiskStore,
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
    let _ = std::fs::remove_dir_all(&stage_dir);
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
