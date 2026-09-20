//! What a queue actually sustains on this disk, per record size.
//!
//!   sd_profile <dir> <secs> <write_interval_ms> <payload>...
//!
//! Writes through the pooled path -- `PagePool::place` then `enqueue_pooled` --
//! because that is the one NormFS uses: the bytes reach the file from the page
//! they were accepted into, and rotation is decided at enqueue time.
//!
//! The reported rate is what the ack channel confirms durable, not what
//! `enqueue` accepted. Accepting is memcpy; a rate quoted from it measures RAM.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use normfs_types::{QueueId, QueueIdResolver};
use normfs_wal::{PagePool, WalHeader, WalSettings, WalStore, max_record_len};
use tokio::sync::mpsc;
use uintn::UintN;

fn settings(interval_ms: u64) -> WalSettings {
    WalSettings {
        max_file_size: 64 * 1024 * 1024,
        write_buffer_size: 4 * 1024 * 1024,
        write_interval: Duration::from_millis(interval_ms),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::None,
        compression_type: normfs_types::CompressionType::None,
        ..Default::default()
    }
}

struct Row {
    payload: usize,
    accepted: u64,
    durable: u64,
    secs: f64,
    page_size: usize,
}

async fn measure(root: &Path, payload: usize, secs: u64, interval_ms: u64) -> Row {
    // Wide enough for the record plus its framing, and never below the page
    // size NormFS defaults to, so small records see the real page count.
    let mut page_size = 256 * 1024;
    while max_record_len(page_size) < payload {
        page_size *= 2;
    }
    // A fixed memory budget per queue rather than a fixed page count: the point
    // is what the disk does, and a 2 MiB record with 16 pages would be
    // measuring an 8 MiB queue against a 4 MiB one.
    let pages = (16 * 1024 * 1024 / page_size).max(4);

    let (written_tx, mut written_rx) = mpsc::unbounded_channel();
    let (complete_tx, _complete_rx) = mpsc::unbounded_channel();
    let store = WalStore::new(root, written_tx, complete_tx);

    let queue_id: QueueId = QueueIdResolver::new("sdprofile").resolve(&format!("p{payload}"));
    let pool = Arc::new(PagePool::new(pages, page_size, 0));
    store
        .start_writer_with_pool(
            &queue_id,
            &UintN::from(1u64),
            WalHeader::default(),
            settings(interval_ms),
            None,
            Some(Arc::clone(&pool)),
        )
        .await
        .unwrap();

    let record = Bytes::from(vec![0xABu8; payload]);
    let deadline = Instant::now() + Duration::from_secs(secs);
    let start = Instant::now();
    let mut accepted: u64 = 0;
    let mut durable: u64 = 0;

    while Instant::now() < deadline {
        // `place` is the back-pressure point: it waits for a page when the pool
        // is full, which is exactly what a producer on a slow card feels.
        let placement = match pool.place(accepted, &record).await {
            Ok(p) => p,
            Err(e) => {
                eprintln!("place refused at {accepted}: {e:?}");
                break;
            }
        };
        if store
            .enqueue_pooled(&queue_id, UintN::from(accepted), Bytes::new(), placement)
            .is_err()
        {
            break;
        }
        accepted += 1;

        while let Ok((_, id)) = written_rx.try_recv() {
            if let Ok(n) = id.to_u64() {
                durable = durable.max(n + 1);
            }
        }
    }
    let secs_elapsed = start.elapsed().as_secs_f64();

    // The close runs the last flush, so anything still owed lands here.
    let _ = store.close().await;
    while let Ok((_, id)) = written_rx.try_recv() {
        if let Ok(n) = id.to_u64() {
            durable = durable.max(n + 1);
        }
    }

    Row {
        payload,
        accepted,
        durable,
        secs: secs_elapsed,
        page_size,
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 4 {
        eprintln!("usage: sd_profile <dir> <secs> <write_interval_ms> <payload>...");
        std::process::exit(2);
    }
    let dir = args[0].clone();
    let secs: u64 = args[1].parse().unwrap();
    let interval: u64 = args[2].parse().unwrap();
    let payloads: Vec<usize> = args[3..].iter().filter_map(|a| a.parse().ok()).collect();

    println!("normfs-wal {} on {dir}", env!("CARGO_PKG_VERSION"));
    println!("{secs}s per size, write_interval {interval} ms, fsync on\n");
    println!(
        "{:>9} | {:>10} | {:>10} | {:>9} | {:>9} | {:>8}",
        "payload", "rec/s acc", "rec/s dur", "MB/s dur", "page", "lag"
    );
    println!(
        "{:->9}-+-{:->10}-+-{:->10}-+-{:->9}-+-{:->9}-+-{:->8}",
        "", "", "", "", "", ""
    );

    for payload in payloads {
        let run = format!("{dir}/sdprofile-{payload}");
        let _ = std::fs::remove_dir_all(&run);
        std::fs::create_dir_all(&run).unwrap();
        let r = measure(Path::new(&run), payload, secs, interval).await;
        let on_disk = (payload + 6) as f64;
        println!(
            "{:>9} | {:>10.0} | {:>10.0} | {:>9.2} | {:>8}K | {:>8}",
            r.payload,
            r.accepted as f64 / r.secs,
            r.durable as f64 / r.secs,
            r.durable as f64 * on_disk / r.secs / 1e6,
            r.page_size / 1024,
            r.accepted.saturating_sub(r.durable),
        );
        let _ = std::fs::remove_dir_all(&run);
    }
}
