//! End-to-end write cost of a whole NormFS instance, per record size.
//!
//!   fs_profile <dir> <secs> <payload>...
//!
//! Through `NormFS::enqueue`, so the figure includes everything a queue really
//! pays for: the memory store, the page pool's back-pressure, the WAL, and the
//! store worker that afterwards re-reads each closed file, compresses it,
//! encrypts it and writes it again. That second write is why a WAL-only
//! measurement flatters the disk.
//!
//! `enqueue` awaits a page, so over a run long enough to fill the pool the
//! accepted rate is the sustained rate -- the disk sets it, not the memcpy.

use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings};

/// Records as the station actually wrote them: `<u32 le len><bytes>`.
///
/// Synthetic payloads get this wrong in both directions. Random bytes are
/// incompressible, so the store pass writes as much as the WAL and compression
/// looks worthless; one buffer repeated compresses to nothing, so the disk
/// looks idle. Real records carry the size distribution and the entropy the
/// card will actually see.
fn load_corpus(path: &str) -> Vec<Bytes> {
    // One shared buffer, sliced: a copy per record doubled the corpus in
    // memory, which on a 2 GB rover beside the station was an OOM kill.
    // `PROFILE_CORPUS_MB` caps how much of the file is loaded.
    let cap = std::env::var("PROFILE_CORPUS_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(96)
        * 1024
        * 1024;
    let mut raw = std::fs::read(path).expect("corpus");
    raw.truncate(cap);
    let raw = Bytes::from(raw);
    let mut out = Vec::new();
    let mut i = 0;
    while i + 4 <= raw.len() {
        let len = u32::from_le_bytes([raw[i], raw[i + 1], raw[i + 2], raw[i + 3]]) as usize;
        i += 4;
        if i + len > raw.len() {
            break;
        }
        out.push(raw.slice(i..i + len));
        i += len;
    }
    out
}

/// `PROFILE_PERSIST=wal|store|cloud` picks the pipeline every queue runs;
/// cloud reads the S3 environment the integration tests use.
fn apply_persist(settings: &mut NormFsSettings) -> &'static str {
    use normfs::{Persist, QueueSettings};
    let mode = std::env::var("PROFILE_PERSIST").unwrap_or_else(|_| "wal".into());
    let persist = match mode.as_str() {
        "store" => Persist::STORE,
        "cloud" => Persist::CLOUD,
        _ => Persist::WAL_STORE,
    };
    // The queue config overrides the WAL settings' codec, so it has to carry
    // the run's codec too or every mode silently runs zstd + AES.
    settings.queue_settings = QueueSettings::all_active().with_default_persist(persist);
    settings.queue_settings.default_config.compression_type =
        settings.wal_settings.compression_type;
    settings.queue_settings.default_config.encryption_type = settings.wal_settings.encryption_type;
    if persist.cloud {
        let endpoint = std::env::var("S3_ENDPOINT_URL").expect("S3_ENDPOINT_URL for cloud mode");
        let prefix = format!(
            "bench-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        settings.cloud_settings = Some(normfs::CloudSettings {
            endpoint,
            bucket: std::env::var("S3_BUCKET").unwrap_or_else(|_| "normfs-bench".into()),
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
            access_key: std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID"),
            secret_key: std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY"),
            prefix,
        });
    }
    if let Some(kb) = std::env::var("PROFILE_PAGE_KB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        settings.mem_page_size = kb * 1024;
    }
    // `PROFILE_FILE_KB` sizes the WAL file and its write buffer together, as
    // the station does: 128 MB of each is a third of a rover's memory.
    if let Some(kb) = std::env::var("PROFILE_FILE_KB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        settings.wal_settings.max_file_size = kb * 1024;
        settings.wal_settings.write_buffer_size =
            (kb * 1024).min(settings.wal_settings.write_buffer_size);
    }
    // `PROFILE_FSYNC=0` is the ceiling: what the path costs before the disk
    // is asked to promise anything.
    if std::env::var("PROFILE_FSYNC").ok().as_deref() == Some("0") {
        settings.wal_settings.enable_fsync = false;
        settings.queue_settings.default_config.enable_fsync = false;
    }
    match persist {
        Persist::STORE => "store",
        Persist::CLOUD => "cloud",
        _ => "wal",
    }
}

async fn ensure_bucket(settings: &NormFsSettings) {
    if let Some(c) = &settings.cloud_settings {
        let client = normfs_cloud::S3Client::new(
            url::Url::parse(&c.endpoint).unwrap(),
            c.bucket.clone(),
            c.region.clone(),
            c.access_key.clone(),
            c.secret_key.clone(),
        )
        .unwrap();
        client.create_bucket().await.expect("bucket");
    }
}

fn dir_files(p: &Path, ext: &str) -> u64 {
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                n += dir_files(&path, ext);
            } else if path.extension().and_then(|s| s.to_str()) == Some(ext) {
                n += 1;
            }
        }
    }
    n
}

fn page_for(payload: usize) -> usize {
    let mut p = 256 * 1024;
    while p < payload * 2 {
        p *= 2;
    }
    p
}

/// Sectors written to the backing device, from /proc/diskstats.
///
/// The store worker lags the writer, so weighing the directory at close counts
/// neither what it still owes nor the read-and-rewrite it performs. What the
/// card actually saw is the only figure that answers "does this fit".
fn device_written_bytes(dev: &str) -> u64 {
    let Ok(s) = std::fs::read_to_string("/proc/diskstats") else {
        return 0;
    };
    for line in s.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() > 9 && f[2] == dev {
            return f[9].parse::<u64>().unwrap_or(0) * 512;
        }
    }
    0
}

fn dir_bytes(p: &Path) -> u64 {
    let mut total = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let Ok(md) = e.metadata() else { continue };
            total += if md.is_dir() {
                dir_bytes(&e.path())
            } else {
                md.len()
            };
        }
    }
    total
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("usage: fs_profile <dir> <secs> <payload>...");
        std::process::exit(2);
    }
    let dir = args[0].clone();
    let secs: u64 = args[1].parse().unwrap();
    let payloads: Vec<usize> = args[2..].iter().filter_map(|a| a.parse().ok()).collect();

    let codec = match std::env::var("PROFILE_CODEC")
        .unwrap_or_else(|_| "raw".into())
        .as_str()
    {
        "zstd" => (
            normfs_types::CompressionType::Zstd,
            normfs_types::EncryptionType::Aes,
        ),
        _ => (
            normfs_types::CompressionType::None,
            normfs_types::EncryptionType::None,
        ),
    };
    println!("codec: {:?} / {:?}", codec.0, codec.1);

    println!(
        "NormFS end-to-end on {dir}, {secs}s per size, interval={} mem={}MB",
        std::env::var("PROFILE_INTERVAL_MS").unwrap_or_else(|_| "50".into()),
        std::env::var("PROFILE_MEM_MB").unwrap_or_else(|_| "64".into())
    );
    println!(
        "{:>5} | {:>9} | {:>10} | {:>9} | {:>9} | {:>9} | {:>6} | {:>7} | {:>8} | {:>7} | {:>5}",
        "mode",
        "payload",
        "rec/s",
        "MB/s in",
        "dev write",
        "dev cycle",
        "amp",
        "page",
        "disk MB",
        "files",
        "read"
    );

    for payload in payloads {
        let corpus: Vec<bytes::Bytes> = match std::env::var("PROFILE_CORPUS") {
            Ok(p) => load_corpus(&p),
            Err(_) => Vec::new(),
        };
        let widest = corpus.iter().map(|b| b.len()).max().unwrap_or(payload);
        let mean = if corpus.is_empty() {
            payload
        } else {
            corpus.iter().map(|b| b.len()).sum::<usize>() / corpus.len()
        };

        let root = format!("{dir}/fsprofile-{payload}");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        let mut settings = NormFsSettings::all_active();
        settings.mem_page_size = page_for(widest);
        settings.max_memory_usage = std::env::var("PROFILE_MEM_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64)
            * 1024
            * 1024;
        // The knob that decides how many bytes reach one fsync, which is what
        // an SD card charges for. Swept rather than assumed.
        if let Some(ms) = std::env::var("PROFILE_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
        {
            settings.wal_settings.write_interval = Duration::from_millis(ms);
        }
        settings.wal_settings.compression_type = codec.0;
        settings.wal_settings.encryption_type = codec.1;
        let mode = apply_persist(&mut settings);
        let page = settings.mem_page_size;
        ensure_bucket(&settings).await;

        let fs = NormFS::new(root.clone(), settings).await.unwrap();
        let queue = fs.resolve("profile");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();

        let mut x: u64 = 0x9E3779B97F4A7C15;
        let mut buf = Vec::with_capacity(payload);

        let dev = std::env::var("PROFILE_DEV").unwrap_or_else(|_| "mmcblk1".into());
        let dev_before = device_written_bytes(&dev);
        let cycle_start = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(secs);
        let start = Instant::now();
        let mut n: u64 = 0;
        let mut offered: u64 = 0;
        while Instant::now() < deadline {
            let rec = if corpus.is_empty() {
                buf.clear();
                for _ in 0..payload {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    buf.push((x >> 24) as u8);
                }
                Bytes::from(buf.clone())
            } else {
                corpus[(n as usize) % corpus.len()].clone()
            };
            offered += rec.len() as u64;
            if let Err(e) = fs.enqueue(&queue, rec).await {
                eprintln!("enqueue failed after {n} records: {e:?}");
                break;
            }
            n += 1;
        }
        let elapsed = start.elapsed().as_secs_f64();
        // Device bytes over the write phase alone. This is the figure `dd`
        // reports, and the only one comparable to it: the full-cycle rate below
        // is diluted by the drain, where the writer is idle and only the store
        // worker is moving bytes.
        let dev_write = device_written_bytes(&dev).saturating_sub(dev_before);
        // The mode's tail rule: store and cloud land the open page here.
        let _ = fs.flush_queue(&queue).await;
        // Read back the first, a middle and the last record through the real
        // reader, so a mode that lost or misnumbered records shows up here
        // rather than in a station log.
        let mut verified = 0u64;
        if n > 0 {
            for id in [0u64, n / 2, n - 1] {
                let (tx, mut rx) = tokio::sync::mpsc::channel(2);
                let want = uintn::UintN::from(id);
                let read = fs
                    .read(
                        &queue,
                        normfs::ReadPosition::Absolute(want.clone()),
                        1,
                        1,
                        tx,
                    )
                    .await;
                if read.is_ok() && rx.recv().await.is_some_and(|e| e.id == want) {
                    verified += 1;
                }
            }
        }
        let _ = fs.close().await;

        let mut last = dir_bytes(Path::new(&root));
        let mut stable = 0;
        let drain_cap = Instant::now() + Duration::from_secs(900);
        while stable < 5 && Instant::now() < drain_cap {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let now = dir_bytes(Path::new(&root));
            if now == last {
                stable += 1;
            } else {
                stable = 0;
                last = now;
            }
        }
        let cycle = cycle_start.elapsed().as_secs_f64();
        let dev_after = device_written_bytes(&dev);
        let on_disk = dir_bytes(Path::new(&root));
        let to_device = dev_after.saturating_sub(dev_before);
        let files = dir_files(Path::new(&root), "store") + dir_files(Path::new(&root), "wal");

        println!(
            "{:>5} | {:>9} | {:>10.1} | {:>9.2} | {:>9.2} | {:>9.2} | {:>6.2} | {:>6}K | {:>8.1} | {:>7} | {:>3}/3",
            mode,
            mean,
            n as f64 / elapsed,
            offered as f64 / elapsed / 1e6,
            dev_write as f64 / elapsed / 1e6,
            to_device as f64 / cycle / 1e6,
            if offered > 0 {
                to_device as f64 / offered as f64
            } else {
                0.0
            },
            page / 1024,
            on_disk as f64 / 1e6,
            files,
            verified,
        );
        use std::io::Write;
        let _ = std::io::stdout().flush();
        let _ = std::fs::remove_dir_all(&root);
    }
}
