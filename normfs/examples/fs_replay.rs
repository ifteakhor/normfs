//! Every queue at once, at production rates, from the station's own records.
//!
//!   fs_replay <dir> <secs> <corpus>:<hz>:<codec> ...
//!
//! One queue measured alone is the best case and not the case that matters: a
//! card interleaving writes across twelve files pays for garbage collection and
//! block rewrites that a single sequential stream never triggers. Production
//! runs them together, so the only honest answer to "does it fit" comes from
//! running them together.
//!
//! Each queue is paced at its target rate rather than run flat out. Falling
//! behind is the result: `achieved < target` means the card cannot hold the
//! workload, and the backlog says by how much.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings};

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

struct Stream {
    name: String,
    corpus: Vec<Bytes>,
    hz: f64,
    zstd: bool,
    widest: usize,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("usage: fs_replay <dir> <secs> <corpus>:<hz>:<codec>...");
        std::process::exit(2);
    }
    let dir = args[0].clone();
    let secs: u64 = args[1].parse().unwrap();

    let streams: Vec<Stream> = args[2..]
        .iter()
        .map(|spec| {
            let p: Vec<&str> = spec.split(':').collect();
            let corpus = load_corpus(p[0]);
            let widest = corpus.iter().map(|b| b.len()).max().unwrap_or(4096);
            Stream {
                name: Path::new(p[0])
                    .file_stem()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                corpus,
                hz: p[1].parse().unwrap(),
                zstd: p.get(2).map(|c| *c == "zstd").unwrap_or(true),
                widest,
            }
        })
        .collect();

    let root = format!("{dir}/fsreplay");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    // One page wide enough for the widest record of any stream: pages are per
    // queue but the setting is global.
    let widest = streams.iter().map(|s| s.widest).max().unwrap_or(4096);
    let mut page = 256 * 1024;
    while page < widest * 2 {
        page *= 2;
    }
    let mut settings = NormFsSettings::all_active();
    settings.mem_page_size = page;
    settings.max_memory_usage = 192 * 1024 * 1024;
    // Whole-instance codec: the per-queue axis is the next thing to sweep, and
    // mixing it in here would confound this run with that one.
    settings.wal_settings.compression_type = if streams.iter().all(|s| !s.zstd) {
        normfs_types::CompressionType::None
    } else {
        normfs_types::CompressionType::Zstd
    };

    let mode = apply_persist(&mut settings);
    ensure_bucket(&settings).await;
    println!(
        "replay {} stream(s) for {secs}s, mode {mode}, page {}K, codec {:?}\n",
        streams.len(),
        settings.mem_page_size / 1024,
        settings.wal_settings.compression_type
    );

    let fs = Arc::new(NormFS::new(root.clone(), settings).await.unwrap());
    let dev = std::env::var("PROFILE_DEV").unwrap_or_else(|_| "mmcblk1".into());
    let dev_before = device_written_bytes(&dev);
    let start = Instant::now();

    let mut handles = Vec::new();
    for s in streams {
        let fs = Arc::clone(&fs);
        let sent = Arc::new(AtomicU64::new(0));
        let bytes = Arc::new(AtomicU64::new(0));
        let (sent_c, bytes_c) = (Arc::clone(&sent), Arc::clone(&bytes));
        let name = s.name.clone();
        let hz = s.hz;
        let handle = tokio::spawn(async move {
            let queue = fs.resolve(&name);
            fs.ensure_queue_exists_for_write(&queue).await.unwrap();
            let period = Duration::from_secs_f64(1.0 / hz);
            let deadline = Instant::now() + Duration::from_secs(secs);
            let mut next = Instant::now();
            let mut i = 0usize;
            while Instant::now() < deadline {
                let rec = s.corpus[i % s.corpus.len()].clone();
                i += 1;
                let len = rec.len() as u64;
                if fs.enqueue(&queue, rec).await.is_err() {
                    break;
                }
                sent_c.fetch_add(1, Ordering::Relaxed);
                bytes_c.fetch_add(len, Ordering::Relaxed);
                next += period;
                let now = Instant::now();
                if next > now {
                    tokio::time::sleep(next - now).await;
                } else {
                    // Behind schedule: do not try to catch up in a burst, just
                    // keep going and let the shortfall show in the result.
                    next = now;
                }
            }
        });
        handles.push((s.name, s.hz, sent, bytes, handle));
    }

    let mut rows = Vec::new();
    for (name, hz, sent, bytes, h) in handles {
        let _ = h.await;
        rows.push((
            name,
            hz,
            sent.load(Ordering::Relaxed),
            bytes.load(Ordering::Relaxed),
        ));
    }
    let elapsed = start.elapsed().as_secs_f64();
    let _ = fs.close().await;
    let files = dir_files(Path::new(&root), "store") + dir_files(Path::new(&root), "wal");

    // Watch the run's own directory, not the device counter. The device
    // counter never settles -- anything else on the system writing to the
    // same card keeps it moving -- so waiting on it always ran to the cap and
    // turned a three-minute point into a twenty-three-minute one.
    let mut last = dir_bytes(Path::new(&root));
    let mut stable = 0;
    let cap = Instant::now() + Duration::from_secs(300);
    while stable < 5 && Instant::now() < cap {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let now = dir_bytes(Path::new(&root));
        if now == last {
            stable += 1;
        } else {
            stable = 0;
            last = now;
        }
    }
    let cycle = start.elapsed().as_secs_f64();
    let to_device = device_written_bytes(&dev).saturating_sub(dev_before);

    println!(
        "{:<20} | {:>8} | {:>9} | {:>7} | {:>9}",
        "stream", "target Hz", "achieved", "keep up", "MB/s in"
    );
    println!(
        "{:-<20}-+-{:->8}-+-{:->9}-+-{:->7}-+-{:->9}",
        "", "", "", "", ""
    );
    let mut total_in = 0.0;
    for (name, hz, sent, bytes) in rows {
        let got = sent as f64 / elapsed;
        total_in += bytes as f64 / elapsed / 1e6;
        println!(
            "{:<20} | {:>8.2} | {:>9.2} | {:>6.0}% | {:>9.3}",
            name,
            hz,
            got,
            got / hz * 100.0,
            bytes as f64 / elapsed / 1e6
        );
    }
    println!(
        "\ntotal in {:.2} MB/s, device {:.2} MB/s over the full cycle, amplification {:.2}x, \
         {:.1} MB on disk in {} files",
        total_in,
        to_device as f64 / cycle / 1e6,
        to_device as f64 / (total_in * elapsed * 1e6).max(1.0),
        dir_bytes(Path::new(&root)) as f64 / 1e6,
        files
    );
}
