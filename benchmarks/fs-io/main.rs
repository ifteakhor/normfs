use bytes::Bytes;
use normfs_fs::{AppendOutcome, Fs, FsConfig, PublishSpec, Runs, TmpMode};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;

const FIXTURE: usize = 64 << 20;
const MAX_WRITE_BYTES: usize = 64usize << 30;

struct Completed {
    samples: Vec<f64>,
    buffer: Vec<u8>,
    operations: usize,
    active_seconds: f64,
}

fn cpu() -> (f64, f64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: the pointer names writable storage of the size getrusage expects.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    // SAFETY: successful getrusage initialized the entire rusage value.
    let usage = unsafe { usage.assume_init() };
    let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    (seconds(usage.ru_utime), seconds(usage.ru_stime))
}

fn corpus(size: usize, pattern: &str) -> Arc<Vec<Bytes>> {
    assert!(matches!(pattern, "entropy" | "structured"));
    let mut state = 0x9e3779b97f4a7c15u64;
    Arc::new(
        (0..16)
            .map(|block| {
                let mut bytes = vec![0; size];
                for (index, byte) in bytes.iter_mut().enumerate() {
                    *byte = if pattern == "entropy" {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        (state >> 24) as u8
                    } else {
                        let text = b"{\"sensor\":\"voltage\",\"value\":12.34,\"unit\":\"V\"}\n";
                        text[(index + block) % text.len()]
                    };
                }
                Bytes::from(bytes)
            })
            .collect(),
    )
}

struct Work {
    dir: PathBuf,
    path: PathBuf,
    file: Arc<File>,
    ino: u64,
}

fn prepare(root: &Path, worker: usize, op: &str, data: &[Bytes]) -> Work {
    let dir = root.join(worker.to_string());
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("data");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    if op == "read" {
        for index in 0..FIXTURE / data[0].len() {
            file.write_all_at(&data[index % data.len()], (index * data[0].len()) as u64)
                .unwrap();
        }
    }
    file.sync_all().unwrap();
    File::open(&dir).unwrap().sync_all().unwrap();
    let ino = file.metadata().unwrap().ino();
    Work {
        dir: dir.clone(),
        path,
        file: Arc::new(file),
        ino,
    }
}

fn raw_publish(tmp: &Path, dst: &Path, parent: &Path, data: &[u8]) {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o644)
        .custom_flags(libc::O_NOFOLLOW)
        .open(tmp)
        .unwrap();
    assert_ne!(file.metadata().unwrap().ino(), 0);
    file.write_all_at(data, 0).unwrap();
    file.sync_all().unwrap();
    drop(file);
    // The production publisher checks its destination before rename as part
    // of replacement accounting; keep that metadata operation in the baseline.
    assert_eq!(
        std::fs::metadata(dst).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    std::fs::rename(tmp, dst).unwrap();
    File::open(parent).unwrap().sync_all().unwrap();
}

fn raw_worker(
    work: &Work,
    data: &[Bytes],
    op: &str,
    duration: Duration,
    limit: usize,
) -> Completed {
    let size = data[0].len();
    let mut buffer = vec![0; size];
    let mut samples = Vec::with_capacity(4096);
    let active = Instant::now();
    let mut index = 0;
    while active.elapsed() < duration {
        assert!(
            op == "read" || index < limit,
            "write footprint cap reached before deadline"
        );
        let start = Instant::now();
        match op {
            "append" => {
                work.file
                    .write_all_at(&data[index % data.len()], (index * size) as u64)
                    .unwrap();
                work.file.sync_all().unwrap();
            }
            "publish" => {
                let (tmp, dst) = (
                    work.dir.join(format!("{index}.tmp")),
                    work.dir.join(format!("{index}.bin")),
                );
                raw_publish(&tmp, &dst, &work.dir, &data[index % data.len()]);
            }
            "read" => {
                let offset = (index % (FIXTURE / size)) * size;
                // Match ReadFile's one-MiB syscall cap, so a four-MiB request
                // does not give the baseline four times fewer read syscalls.
                for (chunk, bytes) in buffer.chunks_mut(1 << 20).enumerate() {
                    work.file
                        .read_exact_at(bytes, (offset + chunk * (1 << 20)) as u64)
                        .unwrap();
                }
            }
            _ => unreachable!(),
        }
        if op != "read" || index % 64 == 0 {
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        index += 1;
    }
    Completed {
        samples,
        buffer,
        operations: index,
        active_seconds: active.elapsed().as_secs_f64(),
    }
}

async fn fs_worker(
    fs: &Fs,
    work: &Work,
    data: &[Bytes],
    op: &str,
    duration: Duration,
    limit: usize,
    mut reader: Option<normfs_fs::ReadFile>,
) -> Completed {
    let size = data[0].len();
    let mut buffer = vec![0; size];
    let mut samples = Vec::with_capacity(4096);
    let active = Instant::now();
    let mut index = 0;
    while active.elapsed() < duration {
        assert!(
            op == "read" || index < limit,
            "write footprint cap reached before deadline"
        );
        let start = Instant::now();
        match op {
            "append" => {
                let result = fs
                    .append_sync_with_inode(
                        work.file.clone(),
                        work.ino,
                        &work.path,
                        (index * size) as u64,
                        Runs(vec![data[index % data.len()].clone()]),
                        true,
                    )
                    .await
                    .unwrap();
                assert!(matches!(result, AppendOutcome::Committed));
            }
            "publish" => {
                let (tmp, dst) = (
                    work.dir.join(format!("{index}.tmp")),
                    work.dir.join(format!("{index}.bin")),
                );
                fs.publish(
                    PublishSpec {
                        tmp: tmp.clone(),
                        dst: dst.clone(),
                        runs: Runs(vec![data[index % data.len()].clone()]),
                        tmp_mode: TmpMode::Excl,
                        sync: true,
                    },
                    None,
                )
                .await
                .unwrap();
            }
            "read" => {
                let reader = reader.as_mut().unwrap();
                reader
                    .seek(std::io::SeekFrom::Start(
                        ((index % (FIXTURE / size)) * size) as u64,
                    ))
                    .await
                    .unwrap();
                reader.read_exact(&mut buffer).await.unwrap();
            }
            _ => unreachable!(),
        }
        if op != "read" || index % 64 == 0 {
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        index += 1;
    }
    Completed {
        samples,
        buffer,
        operations: index,
        active_seconds: active.elapsed().as_secs_f64(),
    }
}

fn verify(work: &Work, data: &[Bytes], op: &str, count: usize, buffer: &[u8]) {
    let size = data[0].len();
    if op == "read" {
        assert_eq!(
            buffer,
            data[((count - 1) % (FIXTURE / size)) % data.len()].as_ref()
        );
        return;
    }
    let mut buffer = vec![0; size];
    for index in 0..count {
        if op == "append" {
            work.file
                .read_exact_at(&mut buffer, (index * size) as u64)
                .unwrap();
        } else {
            let (tmp, dst) = (
                work.dir.join(format!("{index}.tmp")),
                work.dir.join(format!("{index}.bin")),
            );
            assert!(!tmp.exists());
            let file = File::open(dst).unwrap();
            assert_eq!(file.metadata().unwrap().len(), size as u64);
            file.read_exact_at(&mut buffer, 0).unwrap();
        }
        assert_eq!(buffer.as_slice(), data[index % data.len()].as_ref());
    }
    if op == "append" {
        assert_eq!(work.file.metadata().unwrap().len(), (size * count) as u64);
    }
}

#[tokio::main(worker_threads = 8)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(
        args.len() - usize::from(args.last().is_some_and(|arg| arg == "--smoke")),
        7,
        "fs_io raw|fs append|publish|read BYTES WORKERS MIN_SECONDS PATTERN [--smoke]"
    );
    let backend = &args[1];
    let op = args[2].clone();
    let size: usize = args[3].parse().unwrap();
    let workers: usize = args[4].parse().unwrap();
    let minimum_seconds: f64 = args[5].parse().unwrap();
    let smoke = args.last().is_some_and(|arg| arg == "--smoke");
    assert!(minimum_seconds.is_finite() && minimum_seconds > 0.0 && minimum_seconds <= 3600.0);
    assert!(
        smoke || minimum_seconds >= 30.0,
        "measurements must run for at least 30 seconds"
    );
    let duration = Duration::from_secs_f64(minimum_seconds);
    let limit = MAX_WRITE_BYTES / size / workers;
    assert!(matches!(backend.as_str(), "raw" | "fs"));
    assert!(matches!(op.as_str(), "append" | "publish" | "read"));
    assert!([32 << 10, 1 << 20, 4 << 20].contains(&size));
    assert!([1, 8].contains(&workers));
    let data = corpus(size, &args[6]);
    let tmp = tempfile::tempdir().unwrap();
    let works: Vec<_> = (0..workers)
        .map(|w| Arc::new(prepare(tmp.path(), w, &op, &data)))
        .collect();
    File::open(tmp.path()).unwrap().sync_all().unwrap();
    File::open(tmp.path().parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
    let (elapsed, user, system, completed) = if backend == "raw" {
        let ready = Arc::new(std::sync::Barrier::new(workers + 1));
        let go = Arc::new(std::sync::Barrier::new(workers + 1));
        let handles: Vec<_> = works
            .iter()
            .map(|work| {
                let (work, data, op, ready, go) = (
                    work.clone(),
                    data.clone(),
                    op.clone(),
                    ready.clone(),
                    go.clone(),
                );
                std::thread::spawn(move || {
                    ready.wait();
                    go.wait();
                    raw_worker(&work, &data, &op, duration, limit)
                })
            })
            .collect();
        ready.wait();
        let before = cpu();
        let start = Instant::now();
        go.wait();
        let done: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let elapsed = start.elapsed().as_secs_f64();
        let after = cpu();
        (elapsed, after.0 - before.0, after.1 - before.1, done)
    } else {
        let fs = Fs::new(FsConfig::default()).unwrap();
        let ready = Arc::new(tokio::sync::Barrier::new(workers + 1));
        let go = Arc::new(tokio::sync::Barrier::new(workers + 1));
        let mut handles = Vec::new();
        for work in &works {
            let (work, data, op, ready, go, fs) = (
                work.clone(),
                data.clone(),
                op.clone(),
                ready.clone(),
                go.clone(),
                fs.clone(),
            );
            handles.push(tokio::spawn(async move {
                let reader = if op == "read" {
                    Some(fs.open_read(&work.path).await.unwrap())
                } else {
                    None
                };
                ready.wait().await;
                go.wait().await;
                fs_worker(&fs, &work, &data, &op, duration, limit, reader).await
            }));
        }
        ready.wait().await;
        let before = cpu();
        let start = Instant::now();
        go.wait().await;
        let mut done = Vec::new();
        for handle in handles {
            done.push(handle.await.unwrap());
        }
        let elapsed = start.elapsed().as_secs_f64();
        let after = cpu();
        (elapsed, after.0 - before.0, after.1 - before.1, done)
    };
    let mut samples = Vec::new();
    let mut worker_operations = Vec::new();
    let mut worker_active_seconds = Vec::new();
    for (work, done) in works.iter().zip(completed) {
        assert!(done.active_seconds >= minimum_seconds);
        assert!(done.operations > 0);
        verify(work, &data, &op, done.operations, &done.buffer);
        let expected_samples = if op == "read" {
            done.operations.div_ceil(64)
        } else {
            done.operations
        };
        assert_eq!(done.samples.len(), expected_samples);
        worker_operations.push(done.operations);
        worker_active_seconds.push(done.active_seconds);
        samples.extend(done.samples);
    }
    let operations: usize = worker_operations.iter().sum();
    samples.sort_by(f64::total_cmp);
    let percentile =
        |p: f64| samples[((samples.len() as f64 * p).ceil() as usize).saturating_sub(1)];
    let mib = (size * operations) as f64 / 1048576.0;
    println!(
        "RESULT {}",
        serde_json::json!({
            "backend":backend, "operation":op, "block_bytes":size, "workers":workers,
            "operations":operations, "minimum_seconds":minimum_seconds, "smoke":smoke,
            "worker_operations":worker_operations, "worker_active_seconds":worker_active_seconds,
            "latency_sample_stride":if op == "read" {64} else {1}, "pattern":args[6], "payload_mib":mib,
            "seconds":elapsed, "mib_s":mib/elapsed, "user_seconds":user,
            "system_seconds":system, "cpu_percent":100.0*(user+system)/elapsed,
            "cpu_seconds_per_gib":(user+system)/(mib/1024.0),
            "p50_ms":percentile(0.5), "p99_ms":percentile(0.99), "max_ms":samples.last(),
            "samples_ms":samples, "verified":true
        })
    );
}
