//! Does moving fs I/O off tokio's blocking pool pay on the rover's workload?
//!
//!   fs_gate <dir> <tokio|pool|uring> <store|wal> <queues> <page> <secs> [--threads N] [--dev mmcblk1]
//!
//! Three backends drive the same two protocols, so a difference between rows
//! is the backend and nothing else:
//!
//!   tokio   `tokio::fs`, one blocking-pool round trip per syscall (today)
//!   pool    a bounded std-thread pool; the whole protocol runs on one thread
//!   uring   one `io_uring`, one SQE in flight per job, a driver thread
//!
//! `store` is direct-store mode's cost per sealed page: create a temp file,
//! write the page in three runs, fsync, rename into the 3-hex-chunk layout,
//! fsync the directory. `wal` is the WAL writer: every 50 ms per queue, write
//! the batch that accumulated and fsync, rotating at 32 MiB.
//!
//! The shape is fixed by the positional arguments and by nothing else; a
//! row's numbers must be reproducible from the row alone. `--dev` names the
//! block device whose write counter is read, and is the one thing that varies
//! by machine rather than by case.
//!
//! One CSV row per run on stdout. The columns that decide anything: `dev_amp`
//! (bytes the card saw per payload byte), `cpu` (share of one core), `p99_us`
//! (per-op latency) and `stall_us` (the worst lag of a 1 ms ticker on a
//! four-worker runtime running beside the load -- what the request path
//! would feel).

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::oneshot;

const WAL_INTERVAL: Duration = Duration::from_millis(50);
/// 200 Hz of ~80 B records, framed: what one queue's flush carries.
const WAL_BATCH: usize = 16 * 1024;
const WAL_ROTATE: u64 = 32 << 20;
const RUN_HEAD: usize = 64;
const RUN_MID: usize = 128;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Backend {
    Tokio,
    Pool,
    Uring,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Profile {
    Store,
    Wal,
}

struct Args {
    dir: PathBuf,
    backend: Backend,
    profile: Profile,
    queues: usize,
    page: usize,
    secs: u64,
    threads: usize,
    dev: String,
}

fn usage() -> ! {
    eprintln!(
        "usage: fs_gate <dir> <tokio|pool|uring> <store|wal> <queues> <page> <secs> [--threads N] [--dev mmcblk1]"
    );
    std::process::exit(2)
}

fn parse_size(s: &str) -> Option<usize> {
    let s = s.to_ascii_lowercase();
    let (num, mul) = if let Some(n) = s.strip_suffix('k') {
        (n, 1024)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 1024 * 1024)
    } else {
        (s.as_str(), 1)
    };
    num.parse::<usize>().ok().map(|n| n * mul)
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().collect();
    if argv.len() < 7 {
        usage();
    }
    let backend = match argv[2].as_str() {
        "tokio" => Backend::Tokio,
        "pool" => Backend::Pool,
        "uring" => Backend::Uring,
        _ => usage(),
    };
    let profile = match argv[3].as_str() {
        "store" => Profile::Store,
        "wal" => Profile::Wal,
        _ => usage(),
    };
    let mut args = Args {
        dir: PathBuf::from(&argv[1]),
        backend,
        profile,
        queues: argv[4].parse().unwrap_or_else(|_| usage()),
        page: parse_size(&argv[5]).unwrap_or_else(|| usage()),
        secs: argv[6].parse().unwrap_or_else(|_| usage()),
        threads: 4,
        dev: "mmcblk1".to_string(),
    };
    let mut i = 7;
    while i + 1 < argv.len() {
        match argv[i].as_str() {
            "--threads" => args.threads = argv[i + 1].parse().unwrap_or_else(|_| usage()),
            "--dev" => args.dev = argv[i + 1].clone(),
            _ => usage(),
        }
        i += 2;
    }
    args
}

// ---------------------------------------------------------------------------
// Measurement

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

fn cpu_seconds() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: RUSAGE_SELF with a zeroed, correctly sized struct.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let t = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 / 1e6;
    t(ru.ru_utime) + t(ru.ru_stime)
}

fn max_rss_mb() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: as above.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    if cfg!(target_os = "macos") {
        ru.ru_maxrss as f64 / (1024.0 * 1024.0)
    } else {
        ru.ru_maxrss as f64 / 1024.0
    }
}

fn kernel_release() -> String {
    let mut u: libc::utsname = unsafe { std::mem::zeroed() };
    // SAFETY: utsname is plain data; uname fills it.
    if unsafe { libc::uname(&mut u) } != 0 {
        return "?".into();
    }
    let bytes: Vec<u8> = u
        .release
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[derive(Default)]
struct Stats {
    ops: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    lat_us: Mutex<Vec<u32>>,
}

impl Stats {
    fn record(&self, started: Instant, bytes: usize) {
        let us = started.elapsed().as_micros().min(u32::MAX as u128) as u32;
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.lat_us.lock().unwrap().push(us);
    }
}

fn percentile(sorted: &[u32], p: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

// ---------------------------------------------------------------------------
// The two protocols, as data

struct Publish {
    tmp: PathBuf,
    dst: PathBuf,
    dir: PathBuf,
    runs: Vec<Bytes>,
}

struct Append {
    fd: RawFd,
    at: u64,
    runs: Vec<Bytes>,
}

enum Job {
    Publish(Publish, oneshot::Sender<std::io::Result<()>>),
    Append(Append, oneshot::Sender<std::io::Result<()>>),
}

fn cstr(p: &Path) -> CString {
    CString::new(p.as_os_str().as_bytes()).expect("path without NUL")
}

fn iovecs(runs: &[Bytes]) -> Vec<libc::iovec> {
    runs.iter()
        .map(|b| libc::iovec {
            iov_base: b.as_ptr() as *mut libc::c_void,
            iov_len: b.len(),
        })
        .collect()
}

fn os_err() -> std::io::Error {
    std::io::Error::last_os_error()
}

// ---------------------------------------------------------------------------
// Backend: raw syscalls (pool)

fn sys_pwritev_all(fd: RawFd, runs: &[Bytes], mut off: u64) -> std::io::Result<()> {
    let mut iov = iovecs(runs);
    let mut first = 0usize;
    while first < iov.len() {
        // SAFETY: iov points into `runs`, which outlives this call.
        let n = unsafe {
            libc::pwritev(
                fd,
                iov[first..].as_ptr(),
                (iov.len() - first) as libc::c_int,
                off as libc::off_t,
            )
        };
        if n < 0 {
            let e = os_err();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if n == 0 {
            return Err(std::io::Error::other("write made no progress"));
        }
        let mut n = n as usize;
        off += n as u64;
        while n > 0 {
            if n >= iov[first].iov_len {
                n -= iov[first].iov_len;
                first += 1;
            } else {
                // SAFETY: advancing within the same buffer.
                iov[first].iov_base = unsafe { (iov[first].iov_base as *mut u8).add(n) as *mut _ };
                iov[first].iov_len -= n;
                n = 0;
            }
        }
    }
    Ok(())
}

fn sys_fsync(fd: RawFd) -> std::io::Result<()> {
    // What Rust's sync_all does on macOS; plain fsync stops at the drive
    // cache there and would flatter this backend against tokio's.
    #[cfg(target_os = "macos")]
    // SAFETY: fd is open.
    if unsafe { libc::fcntl(fd, libc::F_FULLFSYNC) } == 0 {
        return Ok(());
    }
    loop {
        // SAFETY: fd is open.
        if unsafe { libc::fsync(fd) } == 0 {
            return Ok(());
        }
        let e = os_err();
        if e.kind() != std::io::ErrorKind::Interrupted {
            return Err(e);
        }
    }
}

struct Fd(RawFd);
impl Drop for Fd {
    fn drop(&mut self) {
        // SAFETY: we own the descriptor.
        unsafe { libc::close(self.0) };
    }
}

fn sys_open(path: &Path, flags: libc::c_int, mode: libc::c_int) -> std::io::Result<Fd> {
    let c = cstr(path);
    loop {
        // SAFETY: c is a valid NUL-terminated path.
        let fd = unsafe { libc::open(c.as_ptr(), flags, mode) };
        if fd >= 0 {
            return Ok(Fd(fd));
        }
        let e = os_err();
        if e.kind() != std::io::ErrorKind::Interrupted {
            return Err(e);
        }
    }
}

fn sys_publish(p: &Publish) -> std::io::Result<()> {
    let fd = sys_open(
        &p.tmp,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC,
        0o644,
    )?;
    sys_pwritev_all(fd.0, &p.runs, 0)?;
    sys_fsync(fd.0)?;
    drop(fd);
    let (src, dst) = (cstr(&p.tmp), cstr(&p.dst));
    // SAFETY: both are valid NUL-terminated paths.
    if unsafe { libc::rename(src.as_ptr(), dst.as_ptr()) } != 0 {
        return Err(os_err());
    }
    let d = sys_open(
        &p.dir,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        0,
    )?;
    sys_fsync(d.0)
}

fn sys_append(a: &Append) -> std::io::Result<()> {
    sys_pwritev_all(a.fd, &a.runs, a.at)?;
    sys_fsync(a.fd)
}

struct Pool {
    tx: Mutex<std::sync::mpsc::Sender<Job>>,
}

impl Pool {
    fn start(threads: usize) -> Arc<Pool> {
        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..threads {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("fs-gate-{i}"))
                .spawn(move || {
                    loop {
                        let job = rx.lock().unwrap().recv();
                        match job {
                            Ok(Job::Publish(p, reply)) => {
                                let _ = reply.send(sys_publish(&p));
                            }
                            Ok(Job::Append(a, reply)) => {
                                let _ = reply.send(sys_append(&a));
                            }
                            Err(_) => return,
                        }
                    }
                })
                .expect("spawn");
        }
        Arc::new(Pool { tx: Mutex::new(tx) })
    }

    fn submit(&self, job: Job) {
        self.tx.lock().unwrap().send(job).expect("pool alive");
    }
}

// ---------------------------------------------------------------------------
// Backend: tokio::fs (status quo)

async fn tokio_publish(p: &Publish) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut f = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&p.tmp)
        .await?;
    for r in &p.runs {
        f.write_all(r).await?;
    }
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(&p.tmp, &p.dst).await?;
    tokio::fs::File::open(&p.dir).await?.sync_all().await
}

async fn tokio_append(file: &mut tokio::fs::File, runs: &[Bytes]) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    for r in runs {
        file.write_all(r).await?;
    }
    file.flush().await?;
    file.sync_all().await
}

// ---------------------------------------------------------------------------
// Backend: io_uring, one op in flight per job

#[cfg(all(target_os = "linux", feature = "uring"))]
mod uring {
    use super::*;
    use io_uring::{IoUring, Probe, opcode, types};
    use std::collections::HashMap;

    pub const NEEDED: &[(u8, &str)] = &[
        (opcode::OpenAt::CODE, "OpenAt"),
        (opcode::Writev::CODE, "Writev"),
        (opcode::Fsync::CODE, "Fsync"),
        (opcode::Close::CODE, "Close"),
        (opcode::RenameAt::CODE, "RenameAt"),
    ];

    pub fn probe() -> Result<Vec<&'static str>, std::io::Error> {
        let ring = IoUring::new(8)?;
        let mut probe = Probe::new();
        ring.submitter().register_probe(&mut probe)?;
        Ok(NEEDED
            .iter()
            .filter(|(code, _)| !probe.is_supported(*code))
            .map(|(_, name)| *name)
            .collect())
    }

    enum Step {
        OpenTmp,
        Write,
        FsyncFile,
        CloseFile,
        Rename,
        OpenDir,
        FsyncDir,
        CloseDir,
    }

    struct InFlight {
        job: Job,
        step: Step,
        fd: RawFd,
        written: u64,
        iov: Vec<libc::iovec>,
        tmp: CString,
        dst: CString,
        dir: CString,
    }

    impl InFlight {
        fn new(job: Job) -> Self {
            let (iov, tmp, dst, dir, step, fd) = match &job {
                Job::Publish(p, _) => (
                    iovecs(&p.runs),
                    cstr(&p.tmp),
                    cstr(&p.dst),
                    cstr(&p.dir),
                    Step::OpenTmp,
                    -1,
                ),
                Job::Append(a, _) => (
                    iovecs(&a.runs),
                    CString::default(),
                    CString::default(),
                    CString::default(),
                    Step::Write,
                    a.fd,
                ),
            };
            InFlight {
                job,
                step,
                fd,
                written: 0,
                iov,
                tmp,
                dst,
                dir,
            }
        }

        fn total(&self) -> u64 {
            match &self.job {
                Job::Publish(p, _) => p.runs.iter().map(|b| b.len() as u64).sum(),
                Job::Append(a, _) => a.runs.iter().map(|b| b.len() as u64).sum(),
            }
        }

        fn base(&self) -> u64 {
            match &self.job {
                Job::Publish(..) => 0,
                Job::Append(a, _) => a.at,
            }
        }

        /// The SQE for the current step. iov and the CStrings live as long as
        /// `self`, which outlives the CQE.
        fn sqe(&mut self, id: u64) -> io_uring::squeue::Entry {
            let e = match self.step {
                Step::OpenTmp => opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), self.tmp.as_ptr())
                    .flags(libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC)
                    .mode(0o644)
                    .build(),
                Step::Write => opcode::Writev::new(
                    types::Fd(self.fd),
                    self.iov.as_ptr(),
                    self.iov.len() as u32,
                )
                .offset(self.base() + self.written)
                .build(),
                Step::FsyncFile | Step::FsyncDir => opcode::Fsync::new(types::Fd(self.fd)).build(),
                Step::CloseFile | Step::CloseDir => opcode::Close::new(types::Fd(self.fd)).build(),
                Step::Rename => opcode::RenameAt::new(
                    types::Fd(libc::AT_FDCWD),
                    self.tmp.as_ptr(),
                    types::Fd(libc::AT_FDCWD),
                    self.dst.as_ptr(),
                )
                .build(),
                Step::OpenDir => opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), self.dir.as_ptr())
                    .flags(libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC)
                    .build(),
            };
            e.user_data(id)
        }

        /// Apply a completion; `Some(result)` when the job is finished.
        fn complete(&mut self, res: i32) -> Option<std::io::Result<()>> {
            if res < 0 {
                let e = std::io::Error::from_raw_os_error(-res);
                if e.kind() == std::io::ErrorKind::Interrupted {
                    return None;
                }
                return Some(Err(e));
            }
            match self.step {
                Step::OpenTmp => {
                    self.fd = res;
                    self.step = Step::Write;
                }
                Step::Write => {
                    if res == 0 {
                        return Some(Err(std::io::Error::other("write made no progress")));
                    }
                    self.written += res as u64;
                    if self.written < self.total() {
                        let mut n = res as usize;
                        while n > 0 {
                            if n >= self.iov[0].iov_len {
                                n -= self.iov[0].iov_len;
                                self.iov.remove(0);
                            } else {
                                // SAFETY: within the same buffer.
                                self.iov[0].iov_base =
                                    unsafe { (self.iov[0].iov_base as *mut u8).add(n) as *mut _ };
                                self.iov[0].iov_len -= n;
                                n = 0;
                            }
                        }
                    } else {
                        self.step = Step::FsyncFile;
                    }
                }
                Step::FsyncFile => match &self.job {
                    Job::Publish(..) => self.step = Step::CloseFile,
                    Job::Append(..) => return Some(Ok(())),
                },
                Step::CloseFile => self.step = Step::Rename,
                Step::Rename => self.step = Step::OpenDir,
                Step::OpenDir => {
                    self.fd = res;
                    self.step = Step::FsyncDir;
                }
                Step::FsyncDir => self.step = Step::CloseDir,
                Step::CloseDir => return Some(Ok(())),
            }
            None
        }

        fn finish(self, result: std::io::Result<()>) {
            match self.job {
                Job::Publish(_, reply) | Job::Append(_, reply) => {
                    let _ = reply.send(result);
                }
            }
        }
    }

    pub struct Driver {
        tx: Mutex<std::sync::mpsc::Sender<Job>>,
    }

    impl Driver {
        pub fn start(entries: u32) -> std::io::Result<Arc<Driver>> {
            let mut ring = IoUring::new(entries)?;
            let (tx, rx) = std::sync::mpsc::channel::<Job>();
            std::thread::Builder::new()
                .name("fs-gate-uring".into())
                .spawn(move || {
                    let mut inflight: HashMap<u64, InFlight> = HashMap::new();
                    let mut next_id = 1u64;
                    loop {
                        // Take new jobs: block only when nothing is in flight.
                        if inflight.is_empty() {
                            match rx.recv() {
                                Ok(job) => {
                                    let mut f = InFlight::new(job);
                                    let e = f.sqe(next_id);
                                    // SAFETY: buffers referenced by e live in `f`, kept in inflight.
                                    unsafe { ring.submission().push(&e).expect("sq full") };
                                    inflight.insert(next_id, f);
                                    next_id += 1;
                                }
                                Err(_) => return,
                            }
                        }
                        while let Ok(job) = rx.try_recv() {
                            let mut f = InFlight::new(job);
                            let e = f.sqe(next_id);
                            // SAFETY: as above.
                            unsafe { ring.submission().push(&e).expect("sq full") };
                            inflight.insert(next_id, f);
                            next_id += 1;
                        }
                        if let Err(e) = ring.submit_and_wait(1) {
                            if e.kind() == std::io::ErrorKind::Interrupted {
                                continue;
                            }
                            panic!("submit_and_wait: {e}");
                        }
                        let cqes: Vec<(u64, i32)> = ring
                            .completion()
                            .map(|c| (c.user_data(), c.result()))
                            .collect();
                        for (id, res) in cqes {
                            let Some(mut f) = inflight.remove(&id) else {
                                continue;
                            };
                            match f.complete(res) {
                                Some(result) => f.finish(result),
                                None => {
                                    let e = f.sqe(id);
                                    // SAFETY: as above.
                                    unsafe { ring.submission().push(&e).expect("sq full") };
                                    inflight.insert(id, f);
                                }
                            }
                        }
                    }
                })
                .expect("spawn");
            Ok(Arc::new(Driver { tx: Mutex::new(tx) }))
        }

        pub fn submit(&self, job: Job) {
            self.tx.lock().unwrap().send(job).expect("driver alive");
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch

enum Exec {
    Tokio,
    Pool(Arc<Pool>),
    #[cfg(all(target_os = "linux", feature = "uring"))]
    Uring(Arc<uring::Driver>),
}

impl Exec {
    async fn publish(&self, p: Publish) -> std::io::Result<()> {
        match self {
            Exec::Tokio => tokio_publish(&p).await,
            Exec::Pool(pool) => {
                let (tx, rx) = oneshot::channel();
                pool.submit(Job::Publish(p, tx));
                rx.await.expect("pool replies")
            }
            #[cfg(all(target_os = "linux", feature = "uring"))]
            Exec::Uring(d) => {
                let (tx, rx) = oneshot::channel();
                d.submit(Job::Publish(p, tx));
                rx.await.expect("driver replies")
            }
        }
    }

    async fn append(&self, a: Append) -> std::io::Result<()> {
        match self {
            Exec::Tokio => unreachable!("tokio wal path keeps its own File"),
            Exec::Pool(pool) => {
                let (tx, rx) = oneshot::channel();
                pool.submit(Job::Append(a, tx));
                rx.await.expect("pool replies")
            }
            #[cfg(all(target_os = "linux", feature = "uring"))]
            Exec::Uring(d) => {
                let (tx, rx) = oneshot::channel();
                d.submit(Job::Append(a, tx));
                rx.await.expect("driver replies")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Queues

fn store_path(qdir: &Path, id: u64) -> (PathBuf, PathBuf) {
    let hex = format!("{id:06x}");
    let dir = qdir.join("store").join(&hex[..3]);
    (dir.clone(), dir.join(format!("{}.store", &hex[3..])))
}

fn page_runs(page: usize, seed: u64) -> Vec<Bytes> {
    // Not random and not constant: the card sees the same bytes whatever the
    // backend, and a card that compresses is not what we are measuring anyway.
    let mut body = vec![0u8; page];
    let mut x = seed.wrapping_mul(0x9E3779B97F4A7C15) | 1;
    for chunk in body.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let b = x.to_le_bytes();
        chunk.copy_from_slice(&b[..chunk.len()]);
    }
    let all = Bytes::from(body);
    vec![
        all.slice(0..RUN_HEAD.min(page)),
        all.slice(RUN_HEAD.min(page)..(RUN_HEAD + RUN_MID).min(page)),
        all.slice((RUN_HEAD + RUN_MID).min(page)..),
    ]
}

async fn run_store_queue(
    exec: Arc<Exec>,
    qdir: PathBuf,
    tmpdir: PathBuf,
    q: usize,
    page: usize,
    stop: Arc<AtomicBool>,
    stats: Arc<Stats>,
) {
    let mut id = 0u64;
    let mut made_dir = String::new();
    while !stop.load(Ordering::Relaxed) {
        let (dir, dst) = store_path(&qdir, id);
        let chunk = dir.to_string_lossy().into_owned();
        if chunk != made_dir {
            std::fs::create_dir_all(&dir).expect("chunk dir");
            made_dir = chunk;
        }
        let p = Publish {
            tmp: tmpdir.join(format!("q{q}-{id}.tmp")),
            dst,
            dir,
            runs: page_runs(page, (q as u64) << 32 | id),
        };
        let started = Instant::now();
        match exec.publish(p).await {
            Ok(()) => stats.record(started, page),
            Err(e) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("q{q} publish {id}: {e}");
            }
        }
        id += 1;
    }
}

async fn run_wal_queue(
    exec: Arc<Exec>,
    qdir: PathBuf,
    q: usize,
    stop: Arc<AtomicBool>,
    stats: Arc<Stats>,
) {
    let wal = qdir.join("wal");
    std::fs::create_dir_all(&wal).expect("wal dir");
    let mut file_id = 0u64;
    let mut off = 0u64;
    let open = |id: u64| -> (std::fs::File, PathBuf) {
        let path = wal.join(format!("{id:06x}.wal"));
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("wal file");
        (f, path)
    };
    let (mut std_file, mut _path) = open(file_id);
    let mut tokio_file = match &*exec {
        Exec::Tokio => Some(tokio::fs::File::from_std(std_file.try_clone().unwrap())),
        _ => None,
    };
    let mut interval = tokio::time::interval(WAL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut batch_no = 0u64;
    while !stop.load(Ordering::Relaxed) {
        interval.tick().await;
        let runs = page_runs(WAL_BATCH, (q as u64) << 40 | batch_no);
        batch_no += 1;
        let started = Instant::now();
        let r = match tokio_file.as_mut() {
            Some(tf) => tokio_append(tf, &runs).await,
            None => {
                exec.append(Append {
                    fd: std_file.as_raw_fd(),
                    at: off,
                    runs,
                })
                .await
            }
        };
        match r {
            Ok(()) => {
                off += WAL_BATCH as u64;
                stats.record(started, WAL_BATCH);
            }
            Err(e) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("q{q} append: {e}");
            }
        }
        if off >= WAL_ROTATE {
            file_id += 1;
            off = 0;
            let (f, p) = open(file_id);
            std_file = f;
            _path = p;
            if tokio_file.is_some() {
                tokio_file = Some(tokio::fs::File::from_std(std_file.try_clone().unwrap()));
            }
        }
    }
}

/// Worst lag of a 1 ms ticker: what a request handler would wait behind.
async fn stall_probe(stop: Arc<AtomicBool>, worst: Arc<AtomicU64>) {
    let mut interval = tokio::time::interval(Duration::from_millis(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        interval.tick().await;
        let now = Instant::now();
        let lag = now.duration_since(last).as_micros().saturating_sub(1000) as u64;
        worst.fetch_max(lag, Ordering::Relaxed);
        last = now;
    }
}

fn main() {
    let args = parse_args();
    let kernel = kernel_release();

    #[cfg(all(target_os = "linux", feature = "uring"))]
    let uring_state = match uring::probe() {
        Ok(missing) if missing.is_empty() => "ok".to_string(),
        Ok(missing) => format!("missing:{}", missing.join("+")),
        Err(e) => format!("setup:{}", e.raw_os_error().unwrap_or(-1)),
    };
    #[cfg(not(all(target_os = "linux", feature = "uring")))]
    let uring_state = "n/a".to_string();

    let exec = match args.backend {
        Backend::Tokio => Exec::Tokio,
        Backend::Pool => Exec::Pool(Pool::start(args.threads)),
        Backend::Uring => {
            #[cfg(all(target_os = "linux", feature = "uring"))]
            {
                if uring_state != "ok" {
                    eprintln!("io_uring unusable here: {uring_state}");
                    std::process::exit(3);
                }
                Exec::Uring(uring::Driver::start(256).expect("ring"))
            }
            #[cfg(not(all(target_os = "linux", feature = "uring")))]
            {
                eprintln!("built without the uring feature or not on Linux");
                std::process::exit(3);
            }
        }
    };
    let exec = Arc::new(exec);

    let root = args.dir.join(format!(
        "fs_gate-{:?}-{:?}-{}q-{}",
        args.backend, args.profile, args.queues, args.page
    ));
    let _ = std::fs::remove_dir_all(&root);
    let tmpdir = root.join("tmp");
    std::fs::create_dir_all(&tmpdir).expect("root");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");

    let stop = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(Stats::default());
    let worst = Arc::new(AtomicU64::new(0));

    let dev_before = device_written_bytes(&args.dev);
    let cpu_before = cpu_seconds();
    let wall = Instant::now();

    rt.block_on(async {
        let mut tasks = Vec::new();
        tasks.push(tokio::spawn(stall_probe(stop.clone(), worst.clone())));
        for q in 0..args.queues {
            let qdir = root.join(format!("q{q}"));
            std::fs::create_dir_all(&qdir).expect("qdir");
            let t = match args.profile {
                Profile::Store => tokio::spawn(run_store_queue(
                    exec.clone(),
                    qdir,
                    tmpdir.clone(),
                    q,
                    args.page,
                    stop.clone(),
                    stats.clone(),
                )),
                Profile::Wal => tokio::spawn(run_wal_queue(
                    exec.clone(),
                    qdir,
                    q,
                    stop.clone(),
                    stats.clone(),
                )),
            };
            tasks.push(t);
        }
        tokio::time::sleep(Duration::from_secs(args.secs)).await;
        stop.store(true, Ordering::Relaxed);
        for t in tasks {
            let _ = t.await;
        }
    });

    let elapsed = wall.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu_before;
    // Let the card's own counter catch up with what it owes.
    std::thread::sleep(Duration::from_secs(2));
    let dev = device_written_bytes(&args.dev).saturating_sub(dev_before);

    let ops = stats.ops.load(Ordering::Relaxed);
    let bytes = stats.bytes.load(Ordering::Relaxed);
    let errors = stats.errors.load(Ordering::Relaxed);
    let mut lat = stats.lat_us.lock().unwrap().clone();
    lat.sort_unstable();
    let amp = if bytes > 0 {
        dev as f64 / bytes as f64
    } else {
        0.0
    };

    println!(
        "kernel,uring,backend,profile,queues,page,threads,secs,ops,ops_s,mb_s,dev_mb,dev_amp,cpu,p50_us,p99_us,stall_us,rss_mb,errors"
    );
    println!(
        "{kernel},{uring_state},{:?},{:?},{},{},{},{:.1},{ops},{:.1},{:.2},{:.1},{:.3},{:.3},{},{},{},{:.0},{errors}",
        args.backend,
        args.profile,
        args.queues,
        args.page,
        args.threads,
        elapsed,
        ops as f64 / elapsed,
        bytes as f64 / elapsed / (1024.0 * 1024.0),
        dev as f64 / (1024.0 * 1024.0),
        amp,
        cpu / elapsed,
        percentile(&lat, 0.50),
        percentile(&lat, 0.99),
        worst.load(Ordering::Relaxed),
        max_rss_mb(),
    );

    let _ = std::fs::remove_dir_all(&root);
}
