use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Metric {
    Admission,
    Queue,
    Open,
    Write,
    FileSync,
    Close,
    Stat,
    Rename,
    DirSync,
    OtherPlan,
    Mkdir,
    ReadWal,
    Process,
    DevWrite,
    DevFileSync,
    DevRename,
    DevDirQueue,
    DevDirSync,
}

const NAMES: [&str; 18] = [
    "admission",
    "queue",
    "open",
    "write",
    "file_sync",
    "close",
    "stat",
    "rename",
    "dir_sync",
    "other_plan",
    "mkdir",
    "read_wal",
    "process",
    "dev_write_await",
    "dev_file_sync_await",
    "dev_rename_await",
    "dev_dir_queue",
    "dev_dir_sync",
];
static ENABLED: AtomicBool = AtomicBool::new(false);
static COUNTS: [AtomicU64; 18] = [const { AtomicU64::new(0) }; 18];
static NANOS: [AtomicU64; 18] = [const { AtomicU64::new(0) }; 18];

pub fn start(enabled: bool) {
    for value in COUNTS.iter().chain(NANOS.iter()) {
        value.store(0, Ordering::Relaxed);
    }
    ENABLED.store(enabled, Ordering::Relaxed);
}

pub fn now() -> Option<Instant> {
    ENABLED.load(Ordering::Relaxed).then(Instant::now)
}

pub fn record(metric: Metric, start: Option<Instant>) {
    if let Some(start) = start {
        let index = metric as usize;
        NANOS[index].fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        COUNTS[index].fetch_add(1, Ordering::Relaxed);
    }
}

pub struct Timer(Metric, Option<Instant>);

impl Timer {
    pub fn new(metric: Metric) -> Self {
        Self(metric, now())
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        record(self.0, self.1);
    }
}

pub fn stop_json() -> String {
    ENABLED.store(false, Ordering::Relaxed);
    let rows: Vec<_> = NAMES
        .iter()
        .enumerate()
        .map(|(index, name)| {
            format!(
                "\"{name}\":{{\"calls\":{},\"seconds\":{}}}",
                COUNTS[index].load(Ordering::Relaxed),
                NANOS[index].load(Ordering::Relaxed) as f64 / 1e9
            )
        })
        .collect();
    format!("{{{}}}", rows.join(","))
}

#[cfg(test)]
mod probe_test;
