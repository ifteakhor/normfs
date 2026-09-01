//! N listeners on one queue, swept to find where delivery degrades.
//!
//! The publisher keeps a fixed cadence and never waits for listeners, so
//! degradation shows up as lag, not as a slower publisher. Two listener
//! modes: `follow` (one live subscription each) and `poll` (a tail read every
//! poll interval). One RESULT line per run, for the sweep script to collect.
//!
//! Knobs (env): NORMFS_LISTENERS (default 16), NORMFS_MODE=follow|poll,
//! NORMFS_PUBLISHES (1500), NORMFS_PUBLISH_HZ (100), NORMFS_POLL_MS (1).

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, ReadPosition};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uintn::UintN;

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn cpu_seconds() -> f64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap();
    let rest = &s[s.rfind(')').unwrap() + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = f[11].parse().unwrap();
    let stime: u64 = f[12].parse().unwrap();
    (utime + stime) as f64 / 100.0
}

fn peak_rss_mib() -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = s.lines().find(|l| l.starts_with("VmHWM:")).unwrap();
    let kib: f64 = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    kib / 1024.0
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let listeners: usize = env_or("NORMFS_LISTENERS", 16);
    let mode: String = env_or("NORMFS_MODE", "follow".to_string());
    let publishes: usize = env_or("NORMFS_PUBLISHES", 1500);
    let hz: u64 = env_or("NORMFS_PUBLISH_HZ", 100);
    let poll_ms: u64 = env_or("NORMFS_POLL_MS", 1);

    let dir = std::path::PathBuf::from("/tmp/normfs-bench-listeners");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let fs = Arc::new(
        NormFS::new(dir.clone(), NormFsSettings::all_active())
            .await
            .unwrap(),
    );
    let queue = fs.resolve("listeners");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    // One record so a tail read has something to land on before the run.
    fs.enqueue(&queue, Bytes::from_static(b"warmup"))
        .await
        .unwrap();

    let epoch = Instant::now();
    // Publish time of each entry id, nanos since epoch; 0 = not published.
    let publish_ns: Arc<Vec<AtomicU64>> =
        Arc::new((0..publishes + 2).map(|_| AtomicU64::new(0)).collect());
    let delivered = Arc::new(AtomicU64::new(0));
    let dropped_subscriptions = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::with_capacity(listeners);
    for _ in 0..listeners {
        let fs = fs.clone();
        let queue = queue.clone();
        let publish_ns = publish_ns.clone();
        let delivered = delivered.clone();
        let dropped = dropped_subscriptions.clone();
        let stop = stop.clone();
        let mode = mode.clone();
        handles.push(tokio::spawn(async move {
            let mut local: Vec<u64> = Vec::with_capacity(publishes);
            let mut last_seen: u64 = 0;
            let record = |id: u64, local: &mut Vec<u64>| {
                if let Some(slot) = publish_ns.get(id as usize) {
                    let t = slot.load(Ordering::Acquire);
                    if t != 0 {
                        let now = epoch.elapsed().as_nanos() as u64;
                        local.push(now.saturating_sub(t) / 1000);
                        delivered.fetch_add(1, Ordering::Relaxed);
                    }
                }
            };
            if mode == "follow" {
                let (tx, mut rx) = tokio::sync::mpsc::channel(4096);
                // Subscribe from the next record: no backlog.
                if fs
                    .read(&queue, ReadPosition::ShiftFromTail(UintN::zero()), 0, 1, tx)
                    .await
                    .is_err()
                {
                    dropped.fetch_add(1, Ordering::Relaxed);
                    return local;
                }
                while let Some(entry) = rx.recv().await {
                    record(entry.id.to_u64().unwrap_or(0), &mut local);
                }
                // The stream ends when the queue closes at the end of the run;
                // ending before that means the subscription was dropped, which
                // is itself a degradation signal.
                if !stop.load(Ordering::Acquire) {
                    dropped.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                while !stop.load(Ordering::Acquire) {
                    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                    if fs
                        .read(&queue, ReadPosition::ShiftFromTail(UintN::zero()), 1, 1, tx)
                        .await
                        .is_ok()
                    {
                        if let Some(entry) = rx.recv().await {
                            let id = entry.id.to_u64().unwrap_or(0);
                            if id > last_seen {
                                // A poller sees only the newest record; ids
                                // it skipped count as never delivered.
                                last_seen = id;
                                record(id, &mut local);
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(poll_ms)).await;
                }
            }
            local
        }));
    }
    // Let subscriptions land before the clock starts.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let cpu0 = cpu_seconds();
    let wall0 = Instant::now();
    let mut enqueue_us: Vec<u64> = Vec::with_capacity(publishes);
    let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / hz));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    for i in 0..publishes {
        tick.tick().await;
        // Entry ids start after the warmup record (id 0), so entry i gets id i+1.
        let id = i as u64 + 1;
        publish_ns[id as usize].store(epoch.elapsed().as_nanos() as u64, Ordering::Release);
        let t = Instant::now();
        fs.enqueue(&queue, Bytes::from(format!("entry-{i}")))
            .await
            .unwrap();
        enqueue_us.push(t.elapsed().as_micros() as u64);
    }
    // Grace for the last deliveries.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let wall = wall0.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu0;

    // Pollers stop on the flag; followers end when the queue closes and their
    // subscriptions drop. Then every listener returns what it recorded.
    stop.store(true, Ordering::Release);
    fs.close_queue(&queue).await.unwrap();
    let mut all: Vec<u64> = Vec::with_capacity(listeners * publishes);
    for h in handles {
        if let Ok(local) = h.await {
            all.extend(local);
        }
    }
    let _ = fs.close().await;
    all.sort_unstable();
    enqueue_us.sort_unstable();

    let expected = (listeners * publishes) as u64;
    println!(
        "RESULT mode={} listeners={} publishes={} hz={} poll_ms={} \
         enqueue_p50_us={} enqueue_p99_us={} enqueue_max_us={} \
         lag_p50_us={} lag_p99_us={} lag_max_us={} \
         delivered={} expected={} dropped_subs={} cpu_s={:.2} rss_mib={:.0} wall_s={:.1}",
        mode,
        listeners,
        publishes,
        hz,
        poll_ms,
        pct(&enqueue_us, 0.5),
        pct(&enqueue_us, 0.99),
        enqueue_us.last().copied().unwrap_or(0),
        pct(&all, 0.5),
        pct(&all, 0.99),
        all.last().copied().unwrap_or(0),
        all.len(),
        expected,
        dropped_subscriptions.load(Ordering::Relaxed),
        cpu,
        peak_rss_mib(),
        wall,
    );
}
