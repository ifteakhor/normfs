use bytes::Bytes;
use normfs::NormFS;
use std::time::{Duration, Instant};

fn cpu_seconds() -> f64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap();
    let rest = &s[s.rfind(')').unwrap() + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = f[11].parse().unwrap();
    let stime: u64 = f[12].parse().unwrap();
    (utime + stime) as f64 / 100.0
}

const QUEUES: usize = 100;
const IDLE_SECS: u64 = 60;

#[tokio::main]
async fn main() {
    let dir = std::env::temp_dir().join("normfs-idle-cpu-bench");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let normfs = NormFS::new(dir.clone(), Default::default()).await.unwrap();
    for i in 0..QUEUES {
        let q = normfs.resolve(&format!("idle_{i}"));
        normfs.ensure_queue_exists_for_write(&q).await.unwrap();
        normfs.enqueue(&q, Bytes::from_static(b"one")).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(5)).await;

    let c0 = cpu_seconds();
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs(IDLE_SECS)).await;
    let wall = t0.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - c0;
    println!(
        "idle: {QUEUES} queues, {wall:.0}s wall, {cpu:.2}s cpu = {:.2}% of one core",
        cpu / wall * 100.0
    );
    let _ = normfs.close().await;
}
