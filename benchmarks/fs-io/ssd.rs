#[cfg(not(target_os = "macos"))]
compile_error!("This benchmark requires macOS F_NOCACHE and F_FULLFSYNC");

use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Barrier};
use std::time::Instant;

fn cpu() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: usage points to writable storage sized for rusage.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    // SAFETY: a successful getrusage initialized usage.
    let usage = unsafe { usage.assume_init() };
    (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64
        + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1e6
}

fn control(file: &File, command: libc::c_int, value: libc::c_int) {
    // SAFETY: the live file descriptor and integer argument match these fcntl commands.
    let result = unsafe { libc::fcntl(file.as_raw_fd(), command, value) };
    assert_eq!(
        result,
        0,
        "fcntl failed: {}",
        std::io::Error::last_os_error()
    );
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(
        args.len(),
        5,
        "ssd_io BLOCK_BYTES CLIENTS TOTAL_MIB TEMP_PARENT"
    );
    let block: usize = args[1].parse().unwrap();
    let clients: usize = args[2].parse().unwrap();
    let total: usize = args[3].parse::<usize>().unwrap() * 1048576;
    assert!([1 << 20, 4 << 20].contains(&block));
    assert!([1, 8, 16, 32].contains(&clients));
    assert!(total >= clients * block && total <= 64usize << 30);
    assert_eq!(total % (clients * block), 0);
    let count = total / clients / block;
    let mut state = 0x9e3779b97f4a7c15u64;
    let data: Arc<Vec<Vec<u8>>> = Arc::new(
        (0..16)
            .map(|_| {
                let mut bytes = vec![0u8; block];
                for chunk in bytes.chunks_exact_mut(8) {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    chunk.copy_from_slice(&state.to_le_bytes());
                }
                bytes
            })
            .collect(),
    );
    let dir = tempfile::tempdir_in(&args[4]).unwrap();
    let files: Vec<_> = (0..clients)
        .map(|index| {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(dir.path().join(index.to_string()))
                .unwrap();
            control(&file, libc::F_NOCACHE, 1);
            Arc::new(file)
        })
        .collect();
    let mut results = Vec::new();
    for operation in ["write", "read"] {
        let ready = Arc::new(Barrier::new(clients + 1));
        let go = Arc::new(Barrier::new(clients + 1));
        let handles: Vec<_> = files
            .iter()
            .map(|file| {
                let (file, data, ready, go) =
                    (file.clone(), data.clone(), ready.clone(), go.clone());
                std::thread::spawn(move || {
                    let mut buffer = vec![0u8; block];
                    ready.wait();
                    go.wait();
                    for index in 0..count {
                        let offset = (index * block) as u64;
                        if operation == "write" {
                            file.write_all_at(&data[index % data.len()], offset)
                                .unwrap();
                        } else {
                            file.read_exact_at(&mut buffer, offset).unwrap();
                        }
                    }
                    if operation == "write" {
                        control(&file, libc::F_FULLFSYNC, 0);
                    }
                    buffer
                })
            })
            .collect();
        ready.wait();
        let cpu_start = cpu();
        let start = Instant::now();
        go.wait();
        let buffers: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let seconds = start.elapsed().as_secs_f64();
        let cpu_seconds = cpu() - cpu_start;
        if operation == "read" {
            for buffer in buffers {
                assert_eq!(buffer, data[(count - 1) % data.len()]);
            }
        } else {
            // Spot checks stay outside timing so memory comparisons cannot limit SSD bandwidth.
            for file in &files {
                assert_eq!(file.metadata().unwrap().len(), (total / clients) as u64);
                let mut buffer = vec![0u8; block];
                for index in [0, count / 2, count - 1] {
                    file.read_exact_at(&mut buffer, (index * block) as u64)
                        .unwrap();
                    assert_eq!(buffer, data[index % data.len()]);
                }
            }
        }
        results.push(serde_json::json!({
            "operation":operation, "block_bytes":block, "clients":clients,
            "total_bytes":total, "seconds":seconds, "mib_s":total as f64/1048576.0/seconds,
            "gb_s":total as f64/1e9/seconds, "cpu_seconds":cpu_seconds,
            "cpu_seconds_per_gib":cpu_seconds/(total as f64/1073741824.0),
            "nocache":true, "full_sync_on_write":true, "spot_checks_passed":true
        }));
    }
    for result in results {
        println!("RESULT {result}");
    }
}
