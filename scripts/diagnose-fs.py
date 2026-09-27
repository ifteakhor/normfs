#!/usr/bin/env python3
"""Build isolated diagnostic copies and measure store publication barriers."""

import argparse
import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import tarfile
import time

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "target/fs-diagnosis"
DEV = "e5895b1f2817c63f5f49ade77c137a579b385907"
PROBE = ROOT / "benchmarks/fs-diagnosis/probe"
CARGO = Path.home() / ".cargo/bin/cargo"


def replace(path, old, new):
    source = path.read_text()
    if source.count(old) != 1:
        raise RuntimeError(f"Expected one instrumentation anchor in {path}: {old!r}")
    path.write_text(source.replace(old, new))


def instrument_store(root):
    writer = root / "normfs-store/src/writer.rs"
    replace(writer, "        let queue_id = &wal_file.queue_id;", """        let _process = normfs_bench_probe::Timer::new(normfs_bench_probe::Metric::Process);
        let read = normfs_bench_probe::Timer::new(normfs_bench_probe::Metric::ReadWal);
        let queue_id = &wal_file.queue_id;""")
    source = writer.read_text()
    start = source.index("        let wal_data = match")
    end = source.index("\n        };", start) + len("\n        };")
    writer.write_text(source[:end] + "\n        drop(read);" + source[end:])


def prepare():
    BUILD.mkdir(parents=True, exist_ok=False)
    for version, revision in [("dev", DEV), ("fs", "HEAD")]:
        root = BUILD / version
        root.mkdir()
        archive = subprocess.check_output(["git", "archive", revision], cwd=ROOT)
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            tar.extractall(root)
        if version == "fs":
            changed = subprocess.check_output(["git", "diff", "HEAD", "--name-only"], cwd=ROOT, text=True)
            for name in changed.splitlines():
                if name.startswith(("normfs-", "uintn-rs/")) and (ROOT / name).is_file():
                    shutil.copyfile(ROOT / name, root / name)
        for name in ["benchmarks/fs-driver/Cargo.toml", "normfs-wal/examples/fs_comparison.rs", "normfs-store/examples/fs_publication.rs"]:
            (root / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, root / name)
        lock = "benchmarks/fs-driver/Cargo.lock" if version == "fs" else "benchmarks/fs-comparison-2026-09-21/dev-driver.Cargo.lock"
        shutil.copyfile(ROOT / lock, root / "benchmarks/fs-driver/Cargo.lock")
        if version == "dev":
            subprocess.run(["git", "apply", "--unsafe-paths", "--directory=" + str(root),
                            str(ROOT / "benchmarks/fs-comparison-2026-09-21/dev-store-api.patch")], check=True, cwd=ROOT)
        crates = ["normfs-store", "benchmarks/fs-driver"]
        if version == "fs":
            crates.append("normfs-fs")
        for crate in crates:
            replace(root / crate / "Cargo.toml", "[dependencies]\n",
                    '[dependencies]\nnormfs-bench-probe = { path = ' + json.dumps(str(PROBE)) + ' }\n')
        harness = root / "normfs-store/examples/fs_publication.rs"
        replace(harness, "    let start = Instant::now();", """    let mut directories = std::collections::BTreeSet::new();
    for (queue, file) in &expected {
        let path = queue.to_store_path(tmp.path(), file);
        let mut parent = path.parent();
        while let Some(dir) = parent {
            if !dir.starts_with(tmp.path()) { break; }
            std::fs::create_dir_all(dir).unwrap();
            directories.insert(dir.to_path_buf());
            parent = dir.parent();
        }
    }
    let temp = tmp.path().join("tmp");
    std::fs::create_dir_all(&temp).unwrap();
    directories.insert(temp);
    for dir in directories.iter().rev() {
        std::fs::File::open(dir).unwrap().sync_all().unwrap();
    }
    std::fs::File::open(tmp.path().parent().unwrap()).unwrap().sync_all().unwrap();
    normfs_bench_probe::start(std::env::var("NORMFS_BENCH_PROFILE").unwrap() == "1");
    let start = Instant::now();""")
        replace(harness, "    let elapsed = start.elapsed().as_secs_f64();", """    let elapsed = start.elapsed().as_secs_f64();
    println!("PROFILE {}", normfs_bench_probe::stop_json());""")
        instrument_store(root)
        if version == "dev":
            writer = root / "normfs-store/src/writer.rs"
            replace(writer, "        file.write_all(&auth_bytes).await?;", """        let write_timer = normfs_bench_probe::Timer::new(normfs_bench_probe::Metric::DevWrite);
        file.write_all(&auth_bytes).await?;""")
            replace(writer, "        file.sync_all().await?;", """        drop(write_timer);
        let file_sync = normfs_bench_probe::Timer::new(normfs_bench_probe::Metric::DevFileSync);
        file.sync_all().await?;
        drop(file_sync);""")
            replace(writer, "        fs::rename(&temp_file_path, &store_file_path).await?;", """        let rename = normfs_bench_probe::Timer::new(normfs_bench_probe::Metric::DevRename);
        fs::rename(&temp_file_path, &store_file_path).await?;
        drop(rename);
        if std::env::var("NORMFS_BENCH_DIRSYNC").unwrap() == "1" {
            let parent = store_file_path.parent().unwrap().to_path_buf();
            let queued = normfs_bench_probe::now();
            tokio::task::spawn_blocking(move || {
                normfs_bench_probe::record(normfs_bench_probe::Metric::DevDirQueue, queued);
                let _sync = normfs_bench_probe::Timer::new(normfs_bench_probe::Metric::DevDirSync);
                std::fs::File::open(parent)?.sync_all()
            }).await.map_err(std::io::Error::other)??;
        }""")
        else:
            lib = root / "normfs-fs/src/lib.rs"
            replace(lib, "        let permit = self\n", "        let admission = normfs_bench_probe::Timer::new(normfs_bench_probe::Metric::Admission);\n        let permit = self\n")
            replace(lib, "        self.exec.submit(Job { task, permit })", """        drop(admission);
        self.exec.submit(Job { task, permit, queued: normfs_bench_probe::now() })""")
            replace(lib, "        self.run_blocking(move || directory::mkdir_all(&path))\n            .await", """        self.run_blocking(move || {
            let _timer = normfs_bench_probe::Timer::new(normfs_bench_probe::Metric::Mkdir);
            directory::mkdir_all(&path)
        }).await""")
            replace(root / "normfs-fs/src/executor.rs", "pub(crate) struct Job {", "pub(crate) struct Job {\n    pub queued: Option<std::time::Instant>,")
            pool = root / "normfs-fs/src/pool.rs"
            replace(pool, "            Ok(Job { task, permit }) => {", """            Ok(Job { task, permit, queued }) => {
                normfs_bench_probe::record(normfs_bench_probe::Metric::Queue, queued);""")
            replace(pool, "        let op = plan.next()?;", """        let op = plan.next()?;
        let _operation = if plan.kind() == Kind::Publish {
            use normfs_bench_probe::{Metric, Timer};
            Some(Timer::new(match op {
                Op::Open => Metric::Open,
                Op::Write => Metric::Write,
                Op::FsyncFile => Metric::FileSync,
                Op::CloseFile => Metric::Close,
                Op::StatDst => Metric::Stat,
                Op::Rename => Metric::Rename,
                Op::FsyncDir => Metric::DirSync,
                _ => Metric::OtherPlan,
            }))
        } else { None };""")
        manifest = root / "benchmarks/fs-driver/Cargo.toml"
        subprocess.run([str(CARGO), "build", "--release", "--offline", "--manifest-path", str(manifest)], check=True)
        subprocess.run([str(CARGO), "build", "--release", "--locked", "--offline", "--manifest-path", str(manifest)], check=True)


def measure(output):
    binaries = {v: BUILD / v / "benchmarks/fs-driver/target/release/fs_publication" for v in ("dev", "fs")}
    output.mkdir(parents=True, exist_ok=False)
    metadata = dict(started=datetime.datetime.now().astimezone().isoformat(), dev=DEV,
                    fs=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                    os=subprocess.check_output(["sw_vers"], text=True),
                    rounds=4, control_rounds=2,
                    binaries={v: hashlib.sha256(p.read_bytes()).hexdigest() for v,p in binaries.items()})
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    variants = [("dev", "dev", "0"), ("dev_dirsync", "dev", "1"), ("fs", "fs", "0")]
    with (output / "results.jsonl").open("x") as results:
        for iteration in range(1, 7):
            profile = iteration not in (3, 6)
            ordered = variants[iteration % 3:] + variants[:iteration % 3]
            if iteration % 2 == 0:
                ordered = list(reversed(ordered))
            for queues in ([1, 12] if iteration % 2 else [12, 1]):
                for name, version, dirsync in ordered:
                    subprocess.run(["sync"], check=True)
                    time.sleep(3)
                    env = dict(os.environ, NORMFS_BENCH_PROFILE=str(int(profile)), NORMFS_BENCH_DIRSYNC=dirsync)
                    before = resource.getrusage(resource.RUSAGE_CHILDREN)
                    start = time.perf_counter()
                    run = subprocess.run([str(binaries[version]), str(queues)], env=env, capture_output=True, text=True, timeout=120)
                    wall = time.perf_counter() - start
                    after = resource.getrusage(resource.RUSAGE_CHILDREN)
                    (output / f"{iteration}-{name}-{queues}.log").write_text(run.stdout + run.stderr)
                    run.check_returncode()
                    def result(prefix):
                        lines = [line[len(prefix):] for line in run.stdout.splitlines() if line.startswith(prefix)]
                        if len(lines) != 1:
                            raise RuntimeError(f"Missing {prefix} in {name} round {iteration}")
                        return json.loads(lines[0])
                    row = result("RESULT ")
                    row.update(version=name, round=iteration, profiled=profile, profile=result("PROFILE "),
                               process_seconds=wall, user_seconds=after.ru_utime-before.ru_utime,
                               system_seconds=after.ru_stime-before.ru_stime)
                    if profile:
                        assert row["profile"]["process"]["calls"] == 384
                        sync_metric = "dir_sync" if name == "fs" else "dev_dir_sync"
                        assert row["profile"][sync_metric]["calls"] == (0 if name == "dev" else 384)
                    else:
                        assert all(m["calls"] == 0 for m in row["profile"].values())
                    results.write(json.dumps(row) + "\n")
                    results.flush()
                    print(f"round={iteration} variant={name} queues={queues} profile={profile} MiB/s={row['publish_mib_s']:.2f}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "measure"])
    parser.add_argument("--output", type=Path, default=ROOT / "benchmarks/fs-diagnosis/results-2026-09-24")
    args = parser.parse_args()
    os.chdir(ROOT)
    if args.action == "prepare":
        prepare()
    else:
        measure(args.output)


if __name__ == "__main__":
    main()
