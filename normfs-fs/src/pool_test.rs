use std::fs;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;

use crate::{AppendOutcome, Fs, FsConfig, PublishSpec, Runs, Scan, ScanResult, TmpMode};

fn fs() -> Fs {
    Fs::new(FsConfig {
        threads: 2,
        ..FsConfig::default()
    })
    .unwrap()
}

fn runs(parts: &[&[u8]]) -> Runs {
    Runs(parts.iter().map(|p| Bytes::copy_from_slice(p)).collect())
}

#[tokio::test]
async fn publish_lands_the_bytes_and_leaves_no_temp() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let tmp = dir.path().join("a.tmp");
    let dst = dir.path().join("a.store");
    let report = fs
        .publish(
            PublishSpec {
                tmp: tmp.clone(),
                dst: dst.clone(),
                runs: runs(&[b"head", b"-", b"body"]),
                tmp_mode: TmpMode::Excl,
                sync: true,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(report.old_len, None);
    assert_eq!(report.new_len, 9);
    assert_eq!(fs::read(&dst).unwrap(), b"head-body");
    assert!(!tmp.exists());

    let report = fs
        .publish(
            PublishSpec {
                tmp,
                dst: dst.clone(),
                runs: runs(&[b"v2"]),
                tmp_mode: TmpMode::Excl,
                sync: true,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(report.old_len, Some(9));
    assert_eq!(fs::read(&dst).unwrap(), b"v2");
}

#[tokio::test]
async fn publish_failure_reports_errno_and_cleans_up() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let tmp = dir.path().join("b.tmp");
    let dst = dir.path().join("missing").join("b.store");
    let err = fs
        .publish(
            PublishSpec {
                tmp: tmp.clone(),
                dst,
                runs: runs(&[b"x"]),
                tmp_mode: TmpMode::Excl,
                sync: true,
            },
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        std::io::Error::from(err).kind(),
        std::io::ErrorKind::NotFound
    );
    assert!(!tmp.exists());
}

#[tokio::test]
async fn accounting_runs_on_the_executor_even_when_the_future_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let (tx, rx) = mpsc::channel::<(Option<u64>, u64, String)>();
    let then = Box::new(move |r: &crate::PublishReport| {
        let name = std::thread::current().name().unwrap_or("").to_string();
        let _ = tx.send((r.old_len, r.new_len, name));
    });
    let fut = fs.publish(
        PublishSpec {
            tmp: dir.path().join("c.tmp"),
            dst: dir.path().join("c.store"),
            runs: runs(&[b"abc"]),
            tmp_mode: TmpMode::Excl,
            sync: true,
        },
        Some(then),
    );
    // Poll once so the job is submitted, then drop the future.
    let mut fut = Box::pin(fut);
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let _ = std::future::Future::poll(fut.as_mut(), &mut cx);
    drop(fut);

    let (old, new, thread) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(old, None);
    assert_eq!(new, 3);
    assert!(thread.starts_with("normfs-fs-"), "ran on {thread}");
    assert_eq!(fs::read(dir.path().join("c.store")).unwrap(), b"abc");
}

#[tokio::test]
async fn append_commits_whole_batches_and_cuts_back_on_failure() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let path = dir.path().join("q.wal");
    let file = Arc::new(
        fs.create_durable(&path, runs(&[b"HDR"]), TmpMode::Excl, true)
            .await
            .unwrap(),
    );
    assert_eq!(fs::read(&path).unwrap(), b"HDR");

    let out = fs
        .append_sync(file.clone(), &path, 3, runs(&[b"one", b"two"]), true)
        .await
        .unwrap();
    assert!(matches!(out, AppendOutcome::Committed));
    assert_eq!(fs::read(&path).unwrap(), b"HDRonetwo");

    crate::fault::fail_flushes(&path, 1);
    let out = fs
        .append_sync(file.clone(), &path, 9, runs(&[b"three"]), true)
        .await
        .unwrap();
    match out {
        AppendOutcome::Failed { err, restored } => {
            assert!(restored);
            assert_eq!(err.raw_os_error(), Some(libc::EIO));
        }
        AppendOutcome::Committed => panic!("committed through an injected failure"),
    }
    assert_eq!(fs::read(&path).unwrap(), b"HDRonetwo");

    let out = fs
        .append_sync(file.clone(), &path, 9, runs(&[b"three"]), true)
        .await
        .unwrap();
    assert!(matches!(out, AppendOutcome::Committed));
    assert_eq!(fs::read(&path).unwrap(), b"HDRonetwothree");

    let out = fs
        .append_sync(file, &path, 14, Runs::default(), true)
        .await
        .unwrap();
    assert!(matches!(out, AppendOutcome::Committed));
}

#[tokio::test]
async fn restore_cuts_the_file_back() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let path = dir.path().join("r.wal");
    fs::write(&path, b"0123456789").unwrap();
    let file = Arc::new(fs::OpenOptions::new().write(true).open(&path).unwrap());
    fs.restore(file, &path, 4).await.unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"0123");
}

#[tokio::test]
async fn markers_come_and_go_durably() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let marker = dir.path().join("closed");
    let f = fs
        .create_durable(&marker, Runs::default(), TmpMode::Trunc, true)
        .await
        .unwrap();
    drop(f);
    assert!(marker.is_file());
    assert!(fs.remove_durable(&marker, true).await.unwrap());
    assert!(!marker.exists());
    assert!(!fs.remove_durable(&marker, false).await.unwrap());

    let err = fs
        .create_durable(
            &dir.path().join("no").join("dir"),
            Runs::default(),
            TmpMode::Excl,
            true,
        )
        .await
        .unwrap_err();
    assert_eq!(
        std::io::Error::from(err).kind(),
        std::io::ErrorKind::NotFound
    );
}

#[tokio::test]
async fn reads_stats_and_scans_run_off_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let wal = dir.path().join("wal");
    fs.mkdir_all(&wal.join("abc")).await.unwrap();
    fs::write(wal.join("abc").join("def.wal"), b"x").unwrap();
    fs::write(wal.join("abc").join("001.wal"), b"yy").unwrap();

    assert_eq!(
        fs.read_whole(&wal.join("abc").join("001.wal"))
            .await
            .unwrap(),
        Bytes::from_static(b"yy")
    );
    assert!(fs.stat(&wal.join("abc")).await.unwrap().unwrap().is_dir());
    assert!(fs.stat(&wal.join("nope")).await.unwrap().is_none());

    let max = fs.scan_ids(&wal, "wal", Scan::Max).await.unwrap();
    let min = fs.scan_ids(&wal, "wal", Scan::Min).await.unwrap();
    assert_eq!(max, ScanResult::One(uintn::UintN::from(0xabcdefu64)));
    assert_eq!(min, ScanResult::One(uintn::UintN::from(0xabc001u64)));
    assert_eq!(
        fs.scan_ids(&dir.path().join("none"), "wal", Scan::Max)
            .await
            .unwrap(),
        ScanResult::None
    );

    fs.unlink(&wal.join("abc").join("001.wal")).await.unwrap();
    assert!(fs.unlink(&wal.join("abc").join("001.wal")).await.is_err());
}
