use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::{Fs, FsConfig, PublishSpec, TmpMode};

struct RestorePermissions(PathBuf);

impl Drop for RestorePermissions {
    fn drop(&mut self) {
        fs::set_permissions(&self.0, fs::Permissions::from_mode(0o700)).unwrap();
    }
}

fn permissions(path: &Path, mode: u32) -> RestorePermissions {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    RestorePermissions(path.to_owned())
}

fn fs() -> Fs {
    Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap()
}

#[tokio::test]
async fn publication_below_a_search_only_ancestor() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("service");
    fs::create_dir(&base).unwrap();
    let _restore = permissions(root.path(), 0o111);
    assert_eq!(
        fs::File::open(root.path()).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    let fs = fs();
    let leaf = base.join("queue/wal");
    fs.mkdir_all(&leaf).await.unwrap();
    fs.mkdir_all(&leaf).await.unwrap();
    let dst = leaf.join("target");
    fs.publish(
        PublishSpec {
            tmp: leaf.join("tmp"),
            dst: dst.clone(),
            runs: crate::Runs(vec![bytes::Bytes::from_static(b"committed")]),
            tmp_mode: TmpMode::Excl,
            sync: true,
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(fs.read_whole(&dst).await.unwrap(), b"committed"[..]);
}

#[tokio::test]
async fn a_failed_parent_sync_is_retried_across_fs_instances() {
    let root = tempfile::tempdir().unwrap();
    let leaf = root.path().join("new");
    let restore = permissions(root.path(), 0o300);
    assert_eq!(
        fs::File::open(root.path()).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    let first = fs();
    assert!(first.mkdir_all(&leaf).await.is_err());
    assert!(leaf.is_dir());
    let second = fs();
    assert!(second.mkdir_all(&leaf).await.is_err());
    drop(restore);
    second.mkdir_all(&leaf).await.unwrap();
    let _restore = permissions(root.path(), 0o111);
    second.mkdir_all(&leaf).await.unwrap();
}

#[tokio::test]
async fn concurrent_creation_shares_completion_for_the_same_directory() {
    let root = tempfile::tempdir().unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    let fs = Fs::new(FsConfig {
        threads: 4,
        ..Default::default()
    })
    .unwrap();
    for i in 0..48 {
        let fs = fs.clone();
        let path = root.path().join(format!("shared/queue-{}/wal", i % 12));
        tasks.spawn(async move {
            fs.mkdir_all(&path).await.unwrap();
            fs.mkdir_all(&path).await.unwrap();
            assert!(path.is_dir());
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
}
