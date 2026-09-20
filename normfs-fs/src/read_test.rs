use std::fs;
use std::sync::mpsc;

use crate::{Fs, FsConfig};

#[tokio::test]
async fn pool_reader_keeps_bytes_when_a_read_is_cancelled_or_resized() {
    use std::future::Future;
    use tokio::io::AsyncReadExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    fs::write(&path, b"0123456789").unwrap();
    let fs = Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap();
    let mut reader = fs.open_read(&path).await.unwrap();
    let (release, blocked) = mpsc::channel();
    let mut blocker = Box::pin(fs.run_blocking(move || {
        blocked.recv().unwrap();
        Ok(())
    }));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(blocker.as_mut().poll(&mut cx).is_pending());
    let mut large = [0; 10];
    let mut read = Box::pin(reader.read(&mut large));
    assert!(read.as_mut().poll(&mut cx).is_pending());
    drop(read);
    release.send(()).unwrap();
    blocker.await.unwrap();
    let mut small = [0; 3];
    reader.read_exact(&mut small).await.unwrap();
    assert_eq!(&small, b"012");
    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).await.unwrap();
    assert_eq!(rest, b"3456789");
    reader.seek(std::io::SeekFrom::End(-2)).await.unwrap();
    rest.clear();
    reader.read_to_end(&mut rest).await.unwrap();
    assert_eq!(rest, b"89");
}

#[tokio::test]
async fn sequential_reads_and_seek_reuse_the_allocation() {
    use tokio::io::AsyncReadExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    fs::write(&path, vec![0x5a; 3 * 1024 * 1024]).unwrap();
    let fs = Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap();
    let mut reader = fs.open_read(&path).await.unwrap();
    let mut block = vec![0; 1024 * 1024];
    reader.read_exact(&mut block).await.unwrap();
    let allocation = reader.buffer.as_ptr();
    for _ in 0..2 {
        reader.read_exact(&mut block).await.unwrap();
        assert_eq!(reader.buffer.as_ptr(), allocation);
        assert!(block.iter().all(|&b| b == 0x5a));
    }
    reader.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    reader.read_exact(&mut block[..4096]).await.unwrap();
    assert_eq!(reader.buffer.as_ptr(), allocation);
}
