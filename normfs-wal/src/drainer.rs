//! The retry that finishes a file whose closing flush did not land.
//!
//! The rotation cannot wait for it -- the enqueue side has already stamped
//! later pages with the next file -- so the tail comes here instead and the
//! queue carries on. On a card that never recovers this is back-pressure rather
//! than loss: the pool fills and `enqueue` waits.

use std::path::PathBuf;
use std::time::Duration;

use normfs_types::QueueId;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use uintn::UintN;

use crate::WalFile;
use crate::page_pool::{PagePool, Stranded};
use std::sync::Arc;

/// The rotation's open-retry delay; the failures are the same failures.
const DRAIN_RETRY_DELAY: Duration = Duration::from_millis(10);

const DRAIN_WARN_EVERY: u32 = 500;

pub(crate) struct StrandedFile {
    pub queue_id: QueueId,
    pub file_id: UintN,
    pub epoch: u64,
    pub path: PathBuf,
    /// Re-applied rather than trusted: `FileTail::restore` only logs when its
    /// own truncate fails.
    pub valid_len: u64,
    pub stranded: Stranded,
    /// Withheld until the tail lands: completion is what makes the store
    /// worker archive the file and unlink it.
    pub wal_file: WalFile,
    pub fsync: bool,
}

pub(crate) enum DrainRequest {
    Retry(Box<StrandedFile>),
}

/// Outlives the writer that spawned it -- the channel delivers what is still
/// queued after the sender drops -- so a close that reports itself incomplete
/// can be followed by one that succeeds.
pub(crate) fn spawn(
    pool: Arc<PagePool>,
    wal_complete_sender: mpsc::UnboundedSender<WalFile>,
    written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
) -> mpsc::UnboundedSender<DrainRequest> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(DrainRequest::Retry(file)) = rx.recv().await {
            if !land(&file).await {
                // Left owed on purpose: a close certifies that everything
                // accepted is on disk, and saying "incomplete" for ever beats
                // certifying the loss. A restart clears it.
                continue;
            }
            pool.clear_stranded(file.epoch);
            log::info!(
                target: "normfs-wal",
                "WAL drainer: queue '{}': file {} landed its tail, entries {}..={}",
                file.queue_id,
                file.file_id,
                file.stranded.first_entry_id,
                file.stranded.last_entry_id
            );
            let _ = wal_complete_sender.send(file.wal_file);
            let _ = written_sender.send((file.queue_id, UintN::from(file.stranded.last_entry_id)));
        }
    });
    tx
}

/// False only for a failure no retry can pass, never for giving up on one.
async fn land(file: &StrandedFile) -> bool {
    let mut attempt: u32 = 0;
    loop {
        match attempt_once(file).await {
            Ok(()) => return true,
            Err(Fatal) => {
                log::error!(
                    target: "normfs-wal",
                    "WAL drainer: queue '{}': file {} cannot take its tail back; entries \
                     {}..={} reach no file. The file is gone or shorter than the prefix \
                     they follow, which no retry can undo.",
                    file.queue_id,
                    file.file_id,
                    file.stranded.first_entry_id,
                    file.stranded.last_entry_id
                );
                return false;
            }
            Err(Transient(e)) => {
                if attempt % DRAIN_WARN_EVERY == 0 {
                    log::error!(
                        target: "normfs-wal",
                        "WAL drainer: queue '{}': landing the tail of file {} failed ({}); \
                         retrying. Entries {}..={} are held here and nowhere else, so this \
                         queue cannot report them durable until it succeeds.",
                        file.queue_id,
                        file.file_id,
                        e,
                        file.stranded.first_entry_id,
                        file.stranded.last_entry_id
                    );
                }
                attempt = attempt.saturating_add(1);
                tokio::time::sleep(DRAIN_RETRY_DELAY).await;
            }
        }
    }
}

use Failure::{Fatal, Transient};

enum Failure {
    Transient(std::io::Error),
    Fatal,
}

async fn attempt_once(file: &StrandedFile) -> Result<(), Failure> {
    // Never `create`: a close usually fails on a full disk, which is when the
    // offload monitor deletes WAL files oldest-first, and this is the oldest
    // survivor. Creating it would pair `set_len` with an empty file and write
    // the tail after a run of NULs.
    let mut handle = OpenOptions::new()
        .write(true)
        .open(&file.path)
        .await
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Fatal
            } else {
                Transient(e)
            }
        })?;

    let len = handle.metadata().await.map_err(Transient)?.len();
    if len < file.valid_len {
        return Err(Fatal);
    }

    // Before every attempt: a previous one may have left bytes behind, and V1
    // derives ids from position, so a stray frame renumbers what follows.
    handle.set_len(file.valid_len).await.map_err(Transient)?;
    handle
        .seek(std::io::SeekFrom::Start(file.valid_len))
        .await
        .map_err(Transient)?;

    for (_, bytes) in &file.stranded.runs {
        handle.write_all(bytes).await.map_err(Transient)?;
    }
    // Its own handle, so without this an injected outage would stop at the
    // closing flush and the retry would land on its first attempt.
    if crate::fault::take_failure(&file.path) {
        return Err(Transient(std::io::Error::other(
            "injected failure landing a stranded tail",
        )));
    }
    handle.flush().await.map_err(Transient)?;
    if file.fsync {
        handle.sync_all().await.map_err(Transient)?;
    }
    Ok(())
}
