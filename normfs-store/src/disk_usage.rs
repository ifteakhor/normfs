use normfs_fs::{Fs, PublishReport, PublishSpec};
use normfs_types::QueueId;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Default)]
pub struct DiskUsage {
    queues: Mutex<HashMap<QueueId, Arc<AsyncMutex<u64>>>>,
}

impl DiskUsage {
    pub fn queue(&self, queue: &QueueId) -> Arc<AsyncMutex<u64>> {
        self.queues
            .lock()
            .unwrap()
            .entry(queue.clone())
            .or_default()
            .clone()
    }

    /// The lock is held across the rename so a rescan never counts the new
    /// file and then sees it added again.
    pub async fn publish(&self, fs: &Fs, queue: &QueueId, spec: PublishSpec) -> io::Result<()> {
        let mut tracked = self.queue(queue).lock_owned().await;
        fs.publish(
            spec,
            Some(Box::new(move |report: &PublishReport| {
                *tracked = tracked
                    .saturating_sub(report.old_len.unwrap_or(0))
                    .saturating_add(report.new_len);
            })),
        )
        .await?;
        Ok(())
    }
}
