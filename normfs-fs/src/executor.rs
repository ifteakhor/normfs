//! What an executor is: something that runs plans and blocking closures on
//! threads that are not the runtime's.

use std::fs::File;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, oneshot};

use crate::plan::Plan;
use crate::{FsError, PublishReport};

/// Runs after a `Publish` plan reaches `Done`, on the executor's thread,
/// before the caller is told. A caller that must keep its bookkeeping in
/// step with the rename puts the bookkeeping here: the future that awaits
/// the job can be dropped, this cannot. A panic is logged without changing
/// the committed publication result.
pub type Accounting = Box<dyn FnOnce(&PublishReport) + Send + 'static>;

/// What a plan needs beyond its paths.
pub(crate) struct Resources {
    /// The open file for `Append` and `Restore`; the executor never closes it.
    pub file: Option<Arc<File>>,
    /// The bytes `Write` steps hand to the kernel, in order.
    pub runs: Vec<Bytes>,
    /// With this off the fsync steps are reported done without being done,
    /// and nothing proved about durability applies to the plan.
    pub sync: bool,
}

/// A plan that has stopped, at `Done` or `Failed`.
pub(crate) struct Finished {
    pub plan: Plan,
    /// The file a `Create` plan opened, still open, when it reached `Done`.
    pub file: Option<File>,
    /// A `Remove` plan found nothing to unlink.
    pub absent: bool,
}

pub(crate) struct PlanJob {
    pub plan: Plan,
    pub res: Resources,
    pub then: Option<Accounting>,
    pub reply: oneshot::Sender<Result<Finished, FsError>>,
}

pub(crate) struct Job {
    pub task: Task,
    pub permit: OwnedSemaphorePermit,
}

pub(crate) enum Task {
    Plan(PlanJob),
    Blocking(Box<dyn FnOnce() + Send + 'static>),
}

pub(crate) trait Executor: Send + Sync {
    /// Queues a job. A queued job always runs to completion, whether or not
    /// anyone is still waiting for its reply.
    fn submit(&self, job: Job) -> Result<(), FsError>;
    fn name(&self) -> &'static str;
}
