//! The filesystem layer: every write and every directory scan in NormFS goes
//! through here, on an executor that is not a tokio worker thread.
//!
//! This is the skeleton. The proved planner, the pool executor and the API
//! land in the next change; what is here now is what `examples/fs_gate.rs`
//! needs to compile.
