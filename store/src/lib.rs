//! How prism's objects are named and which of them are worth reading.

pub mod merged;

mod live;
mod writer;

pub use live::LiveStore;
pub use writer::ObjectWriter;

/// Backend-agnostic url the object store is mounted under, so paths are
/// identical whether they resolve to memory or a bucket. Datafusion keys
/// registered stores by scheme *and* host, so both must appear here. Lives here
/// rather than in one consumer so every crate mounts it at the same place.
pub const OBJECT_STORE_URL: &str = "prism://store";
