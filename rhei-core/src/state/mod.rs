/// The [`StateBackend`](backend::StateBackend) trait for key-value storage.
pub mod backend;
/// Operator-scoped [`StateContext`](context::StateContext) (memtable + backend).
pub mod context;
/// JSON-file-backed local state backend.
pub mod local_backend;
/// In-memory write buffer with dirty tracking.
pub mod memtable;
/// Key-namespacing backend wrapper.
pub mod prefixed_backend;
/// `SlateDB` object-storage backend.
pub mod slatedb_backend;
/// Tiered backend: Foyer L2 cache + `SlateDB` L3 storage.
pub mod tiered_backend;
