/// The [`StateBackend`](backend::StateBackend) trait for key-value storage.
pub mod backend;
/// Operator-scoped [`StateContext`](context::StateContext) (memtable + backend).
pub mod context;
/// List-valued typed state wrapper.
pub mod list_state;
/// JSON-file-backed local state backend.
pub mod local_backend;
/// Map-valued typed state wrapper with per-key storage.
pub mod map_state;
/// In-memory write buffer with dirty tracking.
pub mod memtable;
/// Key-namespacing backend wrapper.
pub mod prefixed_backend;
/// `SlateDB` object-storage backend.
pub mod slatedb_backend;
/// Tiered backend: Foyer L2 cache + `SlateDB` L3 storage.
pub mod tiered_backend;
/// Event-time timer service.
pub mod timer_service;
/// Single-value typed state wrapper.
pub mod value_state;
