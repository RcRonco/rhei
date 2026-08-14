//! Cluster topology: key groups, membership, and the mapping between them.
//!
//! These types are the vocabulary shared by discovery (who is in the cluster),
//! routing (which worker owns a row), and state (where a key's bytes live).
//! Keeping them in one place is what lets all three stay consistent across a
//! rescale.
//!
//! - [`key_group`] — the fixed-cardinality bucket space that decouples key
//!   ownership from worker count.
//! - [`membership`] — cluster views, the deterministic topology resolved from
//!   them, and the [`MembershipProvider`](membership::MembershipProvider) trait
//!   that discovery backends implement.

/// Key groups: the unit of state partitioning and reassignment.
pub mod key_group;
/// Cluster membership and resolved topology.
pub mod membership;

pub use key_group::{
    DEFAULT_MAX_PARALLELISM, KeyGroupAssignment, KeyGroupId, KeyGroupMigration, KeyGroupRange,
    effective_parallelism, key_group_for, range_for_worker, worker_for_key, worker_for_key_group,
};
pub use membership::{
    ClusterTopology, ClusterView, Member, MemberState, MembershipProvider, StaticMembership,
};
