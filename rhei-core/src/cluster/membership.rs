//! Cluster membership: who is in the cluster, and the topology derived from it.
//!
//! Membership is discovered at runtime rather than fixed by a `--peers` flag.
//! A [`MembershipProvider`] publishes a stream of [`ClusterView`]s; each view
//! is resolved into a [`ClusterTopology`] — the concrete, ordered process list
//! that Timely and the key-group assignment are built from.
//!
//! **Determinism is the core requirement.** Every node resolves the topology
//! independently from its own copy of the view. If two nodes disagree about
//! the process ordering, they disagree about which worker owns which key
//! group, and rows get routed to workers that do not hold the matching state.
//! [`ClusterView::resolve`] is therefore a pure function of the view's
//! contents: members are sorted by node ID, and only members in a
//! participating state are included.

use serde::{Deserialize, Serialize};

use super::key_group::{KeyGroupAssignment, validate_max_parallelism};

/// Liveness of a cluster member, as understood by the local node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MemberState {
    /// Node has been seen recently and is eligible to run workers.
    Alive,
    /// Node has missed heartbeats but has not yet been declared dead.
    ///
    /// Suspect nodes are excluded from the topology: keeping them would stall
    /// the dataflow waiting for a process that may never connect. If the node
    /// recovers it rejoins as `Alive` and triggers another rescale.
    Suspect,
    /// Node is confirmed gone (graceful leave or failure detector timeout).
    Dead,
}

impl MemberState {
    /// Whether a member in this state should be given workers.
    #[must_use]
    pub const fn participates(self) -> bool {
        matches!(self, Self::Alive)
    }
}

/// One node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    /// Stable, unique node identifier. Determines topology ordering, so it
    /// must be identical across every node's view of this member.
    pub node_id: String,
    /// Data-plane address (`host:port`) used for Timely's TCP transport.
    pub data_addr: String,
    /// Number of Timely worker threads this node is prepared to run.
    pub workers: usize,
    /// Liveness as understood by the local node.
    pub state: MemberState,
    /// Incarnation number, bumped each time the node restarts.
    ///
    /// Distinguishes "same node, still running" from "same node, restarted and
    /// lost its in-memory state" — a restart must force a rescale even though
    /// the member list looks unchanged.
    pub generation: u64,
}

impl Member {
    /// Construct an alive member at generation 0.
    pub fn new(node_id: impl Into<String>, data_addr: impl Into<String>, workers: usize) -> Self {
        Self {
            node_id: node_id.into(),
            data_addr: data_addr.into(),
            workers,
            state: MemberState::Alive,
            generation: 0,
        }
    }

    /// Set the incarnation number.
    #[must_use]
    pub fn with_generation(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }

    /// Set the liveness state.
    #[must_use]
    pub fn with_state(mut self, state: MemberState) -> Self {
        self.state = state;
        self
    }
}

/// A snapshot of cluster membership as observed by the local node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterView {
    /// All known members, in arbitrary order.
    pub members: Vec<Member>,
}

impl ClusterView {
    /// Build a view from a member list.
    #[must_use]
    pub fn new(members: Vec<Member>) -> Self {
        Self { members }
    }

    /// Members eligible to run workers, sorted by node ID.
    #[must_use]
    pub fn participants(&self) -> Vec<Member> {
        let mut alive: Vec<Member> = self
            .members
            .iter()
            .filter(|m| m.state.participates() && m.workers > 0)
            .cloned()
            .collect();
        alive.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        alive
    }

    /// Resolve this view into a concrete topology.
    ///
    /// Process IDs are assigned by sorted node ID, so all nodes that observe
    /// the same participating set produce byte-identical topologies.
    ///
    /// # Errors
    /// Returns an error if no member is eligible to run workers, or if
    /// `max_parallelism` is out of range.
    pub fn resolve(&self, max_parallelism: usize) -> anyhow::Result<ClusterTopology> {
        validate_max_parallelism(max_parallelism)?;
        let participants = self.participants();
        anyhow::ensure!(
            !participants.is_empty(),
            "cannot resolve topology: no alive members with workers > 0"
        );

        // Timely's Cluster config takes a single `threads` value shared by every
        // process, so a heterogeneous cluster has to converge on one number.
        // The minimum is the only safe choice: a node cannot run more worker
        // threads than it advertised.
        let workers_per_process = participants
            .iter()
            .map(|m| m.workers)
            .min()
            .unwrap_or(1)
            .max(1);

        if participants
            .iter()
            .any(|m| m.workers != workers_per_process)
        {
            tracing::warn!(
                workers_per_process,
                advertised = ?participants.iter().map(|m| (&m.node_id, m.workers)).collect::<Vec<_>>(),
                "heterogeneous worker counts; clamping every process to the minimum"
            );
        }

        Ok(ClusterTopology {
            processes: participants,
            workers_per_process,
            max_parallelism,
        })
    }
}

/// A resolved, ordered cluster topology ready to execute.
///
/// This is the bridge between discovery and execution: it yields both the
/// Timely cluster configuration (process index + peer addresses) and the
/// key-group assignment that routes data and addresses state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterTopology {
    /// Participating processes, indexed by process ID (sorted by node ID).
    pub processes: Vec<Member>,
    /// Worker threads per process. Uniform across the cluster.
    pub workers_per_process: usize,
    /// Total key groups. Fixed for the pipeline's lifetime.
    pub max_parallelism: usize,
}

impl ClusterTopology {
    /// A single-process topology, for local (non-clustered) execution.
    ///
    /// # Errors
    /// Returns an error if `max_parallelism` is out of range.
    pub fn single_process(workers: usize, max_parallelism: usize) -> anyhow::Result<Self> {
        validate_max_parallelism(max_parallelism)?;
        anyhow::ensure!(workers > 0, "workers must be at least 1");
        Ok(Self {
            processes: vec![Member::new("local", "127.0.0.1:0", workers)],
            workers_per_process: workers,
            max_parallelism,
        })
    }

    /// Number of participating processes.
    #[must_use]
    pub fn n_processes(&self) -> usize {
        self.processes.len()
    }

    /// Total worker count across the cluster: the key-group parallelism.
    #[must_use]
    pub fn total_workers(&self) -> usize {
        self.processes.len() * self.workers_per_process
    }

    /// Timely peer addresses, ordered by process ID.
    #[must_use]
    pub fn addresses(&self) -> Vec<String> {
        self.processes.iter().map(|m| m.data_addr.clone()).collect()
    }

    /// Process ID for `node_id`, or `None` if it is not participating.
    #[must_use]
    pub fn process_id_of(&self, node_id: &str) -> Option<usize> {
        self.processes.iter().position(|m| m.node_id == node_id)
    }

    /// Global worker indices owned by the process at `process_id`.
    #[must_use]
    pub fn local_worker_range(&self, process_id: usize) -> std::ops::Range<usize> {
        let start = process_id * self.workers_per_process;
        start..start + self.workers_per_process
    }

    /// The key-group assignment implied by this topology.
    ///
    /// # Errors
    /// Returns an error if the topology has no workers.
    pub fn assignment(&self) -> anyhow::Result<KeyGroupAssignment> {
        KeyGroupAssignment::new(self.max_parallelism, self.total_workers())
    }

    /// A fingerprint of the parts of the topology that affect execution.
    ///
    /// Two topologies with the same fingerprint produce the same Timely
    /// configuration and the same key-group assignment, so a membership event
    /// that leaves the fingerprint unchanged needs no rescale. Node
    /// `generation` is included so that a node restarting in place — same ID,
    /// same address — still forces a restart from checkpoint.
    #[must_use]
    pub fn fingerprint(&self) -> u64 {
        let mut buf = String::new();
        for m in &self.processes {
            buf.push_str(&m.node_id);
            buf.push('@');
            buf.push_str(&m.data_addr);
            buf.push('#');
            buf.push_str(&m.generation.to_string());
            buf.push(';');
        }
        buf.push_str(&self.workers_per_process.to_string());
        buf.push('/');
        buf.push_str(&self.max_parallelism.to_string());
        seahash::hash(buf.as_bytes())
    }

    /// Human-readable one-line summary for logs.
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{} process(es) x {} worker(s) = {} workers, {} key groups",
            self.n_processes(),
            self.workers_per_process,
            self.total_workers(),
            self.max_parallelism
        )
    }
}

/// A source of cluster membership updates.
///
/// Implementations range from a fixed list (`StaticMembership`) to gossip-based
/// discovery. The runtime treats them uniformly: get the current view, and
/// await changes.
#[async_trait::async_trait]
pub trait MembershipProvider: Send + Sync + std::fmt::Debug {
    /// The current membership snapshot.
    async fn current_view(&self) -> anyhow::Result<ClusterView>;

    /// Wait until membership changes, then return the new view.
    ///
    /// Returns `None` when the provider has shut down and will publish no
    /// further updates. Implementations should only return on a *material*
    /// change; spurious wakeups cost a pipeline restart.
    async fn changed(&self) -> Option<ClusterView>;

    /// This node's own stable identifier.
    fn node_id(&self) -> &str;

    /// Announce a graceful departure so peers rescale without waiting for the
    /// failure detector to time out.
    async fn leave(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// A fixed membership list that never changes.
///
/// Used for `--peers`-style static clusters and as a test double. `changed()`
/// never returns, so the rescale supervisor simply parks on it.
#[derive(Debug, Clone)]
pub struct StaticMembership {
    node_id: String,
    view: ClusterView,
}

impl StaticMembership {
    /// Build a static provider for `node_id` over a fixed member list.
    pub fn new(node_id: impl Into<String>, members: Vec<Member>) -> Self {
        Self {
            node_id: node_id.into(),
            view: ClusterView::new(members),
        }
    }

    /// Build a static provider from a `--peers`-style address list.
    ///
    /// Node IDs are derived from process position so the resolved ordering
    /// matches the order the addresses were supplied in.
    pub fn from_peers(process_id: usize, peers: &[String], workers: usize) -> Self {
        let members = peers
            .iter()
            .enumerate()
            .map(|(i, addr)| Member::new(format!("p{i:05}"), addr.clone(), workers))
            .collect();
        Self::new(
            format!("p{process_id:05}"),
            ClusterView::new(members).members,
        )
    }
}

#[async_trait::async_trait]
impl MembershipProvider for StaticMembership {
    async fn current_view(&self) -> anyhow::Result<ClusterView> {
        Ok(self.view.clone())
    }

    async fn changed(&self) -> Option<ClusterView> {
        // Static membership never changes. Park forever rather than returning,
        // so the supervisor's select! simply never fires this branch.
        std::future::pending().await
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn view(specs: &[(&str, &str, usize)]) -> ClusterView {
        ClusterView::new(
            specs
                .iter()
                .map(|(id, addr, w)| Member::new(*id, *addr, *w))
                .collect(),
        )
    }

    #[test]
    fn resolve_orders_processes_by_node_id() {
        // Insertion order must not affect the resolved topology.
        let a = view(&[
            ("node-c", "hc:2101", 2),
            ("node-a", "ha:2101", 2),
            ("node-b", "hb:2101", 2),
        ]);
        let b = view(&[
            ("node-a", "ha:2101", 2),
            ("node-b", "hb:2101", 2),
            ("node-c", "hc:2101", 2),
        ]);

        let ta = a.resolve(128).unwrap();
        let tb = b.resolve(128).unwrap();
        assert_eq!(ta, tb, "topology must not depend on member insertion order");
        assert_eq!(ta.addresses(), vec!["ha:2101", "hb:2101", "hc:2101"]);
        assert_eq!(ta.fingerprint(), tb.fingerprint());
    }

    #[test]
    fn resolve_excludes_non_alive_members() {
        let v = ClusterView::new(vec![
            Member::new("node-a", "ha:2101", 2),
            Member::new("node-b", "hb:2101", 2).with_state(MemberState::Suspect),
            Member::new("node-c", "hc:2101", 2).with_state(MemberState::Dead),
        ]);
        let topo = v.resolve(128).unwrap();
        assert_eq!(topo.n_processes(), 1);
        assert_eq!(topo.addresses(), vec!["ha:2101"]);
    }

    #[test]
    fn resolve_excludes_zero_worker_members() {
        let v = view(&[("node-a", "ha:2101", 2), ("node-b", "hb:2101", 0)]);
        let topo = v.resolve(128).unwrap();
        assert_eq!(topo.n_processes(), 1);
    }

    #[test]
    fn resolve_fails_with_no_participants() {
        let v = ClusterView::new(vec![
            Member::new("node-a", "ha:2101", 2).with_state(MemberState::Dead),
        ]);
        let err = v.resolve(128).unwrap_err();
        assert!(
            err.to_string().contains("no alive members"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn heterogeneous_workers_clamp_to_minimum() {
        // Timely takes one `threads` value for the whole cluster, so a node
        // cannot be asked to run more threads than it advertised.
        let v = view(&[
            ("node-a", "ha:2101", 8),
            ("node-b", "hb:2101", 2),
            ("node-c", "hc:2101", 4),
        ]);
        let topo = v.resolve(128).unwrap();
        assert_eq!(topo.workers_per_process, 2);
        assert_eq!(topo.total_workers(), 6);
    }

    #[test]
    fn topology_worker_ranges_partition_the_workers() {
        let topo = view(&[
            ("node-a", "ha:2101", 3),
            ("node-b", "hb:2101", 3),
            ("node-c", "hc:2101", 3),
        ])
        .resolve(128)
        .unwrap();

        assert_eq!(topo.total_workers(), 9);
        assert_eq!(topo.local_worker_range(0), 0..3);
        assert_eq!(topo.local_worker_range(1), 3..6);
        assert_eq!(topo.local_worker_range(2), 6..9);
    }

    #[test]
    fn fingerprint_changes_when_a_node_joins() {
        let before = view(&[("node-a", "ha:2101", 2)]).resolve(128).unwrap();
        let after = view(&[("node-a", "ha:2101", 2), ("node-b", "hb:2101", 2)])
            .resolve(128)
            .unwrap();
        assert_ne!(before.fingerprint(), after.fingerprint());
    }

    #[test]
    fn fingerprint_changes_when_a_node_restarts_in_place() {
        // Same ID, same address, new incarnation: the node lost its in-memory
        // state, so this must force a rescale even though membership looks equal.
        let before = ClusterView::new(vec![Member::new("node-a", "ha:2101", 2)])
            .resolve(128)
            .unwrap();
        let after = ClusterView::new(vec![Member::new("node-a", "ha:2101", 2).with_generation(1)])
            .resolve(128)
            .unwrap();
        assert_ne!(before.fingerprint(), after.fingerprint());
    }

    #[test]
    fn fingerprint_is_stable_for_equivalent_views() {
        let a = view(&[("node-a", "ha:2101", 2), ("node-b", "hb:2101", 2)]);
        let b = view(&[("node-b", "hb:2101", 2), ("node-a", "ha:2101", 2)]);
        assert_eq!(
            a.resolve(128).unwrap().fingerprint(),
            b.resolve(128).unwrap().fingerprint()
        );
    }

    #[test]
    fn topology_assignment_matches_total_workers() {
        let topo = view(&[("node-a", "ha:2101", 2), ("node-b", "hb:2101", 2)])
            .resolve(128)
            .unwrap();
        let assignment = topo.assignment().unwrap();
        assert_eq!(assignment.parallelism, 4);
        assert_eq!(assignment.max_parallelism, 128);
    }

    #[test]
    fn single_process_topology() {
        let topo = ClusterTopology::single_process(4, 128).unwrap();
        assert_eq!(topo.n_processes(), 1);
        assert_eq!(topo.total_workers(), 4);
        assert_eq!(topo.local_worker_range(0), 0..4);
    }

    #[tokio::test]
    async fn static_membership_reports_its_view() {
        let provider =
            StaticMembership::from_peers(1, &["h0:2101".to_string(), "h1:2101".to_string()], 2);
        assert_eq!(provider.node_id(), "p00001");
        let topo = provider.current_view().await.unwrap().resolve(128).unwrap();
        assert_eq!(topo.n_processes(), 2);
        assert_eq!(topo.process_id_of("p00001"), Some(1));
        assert_eq!(topo.addresses(), vec!["h0:2101", "h1:2101"]);
    }
}
