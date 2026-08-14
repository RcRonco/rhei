//! Gossip-based cluster discovery, backed by [chitchat].
//!
//! Nodes find each other through a Scuttlebutt-style gossip protocol rather
//! than a static `--peers` list. Each node advertises two facts about itself —
//! its Timely data-plane address and how many worker threads it can run — and
//! learns the same about everyone else. Chitchat's phi-accrual failure detector
//! removes nodes that stop heartbeating, which is what turns a crashed node
//! into a rescale event.
//!
//! Gossip carries **control-plane metadata only**. Records never travel over
//! it: the data plane stays on Timely's TCP transport, addressed by the
//! `data_addr` each node advertises here.
//!
//! ```text
//!   node A ─┐                        ┌─ ClusterView ─┐
//!   node B ─┼─ chitchat gossip ──────┤   resolve()   ├─→ ClusterTopology
//!   node C ─┘   (UDP, phi-accrual)   └───────────────┘        │
//!                                                             ▼
//!                                              Timely config + key groups
//! ```
//!
//! [chitchat]: https://github.com/quickwit-oss/chitchat

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::UdpTransport;
use chitchat::{ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, spawn_chitchat};
use rhei_core::cluster::{ClusterView, Member, MembershipProvider};
use tokio::sync::Mutex;

/// Gossip key carrying a node's Timely data-plane address.
const KEY_DATA_ADDR: &str = "rhei:data_addr";
/// Gossip key carrying a node's advertised worker-thread count.
const KEY_WORKERS: &str = "rhei:workers";

/// Configuration for gossip-based discovery.
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// Stable node identifier, unique across the cluster.
    ///
    /// Determines this node's position in the resolved topology, so it must be
    /// stable across restarts — a node that picks a fresh random ID each boot
    /// looks like a permanent join/leave churn to everyone else.
    pub node_id: String,
    /// Cluster name. Nodes only gossip with peers sharing this value, so it
    /// keeps unrelated clusters on a shared network from merging.
    pub cluster_id: String,
    /// Address to bind the gossip (UDP) socket to.
    pub gossip_addr: SocketAddr,
    /// Address peers should use to gossip with this node. Defaults to
    /// `gossip_addr` when `None` (differs behind NAT or in containers).
    pub gossip_advertise_addr: Option<SocketAddr>,
    /// Timely data-plane address (`host:port`) advertised to peers.
    pub data_addr: String,
    /// Worker threads this node offers.
    pub workers: usize,
    /// Seed nodes to contact on startup. Only needs to be reachable once;
    /// membership propagates from there.
    pub seeds: Vec<String>,
    /// Interval between gossip rounds.
    pub gossip_interval: Duration,
    /// Incarnation number, bumped on each restart so peers can tell a restarted
    /// node from a continuously running one.
    pub generation: u64,
}

impl GossipConfig {
    /// Build a config with sensible defaults for the given identity.
    pub fn new(
        node_id: impl Into<String>,
        gossip_addr: SocketAddr,
        data_addr: impl Into<String>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            cluster_id: "rhei".to_string(),
            gossip_addr,
            gossip_advertise_addr: None,
            data_addr: data_addr.into(),
            workers: 1,
            seeds: Vec::new(),
            gossip_interval: Duration::from_millis(500),
            // Wall-clock start time makes restarts monotonic without needing
            // durable storage: a restarted node always outranks its own
            // previous incarnation.
            generation: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Set the advertised worker-thread count.
    #[must_use]
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    /// Set the seed node list.
    #[must_use]
    pub fn with_seeds(mut self, seeds: Vec<String>) -> Self {
        self.seeds = seeds;
        self
    }

    /// Set the cluster name.
    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = cluster_id.into();
        self
    }

    /// Set the address peers should gossip with (for NAT / container setups).
    #[must_use]
    pub fn with_advertise_addr(mut self, addr: SocketAddr) -> Self {
        self.gossip_advertise_addr = Some(addr);
        self
    }
}

/// A [`MembershipProvider`] backed by chitchat gossip.
pub struct GossipMembership {
    node_id: String,
    handle: ChitchatHandle,
    /// Cloned watch receiver used by `changed()`.
    ///
    /// Held behind a mutex because `MembershipProvider::changed` takes `&self`
    /// but `watch::Receiver::changed` needs `&mut self`. Only the single
    /// supervisor task awaits this, so there is no contention.
    watcher: Mutex<
        tokio::sync::watch::Receiver<std::collections::BTreeMap<ChitchatId, chitchat::NodeState>>,
    >,
}

impl std::fmt::Debug for GossipMembership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipMembership")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl GossipMembership {
    /// Join the gossip cluster and begin advertising this node.
    ///
    /// # Errors
    /// Returns an error if the gossip socket cannot be bound or chitchat fails
    /// to start.
    pub async fn join(config: GossipConfig) -> anyhow::Result<Arc<Self>> {
        anyhow::ensure!(!config.node_id.is_empty(), "node_id must not be empty");
        anyhow::ensure!(config.workers > 0, "workers must be at least 1");

        let advertise_addr = config.gossip_advertise_addr.unwrap_or(config.gossip_addr);
        let chitchat_id =
            ChitchatId::new(config.node_id.clone(), config.generation, advertise_addr);

        let chitchat_config = ChitchatConfig {
            chitchat_id,
            cluster_id: config.cluster_id.clone(),
            gossip_interval: config.gossip_interval,
            listen_addr: config.gossip_addr,
            seed_nodes: config.seeds.clone(),
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(3600),
            catchup_callback: None,
            extra_liveness_predicate: None,
            // V1 compresses digests. Safe as a fixed choice because every node
            // speaking rhei gossip runs this same code — chitchat's warning about
            // staying on V0 applies to clusters mixing chitchat versions.
            protocol_version: chitchat::ProtocolVersion::V1,
        };

        // Advertise the facts peers need to build a topology. A node that has
        // not yet published both is skipped by `view_from_nodes` rather than
        // being given workers with an unknown address.
        let initial_kvs = vec![
            (KEY_DATA_ADDR.to_string(), config.data_addr.clone()),
            (KEY_WORKERS.to_string(), config.workers.to_string()),
        ];

        let handle = spawn_chitchat(chitchat_config, initial_kvs, &UdpTransport)
            .await
            .map_err(|e| {
                anyhow::anyhow!("failed to start gossip on {}: {e}", config.gossip_addr)
            })?;

        let watcher = handle.with_chitchat(|c| c.live_nodes_watcher()).await;

        tracing::info!(
            node_id = %config.node_id,
            cluster_id = %config.cluster_id,
            gossip_addr = %config.gossip_addr,
            data_addr = %config.data_addr,
            workers = config.workers,
            seeds = ?config.seeds,
            generation = config.generation,
            "joined gossip cluster"
        );

        Ok(Arc::new(Self {
            node_id: config.node_id,
            handle,
            watcher: Mutex::new(watcher),
        }))
    }

    /// Build a [`ClusterView`] from a chitchat live-node map.
    ///
    /// Nodes that have not yet gossiped a usable `data_addr`/`workers` pair are
    /// omitted: including one would put an unreachable address into the Timely
    /// peer list and hang every process trying to connect to it.
    fn view_from_nodes(
        nodes: &std::collections::BTreeMap<ChitchatId, chitchat::NodeState>,
    ) -> ClusterView {
        let members = nodes
            .iter()
            .filter_map(|(id, state)| {
                let data_addr = state.get(KEY_DATA_ADDR)?;
                let workers: usize = state.get(KEY_WORKERS)?.parse().ok()?;
                if data_addr.is_empty() || workers == 0 {
                    return None;
                }
                Some(
                    Member::new(id.node_id.to_string(), data_addr, workers)
                        .with_generation(id.generation_id),
                )
            })
            .collect();
        ClusterView::new(members)
    }

    /// Current live-node snapshot as a [`ClusterView`].
    async fn snapshot(&self) -> ClusterView {
        let nodes = self.watcher.lock().await.borrow().clone();
        Self::view_from_nodes(&nodes)
    }
}

#[async_trait::async_trait]
impl MembershipProvider for GossipMembership {
    async fn current_view(&self) -> anyhow::Result<ClusterView> {
        Ok(self.snapshot().await)
    }

    async fn changed(&self) -> Option<ClusterView> {
        let mut watcher = self.watcher.lock().await;
        // `changed()` errors only when the sender is dropped, i.e. chitchat has
        // shut down — report end-of-stream rather than spinning.
        watcher.changed().await.ok()?;
        let nodes = watcher.borrow_and_update().clone();
        Some(Self::view_from_nodes(&nodes))
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    async fn leave(&self) -> anyhow::Result<()> {
        // A graceful leave lets peers rescale immediately instead of waiting
        // out the failure detector.
        tracing::info!(node_id = %self.node_id, "leaving gossip cluster");
        self.handle
            .initiate_shutdown()
            .map_err(|e| anyhow::anyhow!("failed to leave gossip cluster: {e}"))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rhei_core::cluster::MemberState;
    use std::collections::BTreeMap;

    /// Build a chitchat `NodeState` carrying the given advertised key-values.
    ///
    /// `NodeState` has no public constructor, so this goes through a real
    /// (never-gossiping) chitchat instance to produce one.
    async fn node_state_with(kvs: &[(&str, &str)]) -> chitchat::NodeState {
        let config = ChitchatConfig {
            chitchat_id: ChitchatId::new("probe".to_string(), 0, "127.0.0.1:0".parse().unwrap()),
            cluster_id: "test".to_string(),
            gossip_interval: Duration::from_secs(3600),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            seed_nodes: Vec::new(),
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(3600),
            catchup_callback: None,
            extra_liveness_predicate: None,
            protocol_version: chitchat::ProtocolVersion::V1,
        };
        let owned: Vec<(String, String)> = kvs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let handle = spawn_chitchat(config, owned, &UdpTransport).await.unwrap();
        let state = handle.with_chitchat(|c| c.self_node_state().clone()).await;
        handle.abort();
        state
    }

    fn id(node_id: &str, generation: u64) -> ChitchatId {
        ChitchatId::new(
            node_id.to_string(),
            generation,
            "127.0.0.1:1".parse().unwrap(),
        )
    }

    #[tokio::test]
    async fn view_includes_fully_advertised_nodes() {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            id("node-a", 7),
            node_state_with(&[(KEY_DATA_ADDR, "ha:2101"), (KEY_WORKERS, "4")]).await,
        );

        let view = GossipMembership::view_from_nodes(&nodes);
        assert_eq!(view.members.len(), 1);
        let m = &view.members[0];
        assert_eq!(m.node_id, "node-a");
        assert_eq!(m.data_addr, "ha:2101");
        assert_eq!(m.workers, 4);
        assert_eq!(m.generation, 7);
        assert_eq!(m.state, MemberState::Alive);
    }

    #[tokio::test]
    async fn nodes_missing_metadata_are_excluded() {
        // A node that has joined gossip but not yet published its data address
        // must not enter the topology — Timely would block forever dialing it.
        let mut nodes = BTreeMap::new();
        nodes.insert(
            id("no-addr", 0),
            node_state_with(&[(KEY_WORKERS, "4")]).await,
        );
        nodes.insert(
            id("no-workers", 0),
            node_state_with(&[(KEY_DATA_ADDR, "hb:2101")]).await,
        );
        nodes.insert(
            id("empty-addr", 0),
            node_state_with(&[(KEY_DATA_ADDR, ""), (KEY_WORKERS, "4")]).await,
        );
        nodes.insert(
            id("zero-workers", 0),
            node_state_with(&[(KEY_DATA_ADDR, "hd:2101"), (KEY_WORKERS, "0")]).await,
        );
        nodes.insert(
            id("bad-workers", 0),
            node_state_with(&[(KEY_DATA_ADDR, "he:2101"), (KEY_WORKERS, "abc")]).await,
        );
        nodes.insert(
            id("good", 0),
            node_state_with(&[(KEY_DATA_ADDR, "hf:2101"), (KEY_WORKERS, "2")]).await,
        );

        let view = GossipMembership::view_from_nodes(&nodes);
        let ids: Vec<&str> = view.members.iter().map(|m| m.node_id.as_str()).collect();
        assert_eq!(ids, vec!["good"], "only fully-advertised nodes participate");
    }

    #[tokio::test]
    async fn view_resolves_to_a_deterministic_topology() {
        let mut nodes = BTreeMap::new();
        for (name, addr) in [
            ("node-c", "hc:2101"),
            ("node-a", "ha:2101"),
            ("node-b", "hb:2101"),
        ] {
            nodes.insert(
                id(name, 0),
                node_state_with(&[(KEY_DATA_ADDR, addr), (KEY_WORKERS, "2")]).await,
            );
        }

        let topo = GossipMembership::view_from_nodes(&nodes)
            .resolve(128)
            .unwrap();
        assert_eq!(topo.addresses(), vec!["ha:2101", "hb:2101", "hc:2101"]);
        assert_eq!(topo.total_workers(), 6);
    }

    #[tokio::test]
    async fn two_nodes_discover_each_other() {
        // End-to-end over real UDP gossip: node B seeds off node A and both
        // converge on a two-member view.
        let a_addr: SocketAddr = "127.0.0.1:19311".parse().unwrap();
        let b_addr: SocketAddr = "127.0.0.1:19312".parse().unwrap();

        let a = GossipMembership::join(
            GossipConfig::new("node-a", a_addr, "127.0.0.1:2101")
                .with_workers(2)
                .with_cluster_id("discovery-test"),
        )
        .await
        .unwrap();

        let b = GossipMembership::join(
            GossipConfig::new("node-b", b_addr, "127.0.0.1:2102")
                .with_workers(2)
                .with_cluster_id("discovery-test")
                .with_seeds(vec![a_addr.to_string()]),
        )
        .await
        .unwrap();

        // Poll until convergence rather than sleeping a fixed amount.
        let converged = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let av = a.current_view().await.unwrap();
                let bv = b.current_view().await.unwrap();
                if av.participants().len() == 2 && bv.participants().len() == 2 {
                    return (av, bv);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;

        let (av, bv) = converged.expect("nodes did not discover each other within 10s");

        // Both sides must resolve to byte-identical topologies, or they would
        // disagree about which worker owns which key group.
        let ta = av.resolve(128).unwrap();
        let tb = bv.resolve(128).unwrap();
        assert_eq!(ta, tb);
        assert_eq!(ta.fingerprint(), tb.fingerprint());
        assert_eq!(ta.addresses(), vec!["127.0.0.1:2101", "127.0.0.1:2102"]);
        assert_eq!(ta.process_id_of("node-a"), Some(0));
        assert_eq!(ta.process_id_of("node-b"), Some(1));

        a.leave().await.unwrap();
        b.leave().await.unwrap();
    }

    #[tokio::test]
    async fn join_rejects_invalid_config() {
        let addr: SocketAddr = "127.0.0.1:19399".parse().unwrap();
        let mut cfg = GossipConfig::new("", addr, "127.0.0.1:2101");
        assert!(GossipMembership::join(cfg.clone()).await.is_err());

        cfg.node_id = "ok".to_string();
        cfg.workers = 0;
        assert!(GossipMembership::join(cfg).await.is_err());
    }
}
