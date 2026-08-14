//! Rescale supervision: turning membership changes into topology generations.
//!
//! Timely cannot add or remove workers from a running dataflow — the worker set
//! is fixed when `timely::execute` is called, and the progress protocol assumes
//! every peer stays alive. So a rescale is necessarily *stop and restart*:
//!
//! ```text
//!   membership change
//!         │
//!         ▼
//!    debounce ──── still settling? ──→ wait
//!         │
//!         ▼  (quiet for `debounce`)
//!   resolve topology ── fingerprint unchanged? ──→ ignore
//!         │
//!         ▼
//!   checkpoint ──→ stop dataflow ──→ recompute key groups ──→ restart
//! ```
//!
//! This is the same shape as Flink's stop-with-savepoint rescale. What makes it
//! cheap here is that state is addressed by key group rather than by worker
//! (see [`rhei_core::cluster::key_group`]), so the restart reassigns *ownership*
//! without moving any bytes: a worker that gains a key group simply reads it
//! from shared L3 storage on first access.
//!
//! ## Debouncing
//!
//! Membership churn is bursty — a rolling restart produces a leave and a join
//! per node, seconds apart. Rescaling on each event would restart the pipeline
//! repeatedly and make no progress. [`RescaleSupervisor`] therefore waits for
//! membership to stay *quiet* for a configured interval, and compares topology
//! fingerprints so that a change which cancels itself out costs nothing.

use std::sync::Arc;
use std::time::Duration;

use rhei_core::cluster::{ClusterTopology, MembershipProvider};

/// Why the supervisor decided to act (or not).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RescaleDecision {
    /// Topology changed materially; the pipeline should restart on it.
    Rescale {
        /// The topology to run next.
        topology: Box<ClusterTopology>,
        /// Key groups that change owner relative to the previous topology.
        moved_key_groups: usize,
    },
    /// Membership changed but the resolved topology is identical — nothing to do.
    NoChange,
    /// Membership left no viable cluster (every node gone or unusable).
    ///
    /// The caller should hold the current topology rather than tearing the
    /// pipeline down: a transient loss of every peer is more likely a network
    /// partition than a real shutdown.
    NoQuorum(String),
    /// The provider shut down and will publish no further updates.
    ProviderClosed,
}

/// Tuning for rescale sensitivity.
#[derive(Debug, Clone)]
pub struct RescalePolicy {
    /// How long membership must stay unchanged before a rescale is triggered.
    ///
    /// Guards against thrashing during rolling restarts and brief network
    /// blips. Too low and a rolling deploy restarts the pipeline once per node;
    /// too high and genuine capacity changes take a long time to take effect.
    pub debounce: Duration,
    /// Number of key groups. Fixed for the pipeline's lifetime.
    pub max_parallelism: usize,
    /// Whether membership changes trigger a rescale automatically.
    ///
    /// When `false` the supervisor still tracks topology and reports staleness,
    /// but never restarts the pipeline on its own.
    pub automatic: bool,
}

impl RescalePolicy {
    /// Policy with the given key group count and default sensitivity.
    #[must_use]
    pub fn new(max_parallelism: usize) -> Self {
        Self {
            debounce: Duration::from_secs(5),
            max_parallelism,
            automatic: true,
        }
    }

    /// Set the debounce interval.
    #[must_use]
    pub fn with_debounce(mut self, debounce: Duration) -> Self {
        self.debounce = debounce;
        self
    }

    /// Enable or disable automatic rescaling.
    #[must_use]
    pub fn with_automatic(mut self, automatic: bool) -> Self {
        self.automatic = automatic;
        self
    }
}

/// Watches a [`MembershipProvider`] and decides when to rescale.
///
/// The supervisor owns no pipeline machinery: it reports *decisions*, and the
/// controller performs the checkpoint/restart. Keeping the policy separate from
/// the mechanism is what makes the debounce and fingerprint logic testable
/// without standing up a cluster.
pub struct RescaleSupervisor {
    provider: Arc<dyn MembershipProvider>,
    policy: RescalePolicy,
    /// Fingerprint of the topology the pipeline is currently running.
    current: Option<ClusterTopology>,
}

impl std::fmt::Debug for RescaleSupervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RescaleSupervisor")
            .field("node_id", &self.provider.node_id())
            .field("policy", &self.policy)
            .field(
                "current",
                &self.current.as_ref().map(ClusterTopology::summary),
            )
            .finish()
    }
}

impl RescaleSupervisor {
    /// Create a supervisor over `provider`.
    #[must_use]
    pub fn new(provider: Arc<dyn MembershipProvider>, policy: RescalePolicy) -> Self {
        Self {
            provider,
            policy,
            current: None,
        }
    }

    /// The topology the pipeline is currently running, if any.
    #[must_use]
    pub fn current(&self) -> Option<&ClusterTopology> {
        self.current.as_ref()
    }

    /// This node's identifier.
    #[must_use]
    pub fn node_id(&self) -> &str {
        self.provider.node_id()
    }

    /// Resolve the initial topology and adopt it as current.
    ///
    /// # Errors
    /// Returns an error if the current membership does not resolve to a viable
    /// cluster — at startup there is no previous topology to fall back on, so
    /// this is fatal rather than a hold.
    pub async fn initial_topology(&mut self) -> anyhow::Result<ClusterTopology> {
        let view = self.provider.current_view().await?;
        let topology = view.resolve(self.policy.max_parallelism)?;
        tracing::info!(
            node_id = %self.provider.node_id(),
            topology = %topology.summary(),
            fingerprint = topology.fingerprint(),
            "initial cluster topology"
        );
        self.current = Some(topology.clone());
        Ok(topology)
    }

    /// Adopt `topology` as the one currently running.
    ///
    /// Called by the controller once a restart has actually taken effect, so
    /// that a failed restart does not make the supervisor believe the new
    /// topology is live.
    pub fn adopt(&mut self, topology: ClusterTopology) {
        self.current = Some(topology);
    }

    /// Await the next rescale decision.
    ///
    /// Blocks until membership changes, then debounces: each further change
    /// within the debounce window restarts the timer, so a burst of churn
    /// produces a single decision once things settle.
    pub async fn next_decision(&mut self) -> RescaleDecision {
        // Wait for the first change. Without this, a quiet cluster would spin.
        let Some(mut view) = self.provider.changed().await else {
            return RescaleDecision::ProviderClosed;
        };

        // Absorb further changes until membership stays quiet for `debounce`.
        loop {
            match tokio::time::timeout(self.policy.debounce, self.provider.changed()).await {
                // Another change arrived inside the window — keep waiting.
                Ok(Some(newer)) => {
                    tracing::debug!(
                        node_id = %self.provider.node_id(),
                        "membership still settling; extending debounce"
                    );
                    view = newer;
                }
                Ok(None) => return RescaleDecision::ProviderClosed,
                // Quiet for a full window: settled.
                Err(_) => break,
            }
        }

        let topology = match view.resolve(self.policy.max_parallelism) {
            Ok(t) => t,
            Err(e) => return RescaleDecision::NoQuorum(e.to_string()),
        };

        // A change that resolves to the same execution shape (e.g. a node
        // updating unrelated metadata, or a leave immediately undone by a
        // rejoin) must not cost a pipeline restart.
        if let Some(ref current) = self.current
            && current.fingerprint() == topology.fingerprint()
        {
            tracing::debug!(
                node_id = %self.provider.node_id(),
                "membership changed but topology fingerprint is unchanged; no rescale"
            );
            return RescaleDecision::NoChange;
        }

        let moved_key_groups = self.moved_key_groups(&topology);

        tracing::info!(
            node_id = %self.provider.node_id(),
            from = self.current.as_ref().map_or_else(|| "none".to_string(), ClusterTopology::summary),
            to = %topology.summary(),
            moved_key_groups,
            automatic = self.policy.automatic,
            "cluster topology change detected"
        );
        metrics::counter!("rhei_cluster_topology_changes_total").increment(1);
        metrics::gauge!("rhei_cluster_processes").set(topology.n_processes() as f64);
        metrics::gauge!("rhei_cluster_workers").set(topology.total_workers() as f64);

        if !self.policy.automatic {
            tracing::warn!(
                "automatic rescaling is disabled; pipeline continues on the previous topology"
            );
            return RescaleDecision::NoChange;
        }

        RescaleDecision::Rescale {
            topology: Box::new(topology),
            moved_key_groups,
        }
    }

    /// How many key groups change owner moving to `next`.
    fn moved_key_groups(&self, next: &ClusterTopology) -> usize {
        let Some(ref current) = self.current else {
            return 0;
        };
        match (current.assignment(), next.assignment()) {
            (Ok(prev), Ok(new)) => new.moved_key_groups(&prev),
            _ => 0,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rhei_core::cluster::{ClusterView, Member};
    use std::sync::Mutex;
    use tokio::sync::Notify;

    /// A membership provider whose view can be driven from tests.
    #[derive(Debug)]
    struct FakeMembership {
        node_id: String,
        view: Mutex<ClusterView>,
        notify: Notify,
        closed: Mutex<bool>,
    }

    impl FakeMembership {
        fn new(members: &[(&str, usize)]) -> Arc<Self> {
            Arc::new(Self {
                node_id: "node-a".to_string(),
                view: Mutex::new(Self::view_of(members)),
                notify: Notify::new(),
                closed: Mutex::new(false),
            })
        }

        fn view_of(members: &[(&str, usize)]) -> ClusterView {
            ClusterView::new(
                members
                    .iter()
                    .map(|(id, w)| Member::new(*id, format!("{id}:2101"), *w))
                    .collect(),
            )
        }

        /// Publish a new membership snapshot.
        fn set(&self, members: &[(&str, usize)]) {
            *self.view.lock().unwrap() = Self::view_of(members);
            self.notify.notify_waiters();
        }

        /// Signal end-of-stream.
        fn close(&self) {
            *self.closed.lock().unwrap() = true;
            self.notify.notify_waiters();
        }
    }

    #[async_trait::async_trait]
    impl MembershipProvider for FakeMembership {
        async fn current_view(&self) -> anyhow::Result<ClusterView> {
            Ok(self.view.lock().unwrap().clone())
        }

        async fn changed(&self) -> Option<ClusterView> {
            self.notify.notified().await;
            if *self.closed.lock().unwrap() {
                return None;
            }
            Some(self.view.lock().unwrap().clone())
        }

        fn node_id(&self) -> &str {
            &self.node_id
        }
    }

    fn policy() -> RescalePolicy {
        RescalePolicy::new(128).with_debounce(Duration::from_millis(80))
    }

    #[tokio::test]
    async fn initial_topology_is_adopted() {
        let provider = FakeMembership::new(&[("node-a", 2), ("node-b", 2)]);
        let mut sup = RescaleSupervisor::new(provider, policy());

        let topo = sup.initial_topology().await.unwrap();
        assert_eq!(topo.total_workers(), 4);
        assert_eq!(sup.current().unwrap().fingerprint(), topo.fingerprint());
    }

    #[tokio::test]
    async fn node_join_triggers_a_rescale() {
        let provider = FakeMembership::new(&[("node-a", 2), ("node-b", 2)]);
        let mut sup = RescaleSupervisor::new(provider.clone(), policy());
        sup.initial_topology().await.unwrap();

        let p = provider.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            p.set(&[("node-a", 2), ("node-b", 2), ("node-c", 2)]);
        });

        let decision = tokio::time::timeout(Duration::from_secs(5), sup.next_decision())
            .await
            .expect("supervisor did not decide in time");

        match decision {
            RescaleDecision::Rescale {
                topology,
                moved_key_groups,
            } => {
                assert_eq!(topology.total_workers(), 6);
                assert!(
                    moved_key_groups > 0,
                    "scaling 4 -> 6 workers must move some key groups"
                );
                assert!(
                    moved_key_groups < 128,
                    "contiguous ranges should leave most key groups in place, moved={moved_key_groups}"
                );
            }
            other => panic!("expected Rescale, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn node_failure_triggers_a_rescale() {
        let provider = FakeMembership::new(&[("node-a", 2), ("node-b", 2), ("node-c", 2)]);
        let mut sup = RescaleSupervisor::new(provider.clone(), policy());
        sup.initial_topology().await.unwrap();

        let p = provider.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            p.set(&[("node-a", 2), ("node-b", 2)]);
        });

        let decision = tokio::time::timeout(Duration::from_secs(5), sup.next_decision())
            .await
            .unwrap();

        match decision {
            RescaleDecision::Rescale { topology, .. } => {
                assert_eq!(topology.total_workers(), 4);
                assert_eq!(topology.addresses(), vec!["node-a:2101", "node-b:2101"]);
            }
            other => panic!("expected Rescale, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn churn_is_debounced_into_one_decision() {
        // A rolling restart emits many events in quick succession. The
        // supervisor must ride them out and act once, on the settled state.
        let provider = FakeMembership::new(&[("node-a", 2)]);
        let mut sup = RescaleSupervisor::new(provider.clone(), policy());
        sup.initial_topology().await.unwrap();

        let p = provider.clone();
        tokio::spawn(async move {
            for members in [
                vec![("node-a", 2), ("node-b", 2)],
                vec![("node-a", 2)],
                vec![("node-a", 2), ("node-b", 2), ("node-c", 2)],
                vec![("node-a", 2), ("node-c", 2)],
            ] {
                tokio::time::sleep(Duration::from_millis(15)).await;
                p.set(&members);
            }
        });

        let decision = tokio::time::timeout(Duration::from_secs(5), sup.next_decision())
            .await
            .unwrap();

        // Only the settled membership matters, not the intermediate states.
        match decision {
            RescaleDecision::Rescale { topology, .. } => {
                assert_eq!(topology.addresses(), vec!["node-a:2101", "node-c:2101"]);
            }
            other => panic!("expected a single settled Rescale, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn membership_change_that_cancels_out_is_not_a_rescale() {
        // Node leaves and rejoins within the debounce window: the resolved
        // topology is identical, so restarting the pipeline would be pure cost.
        let provider = FakeMembership::new(&[("node-a", 2), ("node-b", 2)]);
        let mut sup = RescaleSupervisor::new(provider.clone(), policy());
        sup.initial_topology().await.unwrap();

        let p = provider.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            p.set(&[("node-a", 2)]);
            tokio::time::sleep(Duration::from_millis(15)).await;
            p.set(&[("node-a", 2), ("node-b", 2)]);
        });

        let decision = tokio::time::timeout(Duration::from_secs(5), sup.next_decision())
            .await
            .unwrap();
        assert_eq!(decision, RescaleDecision::NoChange);
    }

    #[tokio::test]
    async fn losing_every_node_reports_no_quorum_rather_than_rescaling() {
        let provider = FakeMembership::new(&[("node-a", 2)]);
        let mut sup = RescaleSupervisor::new(provider.clone(), policy());
        sup.initial_topology().await.unwrap();

        let p = provider.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            p.set(&[]);
        });

        let decision = tokio::time::timeout(Duration::from_secs(5), sup.next_decision())
            .await
            .unwrap();
        assert!(
            matches!(decision, RescaleDecision::NoQuorum(_)),
            "expected NoQuorum, got {decision:?}"
        );
        // The previously-running topology is retained, not discarded.
        assert_eq!(sup.current().unwrap().total_workers(), 2);
    }

    #[tokio::test]
    async fn manual_mode_never_rescales() {
        let provider = FakeMembership::new(&[("node-a", 2)]);
        let mut sup = RescaleSupervisor::new(provider.clone(), policy().with_automatic(false));
        sup.initial_topology().await.unwrap();

        let p = provider.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            p.set(&[("node-a", 2), ("node-b", 2)]);
        });

        let decision = tokio::time::timeout(Duration::from_secs(5), sup.next_decision())
            .await
            .unwrap();
        assert_eq!(decision, RescaleDecision::NoChange);
    }

    #[tokio::test]
    async fn provider_shutdown_ends_the_loop() {
        let provider = FakeMembership::new(&[("node-a", 2)]);
        let mut sup = RescaleSupervisor::new(provider.clone(), policy());
        sup.initial_topology().await.unwrap();

        let p = provider.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            p.close();
        });

        let decision = tokio::time::timeout(Duration::from_secs(5), sup.next_decision())
            .await
            .unwrap();
        assert_eq!(decision, RescaleDecision::ProviderClosed);
    }

    #[tokio::test]
    async fn adopt_updates_the_baseline_for_future_decisions() {
        let provider = FakeMembership::new(&[("node-a", 2)]);
        let mut sup = RescaleSupervisor::new(provider.clone(), policy());
        sup.initial_topology().await.unwrap();

        let two = ClusterView::new(vec![
            Member::new("node-a", "node-a:2101", 2),
            Member::new("node-b", "node-b:2101", 2),
        ])
        .resolve(128)
        .unwrap();
        sup.adopt(two.clone());
        assert_eq!(sup.current().unwrap().fingerprint(), two.fingerprint());

        // Re-publishing the adopted membership is now a no-op.
        let p = provider.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            p.set(&[("node-a", 2), ("node-b", 2)]);
        });
        let decision = tokio::time::timeout(Duration::from_secs(5), sup.next_decision())
            .await
            .unwrap();
        assert_eq!(decision, RescaleDecision::NoChange);
    }
}
