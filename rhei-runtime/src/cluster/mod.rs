//! Dynamic cluster discovery and rescaling.
//!
//! This module is the control plane: it decides *who is in the cluster* and
//! *when the pipeline should restart on a new shape*. The vocabulary it works
//! in — key groups, cluster views, resolved topologies — lives in
//! [`rhei_core::cluster`].
//!
//! - [`gossip`] — chitchat-backed discovery and failure detection
//!   (requires the `chitchat` feature).
//! - [`rescale`] — debounced supervision that turns membership churn into a
//!   small number of deliberate topology generations.
//!
//! Static `--peers` clusters keep working unchanged: they use
//! [`StaticMembership`](rhei_core::cluster::StaticMembership), whose membership
//! never changes, so the supervisor simply never fires.

/// Gossip-based cluster discovery backed by chitchat.
#[cfg(feature = "chitchat")]
pub mod gossip;
/// Rescale supervision: debouncing membership churn into topology generations.
pub mod rescale;

#[cfg(feature = "chitchat")]
pub use gossip::{GossipConfig, GossipMembership};
pub use rescale::{RescaleDecision, RescalePolicy, RescaleSupervisor};
