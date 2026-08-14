//! Physical key layout for operator state.
//!
//! [`PrefixedBackend`](super::prefixed_backend::PrefixedBackend) namespaces
//! keys by operator, and the runtime historically folded the worker index and
//! process ID into that prefix (`p{pid}/w{idx}/{op}`). That makes durable state
//! a function of the cluster layout: change the worker count and every key
//! lands under a prefix nobody looks up any more.
//!
//! State is addressed by *key group* instead:
//!
//! ```text
//! kg{group:05}/{operator}/{state_key}
//! ```
//!
//! The physical key is identical no matter which worker or process performs the
//! access. After a rescale, the worker that inherits key group 37 issues exactly
//! the reads the previous owner issued, and shared L3 storage serves them. No
//! state migration, no rewrite — just cache warming on the gaining worker.
//!
//! The zero-padded group ID keeps the lexicographic order of physical keys
//! aligned with numeric group order, so an owned range is a contiguous scan in
//! ordered stores like `SlateDB`. Putting the group *before* the operator makes
//! a whole key group contiguous across every operator, which is what warming a
//! gained range wants to scan.
//!
//! # The group comes from the partition key, not the storage key
//!
//! The key group must be derived from the same bytes the exchange routed on —
//! the record's `key_by` output — and **not** from the storage key that a state
//! wrapper happens to build.
//!
//! This distinction is easy to get wrong and fails silently. `KeyedState`
//! stores under `"{namespace}:{encoded_key}"`, so hashing the storage key puts
//! `counts:"alpha"` in a different group than routing put `alpha` in. Nothing
//! breaks immediately, because the physical key is still a pure function of the
//! logical key and every worker derives it identically — reads and writes stay
//! consistent, and state still survives a rescale.
//!
//! What breaks is every *per-key-group* operation, which is the reason key
//! groups exist beyond storage layout:
//!
//! - Warming the groups a worker gains on rescale would prefetch keys belonging
//!   to rows that route elsewhere, and miss the ones that route here.
//! - A scan of [`keyed_prefix`] returns keys the scanning worker does not own.
//!
//! So the group is computed by the layer that knows the logical key (the typed
//! state wrappers, via [`StateContext`](super::context::StateContext)) rather
//! than by a backend wrapper that only ever sees the finished storage key.
//!
//! # Unpartitioned state
//!
//! State that is not keyed by a record key — [`ValueState`](super::value_state)
//! and [`ListState`](super::list_state), which are scoped to a name rather than
//! to a key — has no partition key and therefore no meaningful key group. It
//! stays per-worker:
//!
//! ```text
//! w{worker}/{operator}/{state_key}
//! ```
//!
//! Sharing one instance across workers would let concurrent read-modify-write
//! races corrupt it, so per-worker is the safe reading. The consequence is that
//! unpartitioned state does not survive a change in worker count — which is
//! inherent: without a key there is nothing to redistribute it by.

use crate::cluster::key_group::{KeyGroupId, key_group_for};

/// Width of the zero-padded key group component. Covers the 32768 upper bound.
const KEY_GROUP_WIDTH: usize = 5;

/// The bytes that place a typed key in a key group.
///
/// These must reproduce exactly what the exchange hashed when it routed the
/// record, or state lands in a group the processing worker does not own.
/// `key_by` produces a `String` and the exchange hashes its raw UTF-8, so
/// string keys map to their own bytes. Other key types have no `key_by`
/// counterpart — an operator keying state by something that is not the
/// partition key is already outside what a partitioned model can promise — so
/// they fall back to a canonical JSON form, which at least keeps the mapping
/// stable across processes and restarts.
///
/// This is deliberately independent of the [`KeyEncoder`] a state wrapper uses
/// for *storage*: how a key is serialized to bytes on disk is a storage
/// decision, while which group it belongs to is a routing decision, and the
/// exchange only ever knew about the latter.
///
/// # Errors
/// Returns an error if `key` cannot be serialized.
pub fn partition_bytes<K: serde::Serialize>(key: &K) -> anyhow::Result<Vec<u8>> {
    match serde_json::to_value(key)? {
        serde_json::Value::String(s) => Ok(s.into_bytes()),
        other => Ok(serde_json::to_vec(&other)?),
    }
}

/// Physical key prefix for keyed state in `key_group`: `kg{group:05}/{operator}/`.
///
/// Range-scanning this prefix yields exactly the entries of one key group for
/// one operator — the unit that moves between workers on a rescale.
#[must_use]
pub fn keyed_prefix(operator: &str, key_group: KeyGroupId) -> String {
    format!("kg{key_group:0KEY_GROUP_WIDTH$}/{operator}/")
}

/// Physical key for keyed state.
///
/// `partition_key` decides the key group and must be the bytes the exchange
/// routed on (see the module docs); `state_key` is what actually gets stored
/// under it, and plays no part in choosing the group.
#[must_use]
pub fn keyed_physical_key(
    operator: &str,
    max_parallelism: usize,
    partition_key: &[u8],
    state_key: &[u8],
) -> Vec<u8> {
    let prefix = keyed_prefix(operator, key_group_for(partition_key, max_parallelism));
    join(&prefix, state_key)
}

/// Physical key prefix for unpartitioned state: `w{worker}/{operator}/`.
#[must_use]
pub fn unpartitioned_prefix(operator: &str, worker_index: usize) -> String {
    format!("w{worker_index}/{operator}/")
}

/// Physical key for state that has no partition key.
#[must_use]
pub fn unpartitioned_physical_key(
    operator: &str,
    worker_index: usize,
    state_key: &[u8],
) -> Vec<u8> {
    join(&unpartitioned_prefix(operator, worker_index), state_key)
}

fn join(prefix: &str, state_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + state_key.len());
    out.extend_from_slice(prefix.as_bytes());
    out.extend_from_slice(state_key);
    out
}

/// How one operator's state is laid out in durable storage.
///
/// Held by [`StateContext`](super::context::StateContext), which is the lowest
/// layer that still knows a record's partition key.
#[derive(Debug, Clone)]
pub struct StateAddressing {
    operator: String,
    max_parallelism: usize,
    worker_index: usize,
}

impl StateAddressing {
    /// Build addressing for `operator`.
    ///
    /// # Errors
    /// Returns an error if `operator` contains a `/` (which would make the
    /// namespace ambiguous) or if `max_parallelism` is out of range.
    pub fn new(
        operator: impl Into<String>,
        max_parallelism: usize,
        worker_index: usize,
    ) -> anyhow::Result<Self> {
        let operator = operator.into();
        anyhow::ensure!(
            !operator.contains('/'),
            "operator name must not contain '/': got {operator:?}"
        );
        crate::cluster::key_group::validate_max_parallelism(max_parallelism)?;
        Ok(Self {
            operator,
            max_parallelism,
            worker_index,
        })
    }

    /// The operator these keys belong to.
    #[must_use]
    pub fn operator(&self) -> &str {
        &self.operator
    }

    /// The key group space this operator's state is spread over.
    #[must_use]
    pub fn max_parallelism(&self) -> usize {
        self.max_parallelism
    }

    /// The key group `partition_key` belongs to.
    #[must_use]
    pub fn key_group_of(&self, partition_key: &[u8]) -> KeyGroupId {
        key_group_for(partition_key, self.max_parallelism)
    }

    /// Physical key for keyed state, grouped by `partition_key`.
    #[must_use]
    pub fn keyed(&self, partition_key: &[u8], state_key: &[u8]) -> Vec<u8> {
        keyed_physical_key(
            &self.operator,
            self.max_parallelism,
            partition_key,
            state_key,
        )
    }

    /// Physical key for state with no partition key.
    #[must_use]
    pub fn unpartitioned(&self, state_key: &[u8]) -> Vec<u8> {
        unpartitioned_physical_key(&self.operator, self.worker_index, state_key)
    }

    /// Prefix covering one owned key group, for warming or migration scans.
    #[must_use]
    pub fn key_group_prefix(&self, key_group: KeyGroupId) -> String {
        keyed_prefix(&self.operator, key_group)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::cluster::key_group::worker_for_key_group;

    #[test]
    fn keyed_layout_is_stable() {
        // The on-disk format is a compatibility surface: changing it silently
        // orphans every existing checkpoint.
        let kg = key_group_for(b"alpha", 128);
        assert_eq!(
            keyed_physical_key("myop", 128, b"alpha", b"counts:\"alpha\""),
            format!("kg{kg:05}/myop/counts:\"alpha\"").into_bytes()
        );
    }

    #[test]
    fn unpartitioned_layout_is_stable() {
        assert_eq!(
            unpartitioned_physical_key("myop", 3, b"total"),
            b"w3/myop/total".to_vec()
        );
    }

    #[test]
    fn key_group_prefix_is_zero_padded_for_ordering() {
        // Lexicographic order must track numeric order, or an owned range stops
        // being a contiguous scan.
        let mut prefixes: Vec<String> = [7u16, 100, 9, 1000]
            .iter()
            .map(|kg| keyed_prefix("op", *kg))
            .collect();
        let sorted = {
            let mut s = prefixes.clone();
            s.sort();
            s
        };
        prefixes.sort_by_key(|p| {
            p.trim_start_matches("kg")
                .split('/')
                .next()
                .unwrap()
                .parse::<u16>()
                .unwrap()
        });
        assert_eq!(prefixes, sorted);
    }

    #[test]
    fn keyed_physical_key_ignores_the_state_key_when_choosing_a_group() {
        // Two different state keys (different namespaces) for the same record
        // key must land in the same group, or one operator's namespaces would
        // scatter across groups that different workers own.
        let a = keyed_physical_key("op", 128, b"alpha", b"counts:\"alpha\"");
        let b = keyed_physical_key("op", 128, b"alpha", b"sums:\"alpha\"");
        let group_of = |k: &[u8]| k.split(|c| *c == b'/').next().unwrap().to_vec();
        assert_eq!(group_of(&a), group_of(&b));
    }

    #[test]
    fn state_lands_in_a_group_the_routing_worker_owns() {
        // The invariant the whole design rests on: for any key, the worker the
        // exchange routes the row to is the worker that owns the key group the
        // row's state is filed under. Derive both from the same bytes and they
        // agree by construction; derive the state group from the storage key
        // instead and this fails for essentially every key.
        const MAX_P: usize = 128;
        for parallelism in [1usize, 2, 3, 5, 8, 16] {
            for i in 0..500u32 {
                let record_key = format!("key-{i}");
                let partition = record_key.as_bytes();

                // Where the exchange sends the row.
                let routed_to =
                    worker_for_key_group(MAX_P, parallelism, key_group_for(partition, MAX_P));

                // Which group the row's state is filed under, via the same
                // storage key `KeyedState` would build.
                let state_key = format!("counts:{}", serde_json::to_string(&record_key).unwrap());
                let physical = keyed_physical_key("op", MAX_P, partition, state_key.as_bytes());
                let group: u16 = std::str::from_utf8(&physical[2..7])
                    .unwrap()
                    .parse()
                    .unwrap();

                assert_eq!(
                    worker_for_key_group(MAX_P, parallelism, group),
                    routed_to,
                    "state for {record_key:?} filed in group {group}, owned by a \
                     different worker than the one the row routes to (p={parallelism})"
                );
            }
        }
    }

    #[test]
    fn addressing_rejects_slash_in_operator_name() {
        assert!(StateAddressing::new("a/b", 128, 0).is_err());
    }

    #[test]
    fn addressing_rejects_invalid_max_parallelism() {
        assert!(StateAddressing::new("op", 0, 0).is_err());
        assert!(StateAddressing::new("op", 100_000, 0).is_err());
    }

    #[test]
    fn keyed_is_independent_of_worker_identity() {
        // The point of the whole design: two different workers must produce the
        // same physical key for the same record key, so ownership can move
        // without touching stored data.
        let a = StateAddressing::new("op", 128, 0).unwrap();
        let b = StateAddressing::new("op", 128, 7).unwrap();
        assert_eq!(a.keyed(b"alpha", b"counts"), b.keyed(b"alpha", b"counts"));
        // Unpartitioned state is the deliberate exception.
        assert_ne!(a.unpartitioned(b"total"), b.unpartitioned(b"total"));
    }

    #[test]
    fn operators_do_not_collide() {
        let a = StateAddressing::new("a", 128, 0).unwrap();
        let b = StateAddressing::new("b", 128, 0).unwrap();
        assert_ne!(a.keyed(b"k", b"count"), b.keyed(b"k", b"count"));
    }
}
