//! Key groups: the unit of state partitioning and reassignment.
//!
//! A key group is a bucket of keys that always moves between workers as a
//! single unit. Every key is deterministically hashed into exactly one of
//! `max_parallelism` key groups:
//!
//! ```text
//! key_group = seahash(key) % max_parallelism
//! ```
//!
//! Workers are then assigned *contiguous ranges* of key groups. This is the
//! indirection that makes rescaling possible: `max_parallelism` is fixed for
//! the lifetime of a pipeline, so a key's group never changes, while the
//! range→worker mapping is recomputed freely whenever the worker count
//! changes.
//!
//! Without key groups, routing is `hash(key) % num_workers` and durable state
//! is addressed by worker index — so changing the worker count relocates
//! essentially every key and orphans all persisted state. With key groups,
//! only whole groups move, and state is addressed by group (see
//! [`KeyGroupBackend`](crate::state::key_group_backend::KeyGroupBackend)), so a
//! worker that gains a range simply reads those groups from shared L3 storage.
//!
//! The assignment math is deliberately identical to Flink's
//! `KeyGroupRangeAssignment`, so the operational model (pick a max parallelism
//! up front, rescale freely below it) carries over.

use serde::{Deserialize, Serialize};

/// Default number of key groups when none is configured.
///
/// This caps the parallelism a pipeline can ever rescale to. 128 comfortably
/// covers single-machine and small-cluster deployments while keeping the
/// per-group bookkeeping cheap. Raising it later is a state-incompatible
/// change — every key would rehash into a different group — so it is recorded
/// in the checkpoint manifest and validated on restore.
pub const DEFAULT_MAX_PARALLELISM: usize = 128;

/// Upper bound on `max_parallelism`.
///
/// Matches Flink's limit. Key group IDs are stored as `u16` on the wire.
pub const UPPER_BOUND_MAX_PARALLELISM: usize = 32_768;

/// Identifier of a key group: `0..max_parallelism`.
pub type KeyGroupId = u16;

/// Compute the key group a key belongs to.
///
/// Uses `seahash` — a fixed-seed hash — so the mapping is stable across
/// processes, restarts and platform differences. A `RandomState`-style hasher
/// would silently repartition state on every restart.
#[must_use]
pub fn key_group_for(key: &[u8], max_parallelism: usize) -> KeyGroupId {
    debug_assert!(max_parallelism > 0, "max_parallelism must be non-zero");
    #[allow(clippy::cast_possible_truncation)]
    {
        (seahash::hash(key) % max_parallelism as u64) as KeyGroupId
    }
}

/// Validate a `max_parallelism` value.
///
/// # Errors
/// Returns an error if the value is zero or exceeds [`UPPER_BOUND_MAX_PARALLELISM`].
pub fn validate_max_parallelism(max_parallelism: usize) -> anyhow::Result<()> {
    anyhow::ensure!(max_parallelism > 0, "max_parallelism must be at least 1");
    anyhow::ensure!(
        max_parallelism <= UPPER_BOUND_MAX_PARALLELISM,
        "max_parallelism ({max_parallelism}) must not exceed {UPPER_BOUND_MAX_PARALLELISM}"
    );
    Ok(())
}

/// An inclusive, contiguous range of key groups owned by one worker.
///
/// An empty range (when there are more workers than key groups) is represented
/// by `start > end`; use [`KeyGroupRange::is_empty`] rather than comparing
/// fields directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyGroupRange {
    /// First key group in the range (inclusive).
    pub start: KeyGroupId,
    /// Last key group in the range (inclusive).
    pub end: KeyGroupId,
}

impl KeyGroupRange {
    /// The empty range, owning no key groups.
    pub const EMPTY: Self = Self { start: 1, end: 0 };

    /// Construct a range from inclusive bounds.
    #[must_use]
    pub const fn new(start: KeyGroupId, end: KeyGroupId) -> Self {
        Self { start, end }
    }

    /// Returns `true` if this range owns no key groups.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.start > self.end
    }

    /// Number of key groups in this range.
    #[must_use]
    pub const fn len(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            (self.end - self.start) as usize + 1
        }
    }

    /// Returns `true` if `key_group` falls inside this range.
    #[must_use]
    pub const fn contains(&self, key_group: KeyGroupId) -> bool {
        !self.is_empty() && key_group >= self.start && key_group <= self.end
    }

    /// Iterate over the key group IDs in this range.
    pub fn iter(&self) -> impl Iterator<Item = KeyGroupId> + use<> {
        let (start, end) = (self.start, self.end);
        (0..self.len()).map(move |i| {
            debug_assert!(end >= start);
            #[allow(clippy::cast_possible_truncation)]
            {
                start + i as KeyGroupId
            }
        })
    }

    /// Key groups present in `self` but not in `other`.
    ///
    /// On a rescale this is what a worker *gains* (`new.difference(&old)`) or
    /// *loses* (`old.difference(&new)`) — the input to state cache warming and
    /// to rescale metrics.
    #[must_use]
    pub fn difference(&self, other: &Self) -> Vec<KeyGroupId> {
        self.iter().filter(|kg| !other.contains(*kg)).collect()
    }
}

impl std::fmt::Display for KeyGroupRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            write!(f, "[]")
        } else {
            write!(f, "[{}..={}]", self.start, self.end)
        }
    }
}

/// The number of workers that can actually own key groups.
///
/// `max_parallelism` is a hard ceiling on useful parallelism: there are only
/// that many groups to hand out. Workers beyond it own nothing. Flink rejects
/// `parallelism > maxParallelism` outright; rhei clamps instead, because the
/// worker count is driven by cluster membership and a node joining should not
/// fail the pipeline. The surplus workers still run stateless work and act as
/// standby capacity.
#[must_use]
pub fn effective_parallelism(max_parallelism: usize, parallelism: usize) -> usize {
    parallelism.min(max_parallelism).max(1)
}

/// Compute the key group range owned by `worker_index` out of `parallelism`
/// workers.
///
/// Ranges are contiguous, non-overlapping, and together cover
/// `0..max_parallelism` exactly. When `parallelism > max_parallelism` the
/// surplus workers receive empty ranges — they participate in the dataflow but
/// own no keyed state.
///
/// # Panics
/// Panics if `parallelism` is zero or `worker_index >= parallelism`.
#[must_use]
pub fn range_for_worker(
    max_parallelism: usize,
    parallelism: usize,
    worker_index: usize,
) -> KeyGroupRange {
    assert!(parallelism > 0, "parallelism must be at least 1");
    assert!(
        worker_index < parallelism,
        "worker_index ({worker_index}) must be less than parallelism ({parallelism})"
    );

    // Only the first `effective` workers take part in the split. Splitting
    // across all `parallelism` workers instead would leave *leading* workers
    // empty while routing (which clamps upward) selects trailing ones — the two
    // would disagree about ownership.
    let effective = effective_parallelism(max_parallelism, parallelism);
    if worker_index >= effective {
        return KeyGroupRange::EMPTY;
    }

    // Flink's KeyGroupRangeAssignment split. The ceiling on `start` matters:
    // it is what makes this the exact inverse of `worker_for_key_group`'s
    // `kg * p / max_p`. The intuitive `[i*max/p, (i+1)*max/p - 1]` floor-split
    // tiles the space correctly but disagrees with that routing formula
    // whenever `max_parallelism` is not a multiple of the worker count.
    let start = (worker_index * max_parallelism).div_ceil(effective);
    let end = ((worker_index + 1) * max_parallelism - 1) / effective;

    if end < start {
        return KeyGroupRange::EMPTY;
    }

    #[allow(clippy::cast_possible_truncation)]
    KeyGroupRange::new(start as KeyGroupId, end as KeyGroupId)
}

/// Inverse of [`range_for_worker`]: which worker owns `key_group`.
///
/// This is the routing function used by the exchange pact. It must agree with
/// [`range_for_worker`] exactly — every worker computes routing independently,
/// and a disagreement would silently send rows to a worker that does not own
/// the corresponding state. The `routing_agrees_with_ranges` test pins that
/// invariant across the whole parameter space.
///
/// # Panics
/// Panics if `max_parallelism` or `parallelism` is zero.
#[must_use]
pub fn worker_for_key_group(
    max_parallelism: usize,
    parallelism: usize,
    key_group: KeyGroupId,
) -> usize {
    assert!(max_parallelism > 0, "max_parallelism must be at least 1");
    assert!(parallelism > 0, "parallelism must be at least 1");
    // Inverse of the split above, over the same effective worker count so the
    // two never disagree.
    let effective = effective_parallelism(max_parallelism, parallelism);
    let idx = key_group as usize * effective / max_parallelism;
    idx.min(effective - 1)
}

/// Route a key directly to its owning worker.
///
/// Convenience composition of [`key_group_for`] and [`worker_for_key_group`].
#[must_use]
pub fn worker_for_key(max_parallelism: usize, parallelism: usize, key: &[u8]) -> usize {
    let kg = key_group_for(key, max_parallelism);
    worker_for_key_group(max_parallelism, parallelism, kg)
}

/// The full key-group assignment for one topology.
///
/// Built once per topology generation and shared by the routing path (which
/// needs group→worker) and the state path (which needs worker→range).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyGroupAssignment {
    /// Total number of key groups. Fixed for the pipeline's lifetime.
    pub max_parallelism: usize,
    /// Number of workers this assignment covers.
    pub parallelism: usize,
    /// Range owned by each worker, indexed by worker index.
    pub ranges: Vec<KeyGroupRange>,
}

impl KeyGroupAssignment {
    /// Build the assignment for `parallelism` workers.
    ///
    /// # Errors
    /// Returns an error if `max_parallelism` is out of range or `parallelism`
    /// is zero.
    pub fn new(max_parallelism: usize, parallelism: usize) -> anyhow::Result<Self> {
        validate_max_parallelism(max_parallelism)?;
        anyhow::ensure!(parallelism > 0, "parallelism must be at least 1");
        let ranges = (0..parallelism)
            .map(|i| range_for_worker(max_parallelism, parallelism, i))
            .collect();
        Ok(Self {
            max_parallelism,
            parallelism,
            ranges,
        })
    }

    /// Number of workers that actually own key groups.
    ///
    /// Equal to `parallelism` in the normal case; capped at `max_parallelism`
    /// when the cluster has more workers than there are key groups to hand out.
    #[must_use]
    pub fn effective_parallelism(&self) -> usize {
        effective_parallelism(self.max_parallelism, self.parallelism)
    }

    /// Workers that own no key groups because the cluster outgrew
    /// `max_parallelism`.
    ///
    /// A non-zero count means the pipeline has stopped scaling: adding nodes
    /// no longer adds keyed throughput. Raising `max_parallelism` requires a
    /// state-incompatible restart, so this is worth surfacing loudly.
    #[must_use]
    pub fn idle_workers(&self) -> usize {
        self.parallelism
            .saturating_sub(self.effective_parallelism())
    }

    /// Range owned by `worker_index`, or [`KeyGroupRange::EMPTY`] if out of bounds.
    #[must_use]
    pub fn range(&self, worker_index: usize) -> KeyGroupRange {
        self.ranges
            .get(worker_index)
            .copied()
            .unwrap_or(KeyGroupRange::EMPTY)
    }

    /// Which worker owns `key_group` under this assignment.
    #[must_use]
    pub fn worker_for(&self, key_group: KeyGroupId) -> usize {
        worker_for_key_group(self.max_parallelism, self.parallelism, key_group)
    }

    /// Which worker owns `key` under this assignment.
    #[must_use]
    pub fn worker_for_key(&self, key: &[u8]) -> usize {
        worker_for_key(self.max_parallelism, self.parallelism, key)
    }

    /// Per-worker key groups gained and lost moving from `prev` to `self`.
    ///
    /// Returns one `(gained, lost)` pair per worker in the *new* assignment.
    /// Workers that disappear in the new assignment are not represented — their
    /// groups show up as gains elsewhere.
    #[must_use]
    pub fn migration_from(&self, prev: &Self) -> Vec<KeyGroupMigration> {
        (0..self.parallelism)
            .map(|w| {
                let new_range = self.range(w);
                let old_range = prev.range(w);
                KeyGroupMigration {
                    worker_index: w,
                    previous: old_range,
                    current: new_range,
                    gained: new_range.difference(&old_range),
                    lost: old_range.difference(&new_range),
                }
            })
            .collect()
    }

    /// Total number of key groups that change owner between `prev` and `self`.
    ///
    /// Useful as a rescale cost metric: it is the number of groups that must be
    /// warmed from L3 on the gaining workers.
    #[must_use]
    pub fn moved_key_groups(&self, prev: &Self) -> usize {
        (0..self.max_parallelism)
            .filter(|&kg| {
                #[allow(clippy::cast_possible_truncation)]
                let kg = kg as KeyGroupId;
                self.worker_for(kg) != prev.worker_for(kg)
            })
            .count()
    }
}

/// What one worker gains and loses across a rescale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyGroupMigration {
    /// Worker index in the new assignment.
    pub worker_index: usize,
    /// Range owned before the rescale.
    pub previous: KeyGroupRange,
    /// Range owned after the rescale.
    pub current: KeyGroupRange,
    /// Key groups newly owned by this worker (must be warmed from L3).
    pub gained: Vec<KeyGroupId>,
    /// Key groups no longer owned by this worker (local caches may be dropped).
    pub lost: Vec<KeyGroupId>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn key_group_is_stable_across_calls() {
        let a = key_group_for(b"user-42", 128);
        let b = key_group_for(b"user-42", 128);
        assert_eq!(a, b);
    }

    #[test]
    fn key_group_is_within_bounds() {
        for i in 0..1000u32 {
            let key = format!("key-{i}");
            let kg = key_group_for(key.as_bytes(), 128);
            assert!(
                kg < 128,
                "key group {kg} out of bounds for max_parallelism 128"
            );
        }
    }

    #[test]
    fn ranges_tile_the_whole_key_group_space() {
        // For every plausible (max_parallelism, parallelism) pair the ranges must
        // cover 0..max exactly once — no gaps, no overlaps. A gap would drop
        // state; an overlap would let two workers write the same keys.
        for max_p in [1usize, 2, 3, 16, 128, 129, 256] {
            for p in 1..=(max_p + 3) {
                let mut seen = vec![0u32; max_p];
                for w in 0..p {
                    let range = range_for_worker(max_p, p, w);
                    for kg in range.iter() {
                        seen[kg as usize] += 1;
                    }
                }
                for (kg, count) in seen.iter().enumerate() {
                    assert_eq!(
                        *count, 1,
                        "key group {kg} covered {count} times with max_p={max_p} p={p}"
                    );
                }
            }
        }
    }

    #[test]
    fn routing_agrees_with_ranges() {
        // worker_for_key_group must select exactly the worker whose range
        // contains the group. Disagreement would route rows to a worker that
        // does not own the matching state.
        for max_p in [1usize, 2, 16, 128, 256] {
            for p in 1..=(max_p + 3) {
                for kg in 0..max_p {
                    #[allow(clippy::cast_possible_truncation)]
                    let kg = kg as KeyGroupId;
                    let worker = worker_for_key_group(max_p, p, kg);
                    let range = range_for_worker(max_p, p, worker);
                    assert!(
                        range.contains(kg),
                        "kg {kg} routed to worker {worker} whose range is {range} \
                         (max_p={max_p}, p={p})"
                    );
                }
            }
        }
    }

    #[test]
    fn key_group_never_changes_when_parallelism_changes() {
        // The whole point of key groups: a key's group is a function of
        // max_parallelism alone, so rescaling cannot rehash it.
        let keys: Vec<String> = (0..200).map(|i| format!("user-{i}")).collect();
        let baseline: Vec<KeyGroupId> = keys
            .iter()
            .map(|k| key_group_for(k.as_bytes(), 128))
            .collect();

        for _p in [1usize, 2, 4, 7, 16, 64] {
            let groups: Vec<KeyGroupId> = keys
                .iter()
                .map(|k| key_group_for(k.as_bytes(), 128))
                .collect();
            assert_eq!(groups, baseline);
        }
    }

    #[test]
    fn rescale_moves_only_a_fraction_of_key_groups() {
        // Contiguous-range assignment should keep most groups in place when
        // scaling by one. If this regresses to hash % n, nearly every group moves.
        let before = KeyGroupAssignment::new(128, 4).unwrap();
        let after = KeyGroupAssignment::new(128, 5).unwrap();
        let moved = after.moved_key_groups(&before);
        assert!(
            moved < 128,
            "expected some key groups to stay put, but {moved}/128 moved"
        );
        assert!(
            moved > 0,
            "scaling 4 -> 5 must move at least some key groups"
        );
    }

    #[test]
    fn empty_ranges_when_more_workers_than_key_groups() {
        let assignment = KeyGroupAssignment::new(4, 8).unwrap();
        let non_empty = assignment.ranges.iter().filter(|r| !r.is_empty()).count();
        assert_eq!(non_empty, 4, "only 4 workers can own the 4 key groups");
        assert_eq!(assignment.effective_parallelism(), 4);
        assert_eq!(assignment.idle_workers(), 4);

        // Every key group still routes to a worker that owns it, and the idle
        // workers are the *trailing* ones — surplus capacity, not a hole in the
        // middle of the assignment.
        for kg in 0..4u16 {
            let w = assignment.worker_for(kg);
            assert!(w < 4, "key group {kg} routed to idle worker {w}");
            assert!(assignment.range(w).contains(kg));
        }
        for w in 4..8 {
            assert!(assignment.range(w).is_empty());
        }
    }

    #[test]
    fn routing_never_selects_an_idle_worker() {
        // Regression guard: an earlier version split ranges across all workers
        // while routing clamped upward, so with p > max_p routing selected
        // workers whose range was empty and rows landed on a worker holding
        // none of the matching state.
        for max_p in 1usize..=8 {
            for p in 1usize..=16 {
                let assignment = KeyGroupAssignment::new(max_p, p).unwrap();
                for kg in 0..max_p {
                    #[allow(clippy::cast_possible_truncation)]
                    let kg = kg as KeyGroupId;
                    let w = assignment.worker_for(kg);
                    assert!(
                        assignment.range(w).contains(kg),
                        "max_p={max_p} p={p}: kg {kg} routed to worker {w} with empty/wrong range {}",
                        assignment.range(w)
                    );
                }
            }
        }
    }

    #[test]
    fn single_worker_owns_everything() {
        let assignment = KeyGroupAssignment::new(128, 1).unwrap();
        assert_eq!(assignment.range(0), KeyGroupRange::new(0, 127));
        assert_eq!(assignment.range(0).len(), 128);
    }

    #[test]
    fn migration_reports_gains_and_losses() {
        let before = KeyGroupAssignment::new(16, 2).unwrap();
        let after = KeyGroupAssignment::new(16, 4).unwrap();
        let migrations = after.migration_from(&before);
        assert_eq!(migrations.len(), 4);

        // Scaling out: worker 0 shrinks (loses groups, gains none).
        assert!(migrations[0].gained.is_empty());
        assert!(!migrations[0].lost.is_empty());

        // Every group lost by someone is gained by someone else — nothing is dropped.
        let lost: HashSet<KeyGroupId> = migrations
            .iter()
            .flat_map(|m| m.lost.iter().copied())
            .collect();
        let gained: HashSet<KeyGroupId> = migrations
            .iter()
            .flat_map(|m| m.gained.iter().copied())
            .collect();
        for kg in &lost {
            // A lost group is either gained by another worker or was already
            // out of range for the shrinking worker.
            let new_owner = after.worker_for(*kg);
            let old_owner = before.worker_for(*kg);
            if new_owner != old_owner {
                assert!(
                    gained.contains(kg),
                    "key group {kg} moved {old_owner} -> {new_owner} but nobody records gaining it"
                );
            }
        }
    }

    #[test]
    fn range_len_and_contains_agree() {
        let range = KeyGroupRange::new(4, 9);
        assert_eq!(range.len(), 6);
        assert_eq!(range.iter().count(), 6);
        assert!(range.contains(4));
        assert!(range.contains(9));
        assert!(!range.contains(3));
        assert!(!range.contains(10));
    }

    #[test]
    fn empty_range_contains_nothing() {
        let range = KeyGroupRange::EMPTY;
        assert!(range.is_empty());
        assert_eq!(range.len(), 0);
        assert_eq!(range.iter().count(), 0);
        for kg in 0..64u16 {
            assert!(!range.contains(kg));
        }
    }

    #[test]
    fn validate_max_parallelism_bounds() {
        assert!(validate_max_parallelism(0).is_err());
        assert!(validate_max_parallelism(1).is_ok());
        assert!(validate_max_parallelism(DEFAULT_MAX_PARALLELISM).is_ok());
        assert!(validate_max_parallelism(UPPER_BOUND_MAX_PARALLELISM).is_ok());
        assert!(validate_max_parallelism(UPPER_BOUND_MAX_PARALLELISM + 1).is_err());
    }

    #[test]
    fn key_distribution_is_reasonably_even() {
        // Not a strict guarantee, but a wildly skewed hash would make rescaling
        // useless in practice.
        let mut counts = [0usize; 16];
        for i in 0..16_000u32 {
            let key = format!("user-{i}");
            let w = worker_for_key(128, 16, key.as_bytes());
            counts[w] += 1;
        }
        let expected = 1000.0;
        for (w, count) in counts.iter().enumerate() {
            #[allow(clippy::cast_precision_loss)]
            let ratio = *count as f64 / expected;
            assert!(
                ratio > 0.5 && ratio < 1.5,
                "worker {w} received {count} keys, expected ~{expected}"
            );
        }
    }
}
