//! Reusable per-batch bump arena for hot-path scratch allocations.
//!
//! [`BatchArena`] wraps a [`bumpalo::Bump`] that lives as long as an operator
//! and is recycled between batches. Operators that need transient scratch
//! space while processing a batch (row-routing lists, staging buffers)
//! allocate it from the arena instead of the global allocator: allocation is
//! a pointer bump, and [`reset`](BatchArena::reset) reclaims everything at
//! once while keeping the backing chunk warm for the next batch. After the
//! first few batches the steady state is zero global-allocator traffic for
//! scratch data.
//!
//! This matters because Timely workers process batches in a tight loop on
//! blocking threads, and (as `ADR/memory-allocator.md` documents) rhei stays
//! on whatever global allocator the embedding binary chose — which is glibc
//! ptmalloc2 by default, the allocator that handles exactly this
//! high-churn pattern worst. The arena sidesteps the question entirely for
//! scratch allocations rather than betting on the embedder's choice.
//!
//! Anything that must outlive the batch (the Arrow arrays handed to Timely,
//! for example) still goes through the global allocator — the arena is for
//! data that dies with the batch.

use bumpalo::Bump;

/// A reusable bump arena scoped to one operator instance.
///
/// One arena per operator per worker: Timely operator closures are
/// single-threaded, so the arena needs no synchronization. Call
/// [`bump`](Self::bump) to allocate scratch for the current batch and
/// [`reset`](Self::reset) before starting the next one.
pub struct BatchArena {
    bump: Bump,
}

impl BatchArena {
    /// Retained-capacity cap applied by [`reset`](Self::reset).
    ///
    /// `Bump::reset` keeps the largest chunk allocated so far, so one
    /// pathologically large batch would otherwise pin that high-water mark
    /// for the lifetime of the operator. Above this cap the arena is
    /// rebuilt from scratch instead. 4 MiB comfortably covers the routing
    /// scratch for millions of rows per batch.
    pub const MAX_RETAINED_BYTES: usize = 4 << 20;

    /// Create an empty arena. No memory is allocated until first use.
    pub fn new() -> Self {
        Self { bump: Bump::new() }
    }

    /// Access the underlying bump allocator for the current batch.
    pub fn bump(&self) -> &Bump {
        &self.bump
    }

    /// Recycle the arena for the next batch.
    ///
    /// Frees all arena allocations in O(1) and retains the largest backing
    /// chunk for reuse — unless it grew past
    /// [`MAX_RETAINED_BYTES`](Self::MAX_RETAINED_BYTES), in which case the
    /// arena is replaced so a single outlier batch doesn't pin memory
    /// forever.
    pub fn reset(&mut self) {
        if self.bump.allocated_bytes() > Self::MAX_RETAINED_BYTES {
            self.bump = Bump::new();
        } else {
            self.bump.reset();
        }
    }
}

impl Default for BatchArena {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for BatchArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchArena")
            .field("allocated_bytes", &self.bump.allocated_bytes())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_is_usable_across_resets() {
        let mut arena = BatchArena::new();
        for round in 0..3u32 {
            arena.reset();
            let mut v = bumpalo::collections::Vec::new_in(arena.bump());
            for i in 0..100u32 {
                v.push(i + round);
            }
            assert_eq!(v.len(), 100);
            assert_eq!(v[0], round);
        }
    }

    #[test]
    fn reset_retains_bounded_footprint() {
        // `allocated_bytes` reports chunk footprint. After a reset the
        // largest chunk is kept warm for the next batch, so the footprint
        // stays non-zero but must not exceed the retention cap.
        let mut arena = BatchArena::new();
        let mut v = bumpalo::collections::Vec::new_in(arena.bump());
        v.extend(0..10_000u64);
        drop(v);
        assert!(arena.bump().allocated_bytes() > 0);
        let before = arena.bump().allocated_bytes();
        arena.reset();
        let after = arena.bump().allocated_bytes();
        assert!(after > 0, "reset should keep a warm chunk");
        assert!(after <= before);
        assert!(after <= BatchArena::MAX_RETAINED_BYTES);
    }

    #[test]
    fn reset_discards_oversized_arena() {
        let mut arena = BatchArena::new();
        // Allocate past the retention cap, then reset: the arena must shrink
        // back to an empty bump rather than pinning the oversized chunk.
        let mut v = bumpalo::collections::Vec::new_in(arena.bump());
        v.extend(std::iter::repeat_n(0u8, BatchArena::MAX_RETAINED_BYTES + 1));
        drop(v);
        arena.reset();
        assert_eq!(arena.bump().allocated_bytes(), 0);
    }
}
