//! Fluent builders for the windowing operators.
//!
//! The window constructors take four or five positional closures whose roles
//! are impossible to tell apart at a call site:
//!
//! ```ignore
//! // not-compiled: illustrative, requires a derived schema in scope
//! TumblingWindow::new(60_000, key_fn, time_fn, accumulate_fn, finish_fn)
//! ```
//!
//! [`Window`] names each one instead:
//!
//! ```ignore
//! // not-compiled: illustrative, requires a derived schema in scope
//! Window::tumbling(60_000)
//!     .key(|v: PageViewView<'_>| v.user_id.to_string())
//!     .time(|v: PageViewView<'_>| v.ts)
//!     .accumulate(|acc: &mut u64, _v: PageViewView<'_>| *acc += 1)
//!     .finish(|user_id: &str, start: u64, end: u64, views: &u64| PerMinute { .. })
//!     .allowed_lateness(5_000)
//!     .build()
//! ```
//!
//! The slots may be filled in any order. Each one is tracked in the builder's
//! type, so `build()` only exists once every required closure has been
//! supplied — a missing `.time(..)` is a compile error, not a runtime
//! surprise. The built operator is exactly what the corresponding
//! `Window::new` would have produced; the builder adds no runtime cost.
//!
//! The input, output, and accumulator types are inferred at the point the
//! operator is attached to a stream, the same way they are for the direct
//! constructors.

use std::fmt;

use super::count_window::CountWindow;
use super::session_window::SessionWindow;
use super::sliding_window::SlidingWindow;
use super::tumbling_window::TumblingWindow;

mod sealed {
    pub trait Sealed {}
}

/// A builder slot holding a closure that has been supplied.
#[derive(Debug, Clone, Copy)]
pub struct Set<F>(F);

/// A builder slot whose closure has not been supplied yet.
#[derive(Debug, Clone, Copy, Default)]
pub struct Unset;

/// Implemented only by [`Set`], so that `build()` is unreachable until every
/// required closure has been supplied.
#[diagnostic::on_unimplemented(
    message = "this window builder is missing a required closure",
    label = "`build()` needs every slot filled first",
    note = "supply the missing `.key(..)`, `.time(..)`, `.accumulate(..)` or `.finish(..)` call"
)]
pub trait Filled: sealed::Sealed {
    /// The closure type stored in this slot.
    type Inner;

    /// Unwraps the stored closure.
    fn into_inner(self) -> Self::Inner;
}

impl<F> sealed::Sealed for Set<F> {}

impl<F> Filled for Set<F> {
    type Inner = F;

    fn into_inner(self) -> F {
        self.0
    }
}

/// Entry point for the window builders.
///
/// Each constructor takes the window's defining parameter — a size, a size and
/// a slide, an inactivity gap, or a row count — and returns a builder whose
/// remaining slots are filled by name.
///
/// Time parameters are in whatever unit the `time()` closure returns; they are
/// not tied to milliseconds.
#[derive(Debug, Clone, Copy, Default)]
pub struct Window;

impl Window {
    /// Starts a [`TumblingWindow`] builder with fixed, non-overlapping windows
    /// of `window_size`.
    ///
    /// # Panics
    /// Panics if `window_size` is 0.
    #[must_use]
    pub fn tumbling(window_size: u64) -> TumblingWindowBuilder<Unset, Unset, Unset, Unset> {
        assert!(window_size > 0, "window_size must be > 0");
        TumblingWindowBuilder {
            window_size,
            allowed_lateness: 0,
            key_fn: Unset,
            time_fn: Unset,
            accumulate_fn: Unset,
            finish_fn: Unset,
        }
    }

    /// Starts a [`SlidingWindow`] builder emitting a window of `window_size`
    /// every `slide` units. Each row lands in `window_size / slide` windows.
    ///
    /// # Panics
    /// Panics if `window_size` or `slide` is 0, or if `slide > window_size`.
    #[must_use]
    pub fn sliding(
        window_size: u64,
        slide: u64,
    ) -> SlidingWindowBuilder<Unset, Unset, Unset, Unset> {
        assert!(window_size > 0, "window_size must be > 0");
        assert!(slide > 0, "slide must be > 0");
        assert!(slide <= window_size, "slide must be <= window_size");
        SlidingWindowBuilder {
            window_size,
            slide,
            allowed_lateness: 0,
            key_fn: Unset,
            time_fn: Unset,
            accumulate_fn: Unset,
            finish_fn: Unset,
        }
    }

    /// Starts a [`SessionWindow`] builder that closes a session after `gap`
    /// units of inactivity for a key.
    ///
    /// # Panics
    /// Panics if `gap` is 0.
    #[must_use]
    pub fn session(gap: u64) -> SessionWindowBuilder<Unset, Unset, Unset, Unset> {
        assert!(gap > 0, "gap must be > 0");
        SessionWindowBuilder {
            gap,
            allowed_lateness: 0,
            key_fn: Unset,
            time_fn: Unset,
            accumulate_fn: Unset,
            finish_fn: Unset,
        }
    }

    /// Starts a [`CountWindow`] builder that fires every `threshold` rows for a
    /// key. Count windows have no notion of time, so they take no `time()`
    /// closure and no allowed lateness.
    ///
    /// # Panics
    /// Panics if `threshold` is 0.
    #[must_use]
    pub fn count(threshold: u64) -> CountWindowBuilder<Unset, Unset, Unset> {
        assert!(threshold > 0, "threshold must be > 0");
        CountWindowBuilder {
            threshold,
            key_fn: Unset,
            accumulate_fn: Unset,
            finish_fn: Unset,
        }
    }
}

/// Builder for [`TumblingWindow`], created by [`Window::tumbling`].
pub struct TumblingWindowBuilder<KF, TF, AF, FF> {
    window_size: u64,
    allowed_lateness: u64,
    key_fn: KF,
    time_fn: TF,
    accumulate_fn: AF,
    finish_fn: FF,
}

impl<KF, TF, AF, FF> fmt::Debug for TumblingWindowBuilder<KF, TF, AF, FF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TumblingWindowBuilder")
            .field("window_size", &self.window_size)
            .field("allowed_lateness", &self.allowed_lateness)
            .finish_non_exhaustive()
    }
}

impl<KF, TF, AF, FF> TumblingWindowBuilder<KF, TF, AF, FF> {
    /// Sets the grouping key: `Fn(I::View<'_>) -> String`.
    #[must_use]
    pub fn key<F>(self, key_fn: F) -> TumblingWindowBuilder<Set<F>, TF, AF, FF> {
        TumblingWindowBuilder {
            window_size: self.window_size,
            allowed_lateness: self.allowed_lateness,
            key_fn: Set(key_fn),
            time_fn: self.time_fn,
            accumulate_fn: self.accumulate_fn,
            finish_fn: self.finish_fn,
        }
    }

    /// Sets the event-time extractor: `Fn(I::View<'_>) -> u64`.
    #[must_use]
    pub fn time<F>(self, time_fn: F) -> TumblingWindowBuilder<KF, Set<F>, AF, FF> {
        TumblingWindowBuilder {
            window_size: self.window_size,
            allowed_lateness: self.allowed_lateness,
            key_fn: self.key_fn,
            time_fn: Set(time_fn),
            accumulate_fn: self.accumulate_fn,
            finish_fn: self.finish_fn,
        }
    }

    /// Sets the accumulation step: `Fn(&mut Acc, I::View<'_>)`.
    #[must_use]
    pub fn accumulate<F>(self, accumulate_fn: F) -> TumblingWindowBuilder<KF, TF, Set<F>, FF> {
        TumblingWindowBuilder {
            window_size: self.window_size,
            allowed_lateness: self.allowed_lateness,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            accumulate_fn: Set(accumulate_fn),
            finish_fn: self.finish_fn,
        }
    }

    /// Sets the emit step: `Fn(&str, u64, u64, &Acc) -> O`, receiving the key,
    /// window start, window end, and the finished accumulator.
    #[must_use]
    pub fn finish<F>(self, finish_fn: F) -> TumblingWindowBuilder<KF, TF, AF, Set<F>> {
        TumblingWindowBuilder {
            window_size: self.window_size,
            allowed_lateness: self.allowed_lateness,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            accumulate_fn: self.accumulate_fn,
            finish_fn: Set(finish_fn),
        }
    }

    /// Sets the grace period for late rows. Defaults to 0.
    #[must_use]
    pub fn allowed_lateness(mut self, lateness: u64) -> Self {
        self.allowed_lateness = lateness;
        self
    }
}

impl<KF: Filled, TF: Filled, AF: Filled, FF: Filled> TumblingWindowBuilder<KF, TF, AF, FF> {
    /// Builds the operator. Type parameters are `build::<Input, Output, Acc>()`
    /// when inference needs help.
    // The window operators carry six or seven type parameters; naming the
    // return type is unavoidably wide.
    #[allow(clippy::type_complexity)]
    #[must_use]
    pub fn build<I, O, Acc>(
        self,
    ) -> TumblingWindow<I, O, Acc, KF::Inner, TF::Inner, AF::Inner, FF::Inner> {
        TumblingWindow::new(
            self.window_size,
            self.key_fn.into_inner(),
            self.time_fn.into_inner(),
            self.accumulate_fn.into_inner(),
            self.finish_fn.into_inner(),
        )
        .with_allowed_lateness(self.allowed_lateness)
    }
}

/// Builder for [`SlidingWindow`], created by [`Window::sliding`].
pub struct SlidingWindowBuilder<KF, TF, AF, FF> {
    window_size: u64,
    slide: u64,
    allowed_lateness: u64,
    key_fn: KF,
    time_fn: TF,
    accumulate_fn: AF,
    finish_fn: FF,
}

impl<KF, TF, AF, FF> fmt::Debug for SlidingWindowBuilder<KF, TF, AF, FF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlidingWindowBuilder")
            .field("window_size", &self.window_size)
            .field("slide", &self.slide)
            .field("allowed_lateness", &self.allowed_lateness)
            .finish_non_exhaustive()
    }
}

impl<KF, TF, AF, FF> SlidingWindowBuilder<KF, TF, AF, FF> {
    /// Sets the grouping key: `Fn(I::View<'_>) -> String`.
    #[must_use]
    pub fn key<F>(self, key_fn: F) -> SlidingWindowBuilder<Set<F>, TF, AF, FF> {
        SlidingWindowBuilder {
            window_size: self.window_size,
            slide: self.slide,
            allowed_lateness: self.allowed_lateness,
            key_fn: Set(key_fn),
            time_fn: self.time_fn,
            accumulate_fn: self.accumulate_fn,
            finish_fn: self.finish_fn,
        }
    }

    /// Sets the event-time extractor: `Fn(I::View<'_>) -> u64`.
    #[must_use]
    pub fn time<F>(self, time_fn: F) -> SlidingWindowBuilder<KF, Set<F>, AF, FF> {
        SlidingWindowBuilder {
            window_size: self.window_size,
            slide: self.slide,
            allowed_lateness: self.allowed_lateness,
            key_fn: self.key_fn,
            time_fn: Set(time_fn),
            accumulate_fn: self.accumulate_fn,
            finish_fn: self.finish_fn,
        }
    }

    /// Sets the accumulation step: `Fn(&mut Acc, I::View<'_>)`.
    #[must_use]
    pub fn accumulate<F>(self, accumulate_fn: F) -> SlidingWindowBuilder<KF, TF, Set<F>, FF> {
        SlidingWindowBuilder {
            window_size: self.window_size,
            slide: self.slide,
            allowed_lateness: self.allowed_lateness,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            accumulate_fn: Set(accumulate_fn),
            finish_fn: self.finish_fn,
        }
    }

    /// Sets the emit step: `Fn(&str, u64, u64, &Acc) -> O`, receiving the key,
    /// window start, window end, and the finished accumulator.
    #[must_use]
    pub fn finish<F>(self, finish_fn: F) -> SlidingWindowBuilder<KF, TF, AF, Set<F>> {
        SlidingWindowBuilder {
            window_size: self.window_size,
            slide: self.slide,
            allowed_lateness: self.allowed_lateness,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            accumulate_fn: self.accumulate_fn,
            finish_fn: Set(finish_fn),
        }
    }

    /// Sets the grace period for late rows. Defaults to 0.
    #[must_use]
    pub fn allowed_lateness(mut self, lateness: u64) -> Self {
        self.allowed_lateness = lateness;
        self
    }
}

impl<KF: Filled, TF: Filled, AF: Filled, FF: Filled> SlidingWindowBuilder<KF, TF, AF, FF> {
    /// Builds the operator. Type parameters are `build::<Input, Output, Acc>()`
    /// when inference needs help.
    // The window operators carry six or seven type parameters; naming the
    // return type is unavoidably wide.
    #[allow(clippy::type_complexity)]
    #[must_use]
    pub fn build<I, O, Acc>(
        self,
    ) -> SlidingWindow<I, O, Acc, KF::Inner, TF::Inner, AF::Inner, FF::Inner> {
        SlidingWindow::new(
            self.window_size,
            self.slide,
            self.key_fn.into_inner(),
            self.time_fn.into_inner(),
            self.accumulate_fn.into_inner(),
            self.finish_fn.into_inner(),
        )
        .with_allowed_lateness(self.allowed_lateness)
    }
}

/// Builder for [`SessionWindow`], created by [`Window::session`].
pub struct SessionWindowBuilder<KF, TF, AF, FF> {
    gap: u64,
    allowed_lateness: u64,
    key_fn: KF,
    time_fn: TF,
    accumulate_fn: AF,
    finish_fn: FF,
}

impl<KF, TF, AF, FF> fmt::Debug for SessionWindowBuilder<KF, TF, AF, FF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionWindowBuilder")
            .field("gap", &self.gap)
            .field("allowed_lateness", &self.allowed_lateness)
            .finish_non_exhaustive()
    }
}

impl<KF, TF, AF, FF> SessionWindowBuilder<KF, TF, AF, FF> {
    /// Sets the grouping key: `Fn(I::View<'_>) -> String`.
    #[must_use]
    pub fn key<F>(self, key_fn: F) -> SessionWindowBuilder<Set<F>, TF, AF, FF> {
        SessionWindowBuilder {
            gap: self.gap,
            allowed_lateness: self.allowed_lateness,
            key_fn: Set(key_fn),
            time_fn: self.time_fn,
            accumulate_fn: self.accumulate_fn,
            finish_fn: self.finish_fn,
        }
    }

    /// Sets the event-time extractor: `Fn(I::View<'_>) -> u64`.
    #[must_use]
    pub fn time<F>(self, time_fn: F) -> SessionWindowBuilder<KF, Set<F>, AF, FF> {
        SessionWindowBuilder {
            gap: self.gap,
            allowed_lateness: self.allowed_lateness,
            key_fn: self.key_fn,
            time_fn: Set(time_fn),
            accumulate_fn: self.accumulate_fn,
            finish_fn: self.finish_fn,
        }
    }

    /// Sets the accumulation step: `Fn(&mut Acc, I::View<'_>)`.
    #[must_use]
    pub fn accumulate<F>(self, accumulate_fn: F) -> SessionWindowBuilder<KF, TF, Set<F>, FF> {
        SessionWindowBuilder {
            gap: self.gap,
            allowed_lateness: self.allowed_lateness,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            accumulate_fn: Set(accumulate_fn),
            finish_fn: self.finish_fn,
        }
    }

    /// Sets the emit step: `Fn(&str, u64, u64, &Acc) -> O`, receiving the key,
    /// session start, last event time, and the finished accumulator.
    #[must_use]
    pub fn finish<F>(self, finish_fn: F) -> SessionWindowBuilder<KF, TF, AF, Set<F>> {
        SessionWindowBuilder {
            gap: self.gap,
            allowed_lateness: self.allowed_lateness,
            key_fn: self.key_fn,
            time_fn: self.time_fn,
            accumulate_fn: self.accumulate_fn,
            finish_fn: Set(finish_fn),
        }
    }

    /// Sets the grace period for late rows. Defaults to 0.
    #[must_use]
    pub fn allowed_lateness(mut self, lateness: u64) -> Self {
        self.allowed_lateness = lateness;
        self
    }
}

impl<KF: Filled, TF: Filled, AF: Filled, FF: Filled> SessionWindowBuilder<KF, TF, AF, FF> {
    /// Builds the operator. Type parameters are `build::<Input, Output, Acc>()`
    /// when inference needs help.
    // The window operators carry six or seven type parameters; naming the
    // return type is unavoidably wide.
    #[allow(clippy::type_complexity)]
    #[must_use]
    pub fn build<I, O, Acc>(
        self,
    ) -> SessionWindow<I, O, Acc, KF::Inner, TF::Inner, AF::Inner, FF::Inner> {
        SessionWindow::new(
            self.gap,
            self.key_fn.into_inner(),
            self.time_fn.into_inner(),
            self.accumulate_fn.into_inner(),
            self.finish_fn.into_inner(),
        )
        .with_allowed_lateness(self.allowed_lateness)
    }
}

/// Builder for [`CountWindow`], created by [`Window::count`].
pub struct CountWindowBuilder<KF, AF, FF> {
    threshold: u64,
    key_fn: KF,
    accumulate_fn: AF,
    finish_fn: FF,
}

impl<KF, AF, FF> fmt::Debug for CountWindowBuilder<KF, AF, FF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountWindowBuilder")
            .field("threshold", &self.threshold)
            .finish_non_exhaustive()
    }
}

impl<KF, AF, FF> CountWindowBuilder<KF, AF, FF> {
    /// Sets the grouping key: `Fn(I::View<'_>) -> String`.
    #[must_use]
    pub fn key<F>(self, key_fn: F) -> CountWindowBuilder<Set<F>, AF, FF> {
        CountWindowBuilder {
            threshold: self.threshold,
            key_fn: Set(key_fn),
            accumulate_fn: self.accumulate_fn,
            finish_fn: self.finish_fn,
        }
    }

    /// Sets the accumulation step: `Fn(&mut Acc, I::View<'_>)`.
    #[must_use]
    pub fn accumulate<F>(self, accumulate_fn: F) -> CountWindowBuilder<KF, Set<F>, FF> {
        CountWindowBuilder {
            threshold: self.threshold,
            key_fn: self.key_fn,
            accumulate_fn: Set(accumulate_fn),
            finish_fn: self.finish_fn,
        }
    }

    /// Sets the emit step: `Fn(&str, u64, &Acc) -> O`, receiving the key, the
    /// row count that triggered the window, and the finished accumulator.
    #[must_use]
    pub fn finish<F>(self, finish_fn: F) -> CountWindowBuilder<KF, AF, Set<F>> {
        CountWindowBuilder {
            threshold: self.threshold,
            key_fn: self.key_fn,
            accumulate_fn: self.accumulate_fn,
            finish_fn: Set(finish_fn),
        }
    }
}

impl<KF: Filled, AF: Filled, FF: Filled> CountWindowBuilder<KF, AF, FF> {
    /// Builds the operator. Type parameters are `build::<Input, Output, Acc>()`
    /// when inference needs help.
    // The window operators carry six or seven type parameters; naming the
    // return type is unavoidably wide.
    #[allow(clippy::type_complexity)]
    #[must_use]
    pub fn build<I, O, Acc>(self) -> CountWindow<I, O, Acc, KF::Inner, AF::Inner, FF::Inner> {
        CountWindow::new(
            self.threshold,
            self.key_fn.into_inner(),
            self.accumulate_fn.into_inner(),
            self.finish_fn.into_inner(),
        )
    }
}
