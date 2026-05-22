//! Batch sequence detection operator (`MATCH_RECOGNIZE` equivalent).
//!
//! Detects ordered event sequences within a time window per partition key.
//! Sequences are matched greedily: quantified steps consume matching events
//! until the next step's predicate matches.

use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::keyed_state::KeyedState;
use crate::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, StreamFunction,
};

// ── Public types ────────────────────────────────────────────────────

/// Strategy for resuming after a complete match.
#[derive(Clone, Debug, Default)]
pub enum AfterMatch {
    /// Overlapping matches allowed. Only the completed sequence is removed;
    /// other in-flight sequences for the same partition continue.
    #[default]
    SkipToNextRow,
    /// Non-overlapping. All in-flight sequences for the partition are cleared
    /// after a match fires.
    SkipPastMatch,
}

/// Repetition bounds for a sequence step.
#[derive(Clone, Debug)]
pub struct Quantifier {
    /// Minimum number of events required to satisfy this step.
    pub min: usize,
    /// Maximum events (None = unbounded).
    pub max: Option<usize>,
}

impl Default for Quantifier {
    fn default() -> Self {
        Self {
            min: 1,
            max: Some(1),
        }
    }
}

impl Quantifier {
    fn is_satisfied(&self, count: usize) -> bool {
        count >= self.min
    }

    fn is_maxed(&self, count: usize) -> bool {
        self.max.is_some_and(|m| count >= m)
    }
}

/// Context passed to the emit function when a match completes.
#[derive(Debug)]
pub struct MatchCtx {
    first_time: u64,
    last_time: u64,
    step_meta: Vec<StepMeta>,
    buffered_events: Option<Vec<Vec<Vec<u8>>>>,
}

#[derive(Debug)]
struct StepMeta {
    name: String,
    count: usize,
    first_time: u64,
    last_time: u64,
}

impl MatchCtx {
    /// Timestamp of the first event in the matched sequence.
    pub fn first_time(&self) -> u64 {
        self.first_time
    }

    /// Timestamp of the last event in the matched sequence.
    pub fn last_time(&self) -> u64 {
        self.last_time
    }

    /// Time span of the match (last - first).
    pub fn duration(&self) -> u64 {
        self.last_time.saturating_sub(self.first_time)
    }

    /// Number of events that matched the given step.
    pub fn step_count(&self, name: &str) -> usize {
        self.step_meta
            .iter()
            .find(|s| s.name == name)
            .map_or(0, |s| s.count)
    }

    /// Timestamp of the first event matching the given step.
    pub fn step_first_time(&self, name: &str) -> u64 {
        self.step_meta
            .iter()
            .find(|s| s.name == name)
            .map_or(0, |s| s.first_time)
    }

    /// Timestamp of the last event matching the given step.
    pub fn step_last_time(&self, name: &str) -> u64 {
        self.step_meta
            .iter()
            .find(|s| s.name == name)
            .map_or(0, |s| s.last_time)
    }

    /// Raw serialized events for a step (only populated with `.retain_events()`).
    pub fn step_events_raw(&self, name: &str) -> &[Vec<u8>] {
        let Some(ref all_events) = self.buffered_events else {
            return &[];
        };
        self.step_meta
            .iter()
            .position(|s| s.name == name)
            .map_or(&[], |idx| &all_events[idx])
    }

    /// Deserialize buffered events for a step (only with `.retain_events()`).
    pub fn step_events<V: serde::de::DeserializeOwned>(&self, name: &str) -> Vec<V> {
        self.step_events_raw(name)
            .iter()
            .filter_map(|bytes| bincode::deserialize(bytes).ok())
            .collect()
    }
}

// ── Internal types ──────────────────────────────────────────────────

type PredicateFn<I> = Arc<dyn for<'a> Fn(<I as RheiSchema>::View<'a>) -> bool + Send + Sync>;
type CorrelateFn<I> = Arc<dyn for<'a> Fn(<I as RheiSchema>::View<'a>) -> String + Send + Sync>;

struct StepDef<I: RheiSchema> {
    name: String,
    predicate: PredicateFn<I>,
    quantifier: Quantifier,
}

impl<I: RheiSchema> Clone for StepDef<I> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            predicate: Arc::clone(&self.predicate),
            quantifier: self.quantifier.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct PartitionState {
    sequences: Vec<InFlightSequence>,
}

#[derive(Serialize, Deserialize, Clone)]
struct InFlightSequence {
    current_step: usize,
    current_step_count: usize,
    first_event_time: u64,
    last_event_time: u64,
    step_counts: Vec<usize>,
    step_times: Vec<(u64, u64)>,
    buffered_events: Option<Vec<Vec<Vec<u8>>>>,
}

struct PendingMatch {
    key: String,
    match_ctx: MatchCtx,
}

// ── Operator ────────────────────────────────────────────────────────

/// Sequence detection operator. Use [`SequenceDetectBuilder`] to construct.
pub struct SequenceDetect<I: RheiSchema, O, KF, TF, EF> {
    key_fn: KF,
    time_fn: TF,
    correlate_fn: Option<CorrelateFn<I>>,
    emit_fn: EF,
    steps: Vec<StepDef<I>>,
    within: u64,
    after_match: AfterMatch,
    retain_events: bool,
    max_in_flight: usize,
    // Runtime state
    last_watermark: u64,
    active_partitions: HashSet<String>,
    pending_matches: Vec<PendingMatch>,
    _phantom: PhantomData<fn(I) -> O>,
}

impl<I: RheiSchema, O, KF: Clone, TF: Clone, EF: Clone> Clone for SequenceDetect<I, O, KF, TF, EF> {
    fn clone(&self) -> Self {
        Self {
            key_fn: self.key_fn.clone(),
            time_fn: self.time_fn.clone(),
            correlate_fn: self.correlate_fn.clone(),
            emit_fn: self.emit_fn.clone(),
            steps: self.steps.clone(),
            within: self.within,
            after_match: self.after_match.clone(),
            retain_events: self.retain_events,
            max_in_flight: self.max_in_flight,
            last_watermark: 0,
            active_partitions: HashSet::new(),
            pending_matches: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

impl<I: RheiSchema, O, KF, TF, EF> fmt::Debug for SequenceDetect<I, O, KF, TF, EF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SequenceDetect")
            .field(
                "steps",
                &self.steps.iter().map(|s| &s.name).collect::<Vec<_>>(),
            )
            .field("within", &self.within)
            .field("after_match", &self.after_match)
            .field("retain_events", &self.retain_events)
            .finish_non_exhaustive()
    }
}

impl<I: RheiSchema, O, KF, TF, EF> SequenceDetect<I, O, KF, TF, EF> {
    /// Start building a new sequence detector.
    pub fn builder() -> SequenceDetectBuilder<I, O, KF, TF, EF> {
        SequenceDetectBuilder {
            key_fn: None,
            time_fn: None,
            correlate_fn: None,
            emit_fn: None,
            steps: Vec::new(),
            within: None,
            after_match: AfterMatch::default(),
            retain_events: false,
            max_in_flight: 1000,
            _phantom: PhantomData,
        }
    }
}

// ── Builder ─────────────────────────────────────────────────────────

/// Builder for [`SequenceDetect`].
pub struct SequenceDetectBuilder<I: RheiSchema, O, KF, TF, EF> {
    key_fn: Option<KF>,
    time_fn: Option<TF>,
    correlate_fn: Option<CorrelateFn<I>>,
    emit_fn: Option<EF>,
    steps: Vec<StepDef<I>>,
    within: Option<u64>,
    after_match: AfterMatch,
    retain_events: bool,
    max_in_flight: usize,
    _phantom: PhantomData<fn(I) -> O>,
}

impl<I: RheiSchema, O, KF, TF, EF> fmt::Debug for SequenceDetectBuilder<I, O, KF, TF, EF> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SequenceDetectBuilder")
            .field("steps", &self.steps.len())
            .field("within", &self.within)
            .finish_non_exhaustive()
    }
}

impl<I: RheiSchema, O, KF, TF, EF> SequenceDetectBuilder<I, O, KF, TF, EF> {
    /// Set the key extraction function (partition key for the output).
    pub fn key_fn(mut self, f: KF) -> Self {
        self.key_fn = Some(f);
        self
    }

    /// Set the timestamp extraction function.
    pub fn time_fn(mut self, f: TF) -> Self {
        self.time_fn = Some(f);
        self
    }

    /// Set a correlation function. Events in the same sequence must share the
    /// same correlation value. Internally, partition = key + correlation.
    pub fn correlate_by<CF>(mut self, f: CF) -> Self
    where
        CF: for<'a> Fn(I::View<'a>) -> String + Send + Sync + 'static,
    {
        self.correlate_fn = Some(Arc::new(f));
        self
    }

    /// Add a named step with a predicate. Steps are matched in order.
    pub fn step<PF>(mut self, name: &str, predicate: PF) -> Self
    where
        PF: for<'a> Fn(I::View<'a>) -> bool + Send + Sync + 'static,
    {
        self.steps.push(StepDef {
            name: name.to_string(),
            predicate: Arc::new(predicate),
            quantifier: Quantifier::default(),
        });
        self
    }

    /// Set the minimum repetitions for the last-added step (unbounded max).
    pub fn at_least(mut self, n: usize) -> Self {
        assert!(
            !self.steps.is_empty(),
            "at_least() requires a preceding step()"
        );
        let len = self.steps.len();
        self.steps[len - 1].quantifier.min = n;
        self.steps[len - 1].quantifier.max = None;
        self
    }

    /// Set the maximum repetitions for the last-added step.
    pub fn at_most(mut self, n: usize) -> Self {
        assert!(
            !self.steps.is_empty(),
            "at_most() requires a preceding step()"
        );
        let len = self.steps.len();
        self.steps[len - 1].quantifier.max = Some(n);
        self
    }

    /// Set the maximum time span for a sequence (first to last event).
    #[allow(clippy::cast_possible_truncation)]
    pub fn within(mut self, duration: Duration) -> Self {
        self.within = Some(duration.as_millis() as u64);
        self
    }

    /// Set the after-match strategy.
    pub fn after_match(mut self, strategy: AfterMatch) -> Self {
        self.after_match = strategy;
        self
    }

    /// Enable event buffering for access in the emit function.
    pub fn retain_events(mut self) -> Self {
        self.retain_events = true;
        self
    }

    /// Set the maximum number of in-flight sequences per partition (default: 1000).
    pub fn max_in_flight(mut self, n: usize) -> Self {
        self.max_in_flight = n;
        self
    }

    /// Set the emit function called when a match completes.
    pub fn emit(mut self, f: EF) -> Self {
        self.emit_fn = Some(f);
        self
    }

    /// Build the operator. Panics if required fields are missing.
    #[allow(clippy::unwrap_used)]
    pub fn build(self) -> SequenceDetect<I, O, KF, TF, EF> {
        assert!(self.key_fn.is_some(), "key_fn is required");
        assert!(self.time_fn.is_some(), "time_fn is required");
        assert!(self.emit_fn.is_some(), "emit() is required");
        assert!(!self.steps.is_empty(), "at least one step() is required");
        assert!(self.within.is_some(), "within() is required");

        SequenceDetect {
            key_fn: self.key_fn.unwrap(),
            time_fn: self.time_fn.unwrap(),
            correlate_fn: self.correlate_fn,
            emit_fn: self.emit_fn.unwrap(),
            steps: self.steps,
            within: self.within.unwrap(),
            after_match: self.after_match,
            retain_events: self.retain_events,
            max_in_flight: self.max_in_flight,
            last_watermark: 0,
            active_partitions: HashSet::new(),
            pending_matches: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

// ── StreamFunction impl ─────────────────────────────────────────────

#[async_trait]
impl<I, O, KF, TF, EF> StreamFunction for SequenceDetect<I, O, KF, TF, EF>
where
    I: RheiSchema,
    O: RheiSchema,
    KF: for<'a> Fn(I::View<'a>) -> String + Send + Sync,
    TF: for<'a> Fn(I::View<'a>) -> u64 + Send + Sync,
    EF: Fn(&str, &MatchCtx) -> O + Send + Sync,
{
    type Input = I;
    type Output = O;

    async fn process(
        &mut self,
        input: RheiBuffer<I>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<O>> {
        if input.is_empty() {
            return Ok(BufferOutput::None);
        }

        let batch = input.as_record_batch();

        let mut row_data: Vec<(String, Option<String>, u64, usize)> = input
            .iter()
            .enumerate_physical()
            .map(|(phys_idx, _)| {
                let view = I::view(batch, phys_idx);
                let key = (self.key_fn)(I::view(batch, phys_idx));
                let correlation = self.correlate_fn.as_ref().map(|cf| (cf)(view));
                let ts = (self.time_fn)(I::view(batch, phys_idx));
                (key, correlation, ts, phys_idx)
            })
            .collect();

        row_data.sort_by_key(|(_, _, ts, _)| *ts);

        for (key, correlation, timestamp, phys_idx) in &row_data {
            let partition_key = match correlation {
                Some(c) => format!("{key}\x1f{c}"),
                None => key.clone(),
            };

            self.active_partitions.insert(partition_key.clone());

            let mut partition: PartitionState = {
                let mut state = KeyedState::<String, PartitionState>::new(&mut ctx.state, "seq");
                state.get(&partition_key).await?.unwrap_or(PartitionState {
                    sequences: Vec::new(),
                })
            };

            partition
                .sequences
                .retain(|seq| timestamp.saturating_sub(seq.first_event_time) <= self.within);

            let (matched_indices, skip_past) =
                self.advance_existing(&mut partition, batch, *phys_idx, *timestamp, key);

            if skip_past {
                partition.sequences.clear();
            } else {
                for idx in matched_indices.into_iter().rev() {
                    partition.sequences.remove(idx);
                }
            }

            if !skip_past {
                self.try_start_sequence(&mut partition, batch, *phys_idx, *timestamp, key);
            }

            let mut state = KeyedState::<String, PartitionState>::new(&mut ctx.state, "seq");
            if partition.sequences.is_empty() {
                state.delete(&partition_key)?;
                self.active_partitions.remove(&partition_key);
            } else {
                state.put(&partition_key, &partition)?;
            }
        }

        Ok(BufferOutput::None)
    }

    async fn on_watermark(
        &mut self,
        watermark: u64,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<O>> {
        self.last_watermark = watermark;

        let partitions: Vec<String> = self.active_partitions.iter().cloned().collect();
        for partition_key in partitions {
            let mut partition: PartitionState = {
                let mut state = KeyedState::<String, PartitionState>::new(&mut ctx.state, "seq");
                let Some(p) = state.get(&partition_key).await? else {
                    self.active_partitions.remove(&partition_key);
                    continue;
                };
                p
            };

            partition
                .sequences
                .retain(|seq| watermark.saturating_sub(seq.first_event_time) <= self.within);

            let mut state = KeyedState::<String, PartitionState>::new(&mut ctx.state, "seq");
            if partition.sequences.is_empty() {
                state.delete(&partition_key)?;
                self.active_partitions.remove(&partition_key);
            } else {
                state.put(&partition_key, &partition)?;
            }
        }

        if self.pending_matches.is_empty() {
            return Ok(BufferOutput::None);
        }

        let matches = std::mem::take(&mut self.pending_matches);
        let mut builder = O::builder(matches.len());
        for m in &matches {
            builder.append((self.emit_fn)(&m.key, &m.match_ctx));
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(builder)))
    }
}

// ── Private helpers ─────────────────────────────────────────────────

impl<I, O, KF, TF, EF> SequenceDetect<I, O, KF, TF, EF>
where
    I: RheiSchema,
    O: RheiSchema,
    KF: for<'a> Fn(I::View<'a>) -> String + Send + Sync,
    TF: for<'a> Fn(I::View<'a>) -> u64 + Send + Sync,
    EF: Fn(&str, &MatchCtx) -> O + Send + Sync,
{
    fn advance_existing(
        &mut self,
        partition: &mut PartitionState,
        batch: &arrow_array::RecordBatch,
        phys_idx: usize,
        timestamp: u64,
        key: &str,
    ) -> (Vec<usize>, bool) {
        let mut matched_indices: Vec<usize> = Vec::new();
        let mut skip_past = false;

        for (seq_idx, seq) in partition.sequences.iter_mut().enumerate() {
            if self.advance_sequence(seq, batch, phys_idx, timestamp) {
                self.emit_match(key, seq);
                matched_indices.push(seq_idx);

                if matches!(self.after_match, AfterMatch::SkipPastMatch) {
                    skip_past = true;
                    break;
                }
            }
        }

        (matched_indices, skip_past)
    }

    fn try_start_sequence(
        &mut self,
        partition: &mut PartitionState,
        batch: &arrow_array::RecordBatch,
        phys_idx: usize,
        timestamp: u64,
        key: &str,
    ) {
        let step0 = &self.steps[0];
        let view = I::view(batch, phys_idx);
        if !(step0.predicate)(view) {
            return;
        }

        let num_steps = self.steps.len();
        let mut new_seq = InFlightSequence {
            current_step: 0,
            current_step_count: 1,
            first_event_time: timestamp,
            last_event_time: timestamp,
            step_counts: vec![0; num_steps],
            step_times: vec![(0, 0); num_steps],
            buffered_events: if self.retain_events {
                Some(vec![Vec::new(); num_steps])
            } else {
                None
            },
        };
        new_seq.step_counts[0] = 1;
        new_seq.step_times[0] = (timestamp, timestamp);

        Self::buffer_event(&mut new_seq, 0, self.retain_events);

        if step0.quantifier.is_satisfied(1) {
            if num_steps == 1 {
                self.emit_match(key, &new_seq);
                return;
            }
            if step0.quantifier.is_maxed(1) {
                new_seq.current_step = 1;
                new_seq.current_step_count = 0;
            }
        }

        partition.sequences.push(new_seq);

        while partition.sequences.len() > self.max_in_flight {
            partition.sequences.remove(0);
        }
    }

    fn advance_sequence(
        &self,
        seq: &mut InFlightSequence,
        batch: &arrow_array::RecordBatch,
        phys_idx: usize,
        timestamp: u64,
    ) -> bool {
        let current = seq.current_step;
        let is_last_step = current == self.steps.len() - 1;
        let step = &self.steps[current];

        let view = I::view(batch, phys_idx);
        let matches_current = (step.predicate)(view);

        if matches_current {
            self.handle_current_match(seq, current, is_last_step, batch, phys_idx, timestamp)
        } else if !is_last_step && step.quantifier.is_satisfied(seq.current_step_count) {
            self.try_advance_to_next(seq, current, batch, phys_idx, timestamp)
        } else {
            false
        }
    }

    fn handle_current_match(
        &self,
        seq: &mut InFlightSequence,
        current: usize,
        is_last_step: bool,
        batch: &arrow_array::RecordBatch,
        phys_idx: usize,
        timestamp: u64,
    ) -> bool {
        seq.current_step_count += 1;
        seq.last_event_time = timestamp;
        seq.step_counts[current] += 1;
        if seq.step_counts[current] == 1 {
            seq.step_times[current].0 = timestamp;
        }
        seq.step_times[current].1 = timestamp;

        Self::buffer_event(seq, current, self.retain_events);

        let step = &self.steps[current];

        if !is_last_step && step.quantifier.is_satisfied(seq.current_step_count) {
            let next_view = I::view(batch, phys_idx);
            if (self.steps[current + 1].predicate)(next_view) {
                seq.current_step = current + 1;
                seq.current_step_count = 1;
                seq.step_counts[current + 1] = 1;
                seq.step_times[current + 1] = (timestamp, timestamp);
                Self::buffer_event(seq, current + 1, self.retain_events);

                let new_is_last = seq.current_step == self.steps.len() - 1;
                return new_is_last
                    && self.steps[seq.current_step]
                        .quantifier
                        .is_satisfied(seq.current_step_count);
            }
        } else if !is_last_step && step.quantifier.is_maxed(seq.current_step_count) {
            seq.current_step = current + 1;
            seq.current_step_count = 0;
        } else if is_last_step && step.quantifier.is_satisfied(seq.current_step_count) {
            return true;
        }

        false
    }

    fn try_advance_to_next(
        &self,
        seq: &mut InFlightSequence,
        current: usize,
        batch: &arrow_array::RecordBatch,
        phys_idx: usize,
        timestamp: u64,
    ) -> bool {
        let next_view = I::view(batch, phys_idx);
        if !(self.steps[current + 1].predicate)(next_view) {
            return false;
        }

        seq.current_step = current + 1;
        seq.current_step_count = 1;
        seq.last_event_time = timestamp;
        seq.step_counts[current + 1] = 1;
        seq.step_times[current + 1] = (timestamp, timestamp);
        Self::buffer_event(seq, current + 1, self.retain_events);

        let new_is_last = seq.current_step == self.steps.len() - 1;
        new_is_last
            && self.steps[seq.current_step]
                .quantifier
                .is_satisfied(seq.current_step_count)
    }

    fn buffer_event(seq: &mut InFlightSequence, step_idx: usize, retain: bool) {
        if retain && let Some(ref mut buffers) = seq.buffered_events {
            buffers[step_idx].push(Vec::new());
        }
    }

    fn emit_match(&mut self, key: &str, seq: &InFlightSequence) {
        let step_meta: Vec<StepMeta> = self
            .steps
            .iter()
            .enumerate()
            .map(|(i, step)| StepMeta {
                name: step.name.clone(),
                count: seq.step_counts[i],
                first_time: seq.step_times[i].0,
                last_time: seq.step_times[i].1,
            })
            .collect();

        let match_ctx = MatchCtx {
            first_time: seq.first_event_time,
            last_time: seq.last_event_time,
            step_meta,
            buffered_events: seq.buffered_events.clone(),
        };

        self.pending_matches.push(PendingMatch {
            key: key.to_string(),
            match_ctx,
        });
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::arrow::RheiSchema as RheiSchemaTrait;
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;

    use arrow_array::builder::ArrayBuilder;

    // ── Test schema ─────────────────────────────────────────────────

    struct LoginEvent {
        user_id: String,
        action: String,
        ts: u64,
    }
    struct LoginEventBuilder {
        user_id: arrow_array::builder::StringBuilder,
        action: arrow_array::builder::StringBuilder,
        ts: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    }
    #[derive(Debug, PartialEq)]
    struct LoginEventView<'a> {
        user_id: &'a str,
        action: &'a str,
        ts: u64,
    }
    struct LoginEventCols<'a> {
        #[allow(dead_code)]
        user_id: &'a arrow_array::StringArray,
    }

    impl crate::arrow::RheiBuilder for LoginEventBuilder {
        type Item = LoginEvent;
        fn append(&mut self, item: LoginEvent) {
            self.user_id.append_value(&item.user_id);
            self.action.append_value(&item.action);
            self.ts.append_value(item.ts);
        }
        fn append_null(&mut self) {
            self.user_id.append_null();
            self.action.append_null();
            self.ts.append_null();
        }
        fn len(&self) -> usize {
            self.user_id.len()
        }
        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                LoginEvent::arrow_schema(),
                vec![
                    Arc::new(self.user_id.finish()),
                    Arc::new(self.action.finish()),
                    Arc::new(self.ts.finish()),
                ],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for LoginEvent {
        type Builder = LoginEventBuilder;
        type View<'a> = LoginEventView<'a>;
        type Columns<'a> = LoginEventCols<'a>;

        fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
            use std::sync::Arc;
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("user_id", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("action", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("ts", arrow_schema::DataType::UInt64, false),
            ]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            LoginEventBuilder {
                user_id: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                action: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                ts: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            }
        }

        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::UInt64Type;
            LoginEventView {
                user_id: batch.column(0).as_string::<i32>().value(index),
                action: batch.column(1).as_string::<i32>().value(index),
                ts: batch.column(2).as_primitive::<UInt64Type>().value(index),
            }
        }

        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            LoginEventCols {
                user_id: batch.column(0).as_string::<i32>(),
            }
        }
    }

    // ── Detection output schema ─────────────────────────────────────

    #[derive(Debug, PartialEq)]
    struct Detection {
        entity: String,
        matched_at: u64,
        step_count: usize,
    }
    struct DetectionBuilder {
        entity: arrow_array::builder::StringBuilder,
        matched_at: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
        step_count: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    }
    #[derive(Debug)]
    struct DetectionView<'a> {
        entity: &'a str,
        matched_at: u64,
        step_count: u64,
    }
    struct DetectionCols<'a> {
        #[allow(dead_code)]
        entity: &'a arrow_array::StringArray,
    }

    impl crate::arrow::RheiBuilder for DetectionBuilder {
        type Item = Detection;
        fn append(&mut self, item: Detection) {
            self.entity.append_value(&item.entity);
            self.matched_at.append_value(item.matched_at);
            self.step_count.append_value(item.step_count as u64);
        }
        fn append_null(&mut self) {
            self.entity.append_null();
            self.matched_at.append_null();
            self.step_count.append_null();
        }
        fn len(&self) -> usize {
            self.entity.len()
        }
        fn finish(mut self) -> arrow_array::RecordBatch {
            use std::sync::Arc;
            arrow_array::RecordBatch::try_new(
                Detection::arrow_schema(),
                vec![
                    Arc::new(self.entity.finish()),
                    Arc::new(self.matched_at.finish()),
                    Arc::new(self.step_count.finish()),
                ],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for Detection {
        type Builder = DetectionBuilder;
        type View<'a> = DetectionView<'a>;
        type Columns<'a> = DetectionCols<'a>;

        fn arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
            use std::sync::Arc;
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("entity", arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new("matched_at", arrow_schema::DataType::UInt64, false),
                arrow_schema::Field::new("step_count", arrow_schema::DataType::UInt64, false),
            ]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            DetectionBuilder {
                entity: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                matched_at: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
                step_count: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            }
        }

        fn view(batch: &arrow_array::RecordBatch, index: usize) -> Self::View<'_> {
            use arrow_array::cast::AsArray;
            use arrow_array::types::UInt64Type;
            DetectionView {
                entity: batch.column(0).as_string::<i32>().value(index),
                matched_at: batch.column(1).as_primitive::<UInt64Type>().value(index),
                step_count: batch.column(2).as_primitive::<UInt64Type>().value(index),
            }
        }

        fn columns(batch: &arrow_array::RecordBatch) -> Self::Columns<'_> {
            use arrow_array::cast::AsArray;
            DetectionCols {
                entity: batch.column(0).as_string::<i32>(),
            }
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────

    fn test_ctx(name: &str) -> OperatorContext {
        let path = std::env::temp_dir().join(format!(
            "rhei_seq_detect_test_{name}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let backend = LocalBackend::new(path, None).unwrap();
        OperatorContext::new(StateContext::new(Box::new(backend)))
    }

    fn make_events(events: &[(&str, &str, u64)]) -> RheiBuffer<LoginEvent> {
        let mut builder = LoginEvent::builder(events.len());
        for &(user_id, action, ts) in events {
            builder.append(LoginEvent {
                user_id: user_id.to_string(),
                action: action.to_string(),
                ts,
            });
        }
        RheiBuffer::from_builder(builder)
    }

    // ── Tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn simple_three_step_sequence() {
        let mut ctx = test_ctx("three_step");
        let mut detector: SequenceDetect<LoginEvent, Detection, _, _, _> =
            SequenceDetect::builder()
                .key_fn(|v: LoginEventView<'_>| v.user_id.to_string())
                .time_fn(|v: LoginEventView<'_>| v.ts)
                .step("read", |v: LoginEventView<'_>| v.action == "read")
                .step("write", |v: LoginEventView<'_>| v.action == "write")
                .step("rename", |v: LoginEventView<'_>| v.action == "rename")
                .within(Duration::from_mins(30))
                .emit(|key: &str, ctx: &MatchCtx| Detection {
                    entity: key.to_string(),
                    matched_at: ctx.last_time(),
                    step_count: 3,
                })
                .build();

        let input = make_events(&[
            ("alice", "read", 1000),
            ("alice", "write", 2000),
            ("alice", "rename", 3000),
        ]);
        let result = detector.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());

        let result = detector.on_watermark(5000, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
        let v = Detection::view(buf.as_record_batch(), 0);
        assert_eq!(v.entity, "alice");
        assert_eq!(v.matched_at, 3000);
    }

    #[tokio::test]
    async fn brute_force_with_quantifier() {
        let mut ctx = test_ctx("brute_force");
        let mut detector: SequenceDetect<LoginEvent, Detection, _, _, _> =
            SequenceDetect::builder()
                .key_fn(|v: LoginEventView<'_>| v.user_id.to_string())
                .time_fn(|v: LoginEventView<'_>| v.ts)
                .step("failed", |v: LoginEventView<'_>| v.action == "LoginFailed")
                .at_least(5)
                .step("success", |v: LoginEventView<'_>| {
                    v.action == "LoginSuccess"
                })
                .within(Duration::from_mins(30))
                .after_match(AfterMatch::SkipPastMatch)
                .emit(|key: &str, ctx: &MatchCtx| Detection {
                    entity: key.to_string(),
                    matched_at: ctx.last_time(),
                    step_count: ctx.step_count("failed"),
                })
                .build();

        let input = make_events(&[
            ("bob", "LoginFailed", 1000),
            ("bob", "LoginFailed", 2000),
            ("bob", "LoginFailed", 3000),
            ("bob", "LoginFailed", 4000),
            ("bob", "LoginFailed", 5000),
            ("bob", "LoginFailed", 6000),
            ("bob", "LoginFailed", 7000),
            ("bob", "LoginSuccess", 8000),
        ]);
        let result = detector.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());

        let result = detector.on_watermark(10000, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
        let v = Detection::view(buf.as_record_batch(), 0);
        assert_eq!(v.entity, "bob");
        assert_eq!(v.matched_at, 8000);
        assert_eq!(v.step_count, 7); // greedy: all 7 failures consumed
    }

    #[tokio::test]
    async fn insufficient_quantifier_no_match() {
        let mut ctx = test_ctx("insufficient");
        let mut detector: SequenceDetect<LoginEvent, Detection, _, _, _> =
            SequenceDetect::builder()
                .key_fn(|v: LoginEventView<'_>| v.user_id.to_string())
                .time_fn(|v: LoginEventView<'_>| v.ts)
                .step("failed", |v: LoginEventView<'_>| v.action == "LoginFailed")
                .at_least(5)
                .step("success", |v: LoginEventView<'_>| {
                    v.action == "LoginSuccess"
                })
                .within(Duration::from_mins(30))
                .emit(|key: &str, ctx: &MatchCtx| Detection {
                    entity: key.to_string(),
                    matched_at: ctx.last_time(),
                    step_count: ctx.step_count("failed"),
                })
                .build();

        let input = make_events(&[
            ("bob", "LoginFailed", 1000),
            ("bob", "LoginFailed", 2000),
            ("bob", "LoginFailed", 3000),
            ("bob", "LoginSuccess", 4000),
        ]);
        detector.process(input, &mut ctx).await.unwrap();
        let result = detector.on_watermark(10000, &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn within_timeout_expires_sequence() {
        let mut ctx = test_ctx("within_expire");
        let mut detector: SequenceDetect<LoginEvent, Detection, _, _, _> =
            SequenceDetect::builder()
                .key_fn(|v: LoginEventView<'_>| v.user_id.to_string())
                .time_fn(|v: LoginEventView<'_>| v.ts)
                .step("read", |v: LoginEventView<'_>| v.action == "read")
                .step("write", |v: LoginEventView<'_>| v.action == "write")
                .within(Duration::from_secs(10))
                .emit(|key: &str, ctx: &MatchCtx| Detection {
                    entity: key.to_string(),
                    matched_at: ctx.last_time(),
                    step_count: 2,
                })
                .build();

        let input = make_events(&[("alice", "read", 1000)]);
        detector.process(input, &mut ctx).await.unwrap();

        let input = make_events(&[("alice", "write", 20000)]);
        detector.process(input, &mut ctx).await.unwrap();

        let result = detector.on_watermark(25000, &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn overlapping_matches_skip_to_next_row() {
        let mut ctx = test_ctx("overlapping");
        let mut detector: SequenceDetect<LoginEvent, Detection, _, _, _> =
            SequenceDetect::builder()
                .key_fn(|v: LoginEventView<'_>| v.user_id.to_string())
                .time_fn(|v: LoginEventView<'_>| v.ts)
                .step("a", |v: LoginEventView<'_>| v.action == "a")
                .step("b", |v: LoginEventView<'_>| v.action == "b")
                .within(Duration::from_mins(1))
                .emit(|key: &str, ctx: &MatchCtx| Detection {
                    entity: key.to_string(),
                    matched_at: ctx.last_time(),
                    step_count: 1,
                })
                .build();

        let input = make_events(&[("x", "a", 1000), ("x", "a", 2000), ("x", "b", 3000)]);
        detector.process(input, &mut ctx).await.unwrap();

        let result = detector.on_watermark(5000, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 2);
    }

    #[tokio::test]
    async fn non_overlapping_skip_past_match() {
        let mut ctx = test_ctx("non_overlapping");
        let mut detector: SequenceDetect<LoginEvent, Detection, _, _, _> =
            SequenceDetect::builder()
                .key_fn(|v: LoginEventView<'_>| v.user_id.to_string())
                .time_fn(|v: LoginEventView<'_>| v.ts)
                .step("a", |v: LoginEventView<'_>| v.action == "a")
                .step("b", |v: LoginEventView<'_>| v.action == "b")
                .within(Duration::from_mins(1))
                .after_match(AfterMatch::SkipPastMatch)
                .emit(|key: &str, ctx: &MatchCtx| Detection {
                    entity: key.to_string(),
                    matched_at: ctx.last_time(),
                    step_count: 1,
                })
                .build();

        let input = make_events(&[("x", "a", 1000), ("x", "a", 2000), ("x", "b", 3000)]);
        detector.process(input, &mut ctx).await.unwrap();

        let result = detector.on_watermark(5000, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
    }

    #[tokio::test]
    async fn correlation_partitioning() {
        let mut ctx = test_ctx("correlation");
        let mut detector: SequenceDetect<LoginEvent, Detection, _, _, _> =
            SequenceDetect::builder()
                .key_fn(|v: LoginEventView<'_>| v.user_id.to_string())
                .time_fn(|v: LoginEventView<'_>| v.ts)
                .correlate_by(|v: LoginEventView<'_>| {
                    v.action.split(':').next().unwrap_or("").to_string()
                })
                .step("step1", |v: LoginEventView<'_>| v.action.ends_with(":read"))
                .step("step2", |v: LoginEventView<'_>| {
                    v.action.ends_with(":write")
                })
                .within(Duration::from_mins(1))
                .emit(|key: &str, ctx: &MatchCtx| Detection {
                    entity: key.to_string(),
                    matched_at: ctx.last_time(),
                    step_count: 2,
                })
                .build();

        let input = make_events(&[
            ("alice", "db1:read", 1000),
            ("alice", "db2:write", 2000),
            ("alice", "db1:write", 3000),
        ]);
        detector.process(input, &mut ctx).await.unwrap();

        let result = detector.on_watermark(5000, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
        let v = Detection::view(buf.as_record_batch(), 0);
        assert_eq!(v.entity, "alice");
        assert_eq!(v.matched_at, 3000);
    }

    #[tokio::test]
    async fn watermark_expires_stale_partitions() {
        let mut ctx = test_ctx("wm_expire");
        let mut detector: SequenceDetect<LoginEvent, Detection, _, _, _> =
            SequenceDetect::builder()
                .key_fn(|v: LoginEventView<'_>| v.user_id.to_string())
                .time_fn(|v: LoginEventView<'_>| v.ts)
                .step("a", |v: LoginEventView<'_>| v.action == "a")
                .step("b", |v: LoginEventView<'_>| v.action == "b")
                .within(Duration::from_secs(5))
                .emit(|key: &str, ctx: &MatchCtx| Detection {
                    entity: key.to_string(),
                    matched_at: ctx.last_time(),
                    step_count: 1,
                })
                .build();

        let input = make_events(&[("alice", "a", 1000)]);
        detector.process(input, &mut ctx).await.unwrap();
        assert_eq!(detector.active_partitions.len(), 1);

        let result = detector.on_watermark(7000, &mut ctx).await.unwrap();
        assert!(result.is_empty());
        assert_eq!(detector.active_partitions.len(), 0);
    }

    #[tokio::test]
    async fn match_ctx_provides_step_metadata() {
        let mut ctx = test_ctx("match_ctx");
        let mut detector: SequenceDetect<LoginEvent, Detection, _, _, _> =
            SequenceDetect::builder()
                .key_fn(|v: LoginEventView<'_>| v.user_id.to_string())
                .time_fn(|v: LoginEventView<'_>| v.ts)
                .step("failed", |v: LoginEventView<'_>| v.action == "LoginFailed")
                .at_least(2)
                .step("success", |v: LoginEventView<'_>| {
                    v.action == "LoginSuccess"
                })
                .within(Duration::from_mins(1))
                .after_match(AfterMatch::SkipPastMatch)
                .emit(|key: &str, ctx: &MatchCtx| {
                    assert_eq!(ctx.step_count("failed"), 3);
                    assert_eq!(ctx.step_count("success"), 1);
                    assert_eq!(ctx.first_time(), 1000);
                    assert_eq!(ctx.last_time(), 4000);
                    assert_eq!(ctx.duration(), 3000);
                    assert_eq!(ctx.step_first_time("failed"), 1000);
                    assert_eq!(ctx.step_last_time("failed"), 3000);
                    Detection {
                        entity: key.to_string(),
                        matched_at: ctx.last_time(),
                        step_count: ctx.step_count("failed"),
                    }
                })
                .build();

        let input = make_events(&[
            ("u1", "LoginFailed", 1000),
            ("u1", "LoginFailed", 2000),
            ("u1", "LoginFailed", 3000),
            ("u1", "LoginSuccess", 4000),
        ]);
        detector.process(input, &mut ctx).await.unwrap();
        let result = detector.on_watermark(5000, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 1);
        let v = Detection::view(buf.as_record_batch(), 0);
        assert_eq!(v.step_count, 3);
    }

    #[tokio::test]
    async fn multiple_keys_independent() {
        let mut ctx = test_ctx("multi_key");
        let mut detector: SequenceDetect<LoginEvent, Detection, _, _, _> =
            SequenceDetect::builder()
                .key_fn(|v: LoginEventView<'_>| v.user_id.to_string())
                .time_fn(|v: LoginEventView<'_>| v.ts)
                .step("a", |v: LoginEventView<'_>| v.action == "a")
                .step("b", |v: LoginEventView<'_>| v.action == "b")
                .within(Duration::from_mins(1))
                .emit(|key: &str, ctx: &MatchCtx| Detection {
                    entity: key.to_string(),
                    matched_at: ctx.last_time(),
                    step_count: 1,
                })
                .build();

        let input = make_events(&[
            ("alice", "a", 1000),
            ("bob", "a", 1500),
            ("alice", "b", 2000),
            ("bob", "b", 2500),
        ]);
        detector.process(input, &mut ctx).await.unwrap();

        let result = detector.on_watermark(5000, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 2);
    }
}
