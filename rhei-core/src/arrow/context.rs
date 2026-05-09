use crate::state::context::StateContext;

/// Context available to operators during batch processing.
///
/// Provides access to the existing state backend (for custom operators using
/// `KeyedState`) and operator-level metrics.
#[derive(Debug)]
pub struct OperatorContext {
    /// Operator-scoped state (memtable + backend). Use `KeyedState` for
    /// typed key-value access.
    pub state: StateContext,
    /// Operator-level metrics (rows processed, processing time, etc.).
    pub metrics: OperatorMetrics,
}

impl OperatorContext {
    /// Create a new operator context wrapping the given state.
    pub fn new(state: StateContext) -> Self {
        Self {
            state,
            metrics: OperatorMetrics::default(),
        }
    }
}

/// Metrics collected per operator during execution.
#[derive(Debug, Default)]
pub struct OperatorMetrics {
    /// Total number of input rows processed.
    pub rows_in: u64,
    /// Total number of output rows emitted.
    pub rows_out: u64,
}
