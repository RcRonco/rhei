//! Graph compilation: converts a logical [`DataflowGraph`](crate::dataflow::DataflowGraph)
//! into an executable [`CompiledGraph`] with topological ordering.
//!
//! Supports arbitrary DAG topologies: fan-out (one stream feeding multiple
//! downstream nodes), merge (multiple inputs), and multiple exchanges.

use std::collections::VecDeque;

use crate::dataflow::{GraphNode, NodeId, NodeKind};

// ── Compiled graph ──────────────────────────────────────────────────

/// A compiled DAG ready for execution.
pub(crate) struct CompiledGraph {
    /// Original nodes, indexed by `NodeId`.
    pub nodes: Vec<GraphNode>,
    /// Node IDs in topological order (sources first, sinks last).
    pub topo_order: Vec<NodeId>,
    /// Source node IDs.
    pub source_ids: Vec<NodeId>,
    /// Sink node IDs.
    pub sink_ids: Vec<NodeId>,
    /// Sorted operator names for checkpoint manifest validation.
    pub operator_names: Vec<String>,
}

// ── DAG compilation ─────────────────────────────────────────────────

/// Compile a dataflow graph into an executable DAG with topological ordering.
///
/// Validates the graph structure:
/// - At least one source (node with no inputs)
/// - At least one sink
/// - No cycles (detected via Kahn's algorithm)
pub(crate) fn compile_graph(nodes: Vec<GraphNode>) -> anyhow::Result<CompiledGraph> {
    if nodes.is_empty() {
        anyhow::bail!("dataflow graph is empty");
    }

    // Classify nodes.
    let source_ids: Vec<NodeId> = nodes
        .iter()
        .filter(|n| matches!(n.kind, NodeKind::Source(_)))
        .map(|n| n.id)
        .collect();

    let sink_ids: Vec<NodeId> = nodes
        .iter()
        .filter(|n| matches!(n.kind, NodeKind::Sink(_)))
        .map(|n| n.id)
        .collect();

    if source_ids.is_empty() {
        anyhow::bail!("dataflow graph has no sources");
    }
    if sink_ids.is_empty() {
        anyhow::bail!("dataflow has no sinks — every stream must reach a sink");
    }

    // Compute successor (output) edges and in-degrees.
    let n = nodes.len();
    let mut successors: Vec<Vec<NodeId>> = vec![vec![]; n];
    let mut in_degree: Vec<usize> = vec![0; n];

    for node in &nodes {
        in_degree[node.id.0] = node.inputs.len();
        for &input_id in &node.inputs {
            successors[input_id.0].push(node.id);
        }
    }

    // Kahn's algorithm for topological sort.
    let mut queue: VecDeque<NodeId> = VecDeque::new();
    for (i, &deg) in in_degree.iter().enumerate() {
        if deg == 0 {
            queue.push_back(NodeId(i));
        }
    }

    let mut topo_order: Vec<NodeId> = Vec::with_capacity(n);
    while let Some(node_id) = queue.pop_front() {
        topo_order.push(node_id);
        for &succ in &successors[node_id.0] {
            in_degree[succ.0] -= 1;
            if in_degree[succ.0] == 0 {
                queue.push_back(succ);
            }
        }
    }

    if topo_order.len() != n {
        anyhow::bail!(
            "cycle detected in dataflow graph (sorted {} of {} nodes)",
            topo_order.len(),
            n
        );
    }

    // Extract sorted operator names.
    let mut operator_names: Vec<String> = nodes
        .iter()
        .filter_map(|node| {
            if let NodeKind::Operator { name, .. } = &node.kind {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();
    operator_names.sort();

    Ok(CompiledGraph {
        nodes,
        topo_order,
        source_ids,
        sink_ids,
        operator_names,
    })
}
