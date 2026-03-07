//! Graph compilation: converts a logical [`DataflowGraph`](crate::dataflow::DataflowGraph)
//! into an executable [`CompiledGraph`] with topological ordering.
//!
//! Supports arbitrary DAG topologies: fan-out (one stream feeding multiple
//! downstream nodes), merge (multiple inputs), and multiple exchanges.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

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
    /// Serializable topology snapshot for the web dashboard API.
    pub topology: ApiTopology,
}

// ── Serializable topology ───────────────────────────────────────────

/// A node in the serializable topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiTopologyNode {
    /// Node index.
    pub id: usize,
    /// Node kind tag: `"source"`, `"transform"`, `"key_by"`, `"operator"`, `"merge"`, `"sink"`.
    pub kind: String,
    /// Human-readable name.
    pub name: String,
}

/// Serializable DAG topology for the web dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiTopology {
    /// Nodes in the graph.
    pub nodes: Vec<ApiTopologyNode>,
    /// Directed edges as `(from_id, to_id)`.
    pub edges: Vec<(usize, usize)>,
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

    // Extract serializable topology before nodes are consumed by execution.
    let topology = extract_topology(&nodes);

    Ok(CompiledGraph {
        nodes,
        topo_order,
        source_ids,
        sink_ids,
        operator_names,
        topology,
    })
}

/// Extract a serializable topology from the graph nodes.
fn extract_topology(nodes: &[GraphNode]) -> ApiTopology {
    let mut api_nodes = Vec::with_capacity(nodes.len());
    let mut edges = Vec::new();

    for node in nodes {
        let (kind, name) = match &node.kind {
            NodeKind::Source(_) => ("source", format!("Source_{}", node.id.0)),
            NodeKind::Transform(_) => ("transform", format!("Transform_{}", node.id.0)),
            NodeKind::KeyBy(_) => ("key_by", format!("KeyBy_{}", node.id.0)),
            NodeKind::Operator { name, .. } => ("operator", name.clone()),
            NodeKind::Merge => ("merge", format!("Merge_{}", node.id.0)),
            NodeKind::Sink(_) => ("sink", format!("Sink_{}", node.id.0)),
        };

        api_nodes.push(ApiTopologyNode {
            id: node.id.0,
            kind: kind.to_string(),
            name,
        });

        for input in &node.inputs {
            edges.push((input.0, node.id.0));
        }
    }

    ApiTopology {
        nodes: api_nodes,
        edges,
    }
}
