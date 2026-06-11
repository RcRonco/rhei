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
#[allow(dead_code)] // source_ids/sink_ids kept for diagnostic/checkpoint use
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
        .filter_map(|node| match &node.kind {
            NodeKind::BatchOperator { name, .. } => Some(name.clone()),
            _ => None,
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
            NodeKind::BatchOperator { name, .. } => ("operator", name.clone()),
            NodeKind::Sink(_) => ("sink", format!("Sink_{}", node.id.0)),
            NodeKind::KeyBy(_) => ("key_by", format!("KeyBy_{}", node.id.0)),
            NodeKind::Merge => ("merge", format!("Merge_{}", node.id.0)),
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::dataflow::{BatchOperatorNode, GraphNode, NodeId, NodeKind, SinkNode, SourceNode};
    use crate::erased_batch::{ErasedBatchOperator, ErasedSink, ErasedSource};

    // Dummy node payloads. `compile_graph` only inspects `node.kind` for
    // classification — it never calls `compile()` — so these bodies are
    // unreachable in the compilation tests.

    struct DummySource;
    impl SourceNode for DummySource {
        fn compile(self: Box<Self>) -> Box<dyn ErasedSource> {
            unreachable!("compile() is not exercised by compiler tests")
        }
    }

    struct DummySink;
    impl SinkNode for DummySink {
        fn compile(self: Box<Self>) -> Box<dyn ErasedSink> {
            unreachable!("compile() is not exercised by compiler tests")
        }
    }

    struct DummyOp;
    impl BatchOperatorNode for DummyOp {
        fn compile(self: Box<Self>) -> Box<dyn ErasedBatchOperator> {
            unreachable!("compile() is not exercised by compiler tests")
        }
    }

    fn source(id: usize) -> GraphNode {
        GraphNode {
            id: NodeId(id),
            kind: NodeKind::Source(Box::new(DummySource)),
            inputs: vec![],
            label: None,
        }
    }

    fn operator(id: usize, name: &str, inputs: &[usize]) -> GraphNode {
        GraphNode {
            id: NodeId(id),
            kind: NodeKind::BatchOperator {
                name: name.to_string(),
                op: Box::new(DummyOp),
            },
            inputs: inputs.iter().map(|&i| NodeId(i)).collect(),
            label: None,
        }
    }

    fn merge(id: usize, inputs: &[usize]) -> GraphNode {
        GraphNode {
            id: NodeId(id),
            kind: NodeKind::Merge,
            inputs: inputs.iter().map(|&i| NodeId(i)).collect(),
            label: None,
        }
    }

    fn sink(id: usize, inputs: &[usize]) -> GraphNode {
        GraphNode {
            id: NodeId(id),
            kind: NodeKind::Sink(Box::new(DummySink)),
            inputs: inputs.iter().map(|&i| NodeId(i)).collect(),
            label: None,
        }
    }

    /// A node appears before all of its successors in `topo_order`.
    fn assert_topologically_ordered(g: &CompiledGraph) {
        let position: std::collections::HashMap<usize, usize> = g
            .topo_order
            .iter()
            .enumerate()
            .map(|(pos, id)| (id.0, pos))
            .collect();
        for node in &g.nodes {
            for input in &node.inputs {
                assert!(
                    position[&input.0] < position[&node.id.0],
                    "input {} must come before {}",
                    input.0,
                    node.id.0
                );
            }
        }
    }

    // `CompiledGraph` is not `Debug`, so `unwrap_err()` is unavailable; pull
    // the error out of the `Result` explicitly instead.
    fn expect_err(result: anyhow::Result<CompiledGraph>) -> String {
        match result {
            Ok(_) => panic!("expected compilation to fail"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn empty_graph_is_rejected() {
        let err = expect_err(compile_graph(vec![]));
        assert!(err.contains("empty"), "got: {err}");
    }

    #[test]
    fn graph_without_source_is_rejected() {
        // A lone sink: no node is classified as a source.
        let err = expect_err(compile_graph(vec![sink(0, &[])]));
        assert!(err.contains("no sources"), "got: {err}");
    }

    #[test]
    fn graph_without_sink_is_rejected() {
        let err = expect_err(compile_graph(vec![source(0)]));
        assert!(err.contains("no sinks"), "got: {err}");
    }

    #[test]
    fn linear_pipeline_compiles_in_order() {
        // source(0) -> op(1) -> sink(2)
        let nodes = vec![source(0), operator(1, "map", &[0]), sink(2, &[1])];
        let g = compile_graph(nodes).unwrap();

        assert_eq!(
            g.topo_order.iter().map(|n| n.0).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(g.source_ids, vec![NodeId(0)]);
        assert_eq!(g.sink_ids, vec![NodeId(2)]);
        assert_eq!(g.operator_names, vec!["map".to_string()]);
        assert_topologically_ordered(&g);
    }

    #[test]
    fn operator_names_are_sorted() {
        // Two operators added out of alphabetical order must come back sorted,
        // which is what the checkpoint manifest validation relies on.
        let nodes = vec![
            source(0),
            operator(1, "zeta", &[0]),
            operator(2, "alpha", &[1]),
            sink(3, &[2]),
        ];
        let g = compile_graph(nodes).unwrap();
        assert_eq!(
            g.operator_names,
            vec!["alpha".to_string(), "zeta".to_string()]
        );
    }

    #[test]
    fn fan_out_and_merge_topology() {
        // source(0) fans out to op(1) and op(2), which merge(3) -> sink(4).
        let nodes = vec![
            source(0),
            operator(1, "left", &[0]),
            operator(2, "right", &[0]),
            merge(3, &[1, 2]),
            sink(4, &[3]),
        ];
        let g = compile_graph(nodes).unwrap();

        assert_eq!(g.topo_order.len(), 5);
        // Source must be first and sink last in any valid ordering.
        assert_eq!(g.topo_order.first(), Some(&NodeId(0)));
        assert_eq!(g.topo_order.last(), Some(&NodeId(4)));
        assert_topologically_ordered(&g);

        // Topology edges mirror the input wiring.
        let mut edges = g.topology.edges.clone();
        edges.sort_unstable();
        assert_eq!(edges, vec![(0, 1), (0, 2), (1, 3), (2, 3), (3, 4)]);

        // Merge node is tagged as such in the serialized topology.
        let merge_node = g.topology.nodes.iter().find(|n| n.id == 3).unwrap();
        assert_eq!(merge_node.kind, "merge");
    }

    #[test]
    fn cycle_is_detected() {
        // source(0) -> op(1) -> op(2) -> op(1) forms a 1<->2 cycle; sink(3)
        // keeps the source/sink validation happy so we reach the cycle check.
        let nodes = vec![
            source(0),
            operator(1, "a", &[0, 2]),
            operator(2, "b", &[1]),
            sink(3, &[2]),
        ];
        let err = expect_err(compile_graph(nodes));
        assert!(err.contains("cycle"), "got: {err}");
    }

    #[test]
    fn topology_tags_each_node_kind() {
        let nodes = vec![source(0), operator(1, "op", &[0]), sink(2, &[1])];
        let g = compile_graph(nodes).unwrap();

        let kinds: std::collections::HashMap<usize, String> = g
            .topology
            .nodes
            .iter()
            .map(|n| (n.id, n.kind.clone()))
            .collect();
        assert_eq!(kinds[&0], "source");
        assert_eq!(kinds[&1], "operator");
        assert_eq!(kinds[&2], "sink");
    }
}
