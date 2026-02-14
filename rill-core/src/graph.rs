use std::collections::HashMap;

/// Types of nodes in the logical plan.
#[derive(Debug, Clone)]
pub enum NodeKind {
    /// A data source operator.
    Source(String),
    /// A map (transform) operator.
    Map(String),
    /// A filter operator.
    Filter(String),
    /// A key-by (partitioning) operator.
    KeyBy(String),
    /// A data sink operator.
    Sink(String),
}

/// A node in the logical execution plan.
#[derive(Debug, Clone)]
pub struct PlanNode {
    /// Unique node identifier within the plan.
    pub id: usize,
    /// The operator kind for this node.
    pub kind: NodeKind,
}

/// The logical plan: nodes + directed edges (adjacency list).
#[derive(Debug, Clone)]
pub struct LogicalPlan {
    /// All nodes in the plan.
    pub nodes: Vec<PlanNode>,
    /// Directed edges as an adjacency list (from node ID to successor IDs).
    pub edges: HashMap<usize, Vec<usize>>,
}

impl LogicalPlan {
    /// Creates an empty logical plan.
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: HashMap::new(),
        }
    }

    fn add_node(&mut self, kind: NodeKind) -> usize {
        let id = self.nodes.len();
        self.nodes.push(PlanNode { id, kind });
        id
    }

    fn add_edge(&mut self, from: usize, to: usize) {
        self.edges.entry(from).or_default().push(to);
    }

    /// Returns the number of nodes in the plan.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the total number of directed edges in the plan.
    pub fn edge_count(&self) -> usize {
        self.edges.values().map(Vec::len).sum()
    }
}

impl Default for LogicalPlan {
    fn default() -> Self {
        Self::new()
    }
}

/// Fluent builder for constructing a `LogicalPlan`.
#[derive(Debug)]
pub struct StreamGraph {
    plan: LogicalPlan,
    last_node: Option<usize>,
}

impl StreamGraph {
    /// Creates a new empty `StreamGraph` builder.
    pub fn new() -> Self {
        Self {
            plan: LogicalPlan::new(),
            last_node: None,
        }
    }

    /// Appends a source node to the graph.
    pub fn source(mut self, name: impl Into<String>) -> Self {
        let id = self.plan.add_node(NodeKind::Source(name.into()));
        self.last_node = Some(id);
        self
    }

    /// Appends a map operator and connects it to the previous node.
    pub fn map(mut self, name: impl Into<String>) -> Self {
        let id = self.plan.add_node(NodeKind::Map(name.into()));
        if let Some(prev) = self.last_node {
            self.plan.add_edge(prev, id);
        }
        self.last_node = Some(id);
        self
    }

    /// Appends a filter operator and connects it to the previous node.
    pub fn filter(mut self, name: impl Into<String>) -> Self {
        let id = self.plan.add_node(NodeKind::Filter(name.into()));
        if let Some(prev) = self.last_node {
            self.plan.add_edge(prev, id);
        }
        self.last_node = Some(id);
        self
    }

    /// Appends a key-by operator and connects it to the previous node.
    pub fn key_by(mut self, name: impl Into<String>) -> Self {
        let id = self.plan.add_node(NodeKind::KeyBy(name.into()));
        if let Some(prev) = self.last_node {
            self.plan.add_edge(prev, id);
        }
        self.last_node = Some(id);
        self
    }

    /// Appends a sink node and connects it to the previous node.
    pub fn sink(mut self, name: impl Into<String>) -> Self {
        let id = self.plan.add_node(NodeKind::Sink(name.into()));
        if let Some(prev) = self.last_node {
            self.plan.add_edge(prev, id);
        }
        self.last_node = Some(id);
        self
    }

    /// Consumes the builder and returns the finished `LogicalPlan`.
    pub fn build(self) -> LogicalPlan {
        self.plan
    }
}

impl Default for StreamGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_linear_graph() {
        let plan = StreamGraph::new()
            .source("vec_source")
            .map("word_split")
            .key_by("by_word")
            .map("count")
            .sink("print_sink")
            .build();

        assert_eq!(plan.node_count(), 5);
        assert_eq!(plan.edge_count(), 4);

        // Verify edge connectivity: 0->1->2->3->4
        assert_eq!(plan.edges[&0], vec![1]);
        assert_eq!(plan.edges[&1], vec![2]);
        assert_eq!(plan.edges[&2], vec![3]);
        assert_eq!(plan.edges[&3], vec![4]);
    }

    #[test]
    fn source_only() {
        let plan = StreamGraph::new().source("src").build();
        assert_eq!(plan.node_count(), 1);
        assert_eq!(plan.edge_count(), 0);
    }

    #[test]
    fn node_kinds() {
        let plan = StreamGraph::new()
            .source("s")
            .map("m")
            .filter("f")
            .key_by("k")
            .sink("sk")
            .build();

        assert!(matches!(plan.nodes[0].kind, NodeKind::Source(_)));
        assert!(matches!(plan.nodes[1].kind, NodeKind::Map(_)));
        assert!(matches!(plan.nodes[2].kind, NodeKind::Filter(_)));
        assert!(matches!(plan.nodes[3].kind, NodeKind::KeyBy(_)));
        assert!(matches!(plan.nodes[4].kind, NodeKind::Sink(_)));
    }
}
