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
    /// A stateful operator (e.g. `TumblingWindow`, `TemporalJoin`).
    Operator(String),
    /// A data sink operator.
    Sink(String),
}

impl NodeKind {
    /// Returns the inner name regardless of variant.
    pub fn name(&self) -> &str {
        match self {
            NodeKind::Source(n)
            | NodeKind::Map(n)
            | NodeKind::Filter(n)
            | NodeKind::KeyBy(n)
            | NodeKind::Operator(n)
            | NodeKind::Sink(n) => n,
        }
    }
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

    /// Returns node indices in topological order (Kahn's algorithm).
    pub fn topological_order(&self) -> Vec<usize> {
        let n = self.nodes.len();
        let mut in_degree = vec![0usize; n];
        for successors in self.edges.values() {
            for &to in successors {
                in_degree[to] += 1;
            }
        }

        let mut queue: std::collections::VecDeque<usize> = in_degree
            .iter()
            .enumerate()
            .filter(|&(_, &d)| d == 0)
            .map(|(i, _)| i)
            .collect();

        let mut order = Vec::with_capacity(n);
        while let Some(node) = queue.pop_front() {
            order.push(node);
            if let Some(successors) = self.edges.get(&node) {
                for &next in successors {
                    in_degree[next] -= 1;
                    if in_degree[next] == 0 {
                        queue.push_back(next);
                    }
                }
            }
        }

        order
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

    /// Appends a stateful operator and connects it to the previous node.
    pub fn operator(mut self, name: impl Into<String>) -> Self {
        let id = self.plan.add_node(NodeKind::Operator(name.into()));
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
            .operator("op")
            .sink("sk")
            .build();

        assert!(matches!(plan.nodes[0].kind, NodeKind::Source(_)));
        assert!(matches!(plan.nodes[1].kind, NodeKind::Map(_)));
        assert!(matches!(plan.nodes[2].kind, NodeKind::Filter(_)));
        assert!(matches!(plan.nodes[3].kind, NodeKind::KeyBy(_)));
        assert!(matches!(plan.nodes[4].kind, NodeKind::Operator(_)));
        assert!(matches!(plan.nodes[5].kind, NodeKind::Sink(_)));
    }

    #[test]
    fn topological_order() {
        let plan = StreamGraph::new()
            .source("src")
            .operator("window")
            .sink("out")
            .build();

        let order = plan.topological_order();
        assert_eq!(order, vec![0, 1, 2]);
    }

    #[test]
    fn node_kind_name() {
        assert_eq!(NodeKind::Source("a".into()).name(), "a");
        assert_eq!(NodeKind::Map("b".into()).name(), "b");
        assert_eq!(NodeKind::Filter("c".into()).name(), "c");
        assert_eq!(NodeKind::KeyBy("d".into()).name(), "d");
        assert_eq!(NodeKind::Operator("e".into()).name(), "e");
        assert_eq!(NodeKind::Sink("f".into()).name(), "f");
    }
}
