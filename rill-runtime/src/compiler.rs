//! Graph compilation: converts a logical [`DataflowGraph`](crate::dataflow::DataflowGraph)
//! into executable [`CompiledPipeline`] segments.
//!
//! For V1, supports linear topologies: source → [transforms] → sink.
//! Fan-out and merge are detected and return an error.

use std::sync::Arc;

use crate::dataflow::{
    ErasedOperator, ErasedSink, ErasedSource, GraphNode, KeyFn, NodeKind, TransformFn,
};

// ── Compiled pipeline types ─────────────────────────────────────────

/// A segment in the compiled pipeline.
pub(crate) enum Segment {
    /// A stateless transform (`map`/`filter`/`flat_map`).
    Transform(TransformFn),
    /// A key-based exchange point.
    Exchange(KeyFn),
    /// A stateful operator with its name and type-erased implementation.
    Operator {
        name: String,
        op: Box<dyn ErasedOperator>,
    },
}

/// A compiled linear pipeline from source to sink.
pub(crate) struct CompiledPipeline {
    pub(crate) source: Box<dyn ErasedSource>,
    pub(crate) segments: Vec<Segment>,
    pub(crate) sink: Box<dyn ErasedSink>,
}

// ── Graph compilation ───────────────────────────────────────────────

/// Compile the dataflow graph into executable pipelines.
///
/// For V1, supports linear topologies: source → [transforms] → sink.
/// Fan-out and merge are detected and return an error.
pub(crate) fn compile(mut nodes: Vec<GraphNode>) -> anyhow::Result<Vec<CompiledPipeline>> {
    let sink_ids: Vec<_> = nodes
        .iter()
        .filter(|n| matches!(n.kind, NodeKind::Sink(_)))
        .map(|n| n.id)
        .collect();

    if sink_ids.is_empty() {
        anyhow::bail!("dataflow has no sinks — every stream must reach a sink");
    }

    let mut pipelines = Vec::new();

    for sink_id in &sink_ids {
        // Walk backwards from sink to source.
        let mut path = Vec::new();
        let mut current = *sink_id;
        loop {
            path.push(current);
            let node = &nodes[current.0];
            if node.inputs.is_empty() {
                break;
            }
            if node.inputs.len() > 1 {
                anyhow::bail!("merge nodes are not yet supported in the execution engine");
            }
            current = node.inputs[0];
        }
        path.reverse();

        if path.len() < 2 {
            anyhow::bail!("degenerate pipeline: fewer than 2 nodes");
        }
        if !matches!(nodes[path[0].0].kind, NodeKind::Source(_)) {
            anyhow::bail!(
                "pipeline does not start with a source (found node {:?})",
                path[0]
            );
        }

        // Extract middle segments.
        let mut segments = Vec::new();
        for &node_id in &path[1..path.len() - 1] {
            let kind = std::mem::replace(&mut nodes[node_id.0].kind, NodeKind::Merge);
            match kind {
                NodeKind::Transform(f) => segments.push(Segment::Transform(f)),
                NodeKind::KeyBy(f) => segments.push(Segment::Exchange(f)),
                NodeKind::Operator { name, op } => {
                    segments.push(Segment::Operator { name, op });
                }
                NodeKind::Source(_) => anyhow::bail!("unexpected source in middle of pipeline"),
                NodeKind::Sink(_) => anyhow::bail!("unexpected sink in middle of pipeline"),
                NodeKind::Merge => {
                    anyhow::bail!("merge nodes are not yet supported in the execution engine")
                }
            }
        }

        // Extract source and sink.
        let source_kind = std::mem::replace(&mut nodes[path[0].0].kind, NodeKind::Merge);
        let sink_kind = std::mem::replace(&mut nodes[path[path.len() - 1].0].kind, NodeKind::Merge);
        let NodeKind::Source(source) = source_kind else {
            anyhow::bail!("expected source node");
        };
        let NodeKind::Sink(sink) = sink_kind else {
            anyhow::bail!("expected sink node");
        };

        pipelines.push(CompiledPipeline {
            source,
            segments,
            sink,
        });
    }

    Ok(pipelines)
}

// ── Segment utilities ───────────────────────────────────────────────

/// Extract operator names from a compiled segment list.
pub(crate) fn operator_names(segments: &[Segment]) -> Vec<String> {
    segments
        .iter()
        .filter_map(|seg| match seg {
            Segment::Operator { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

/// Clone segments (Arc transforms are cheap, operators use `clone_erased`).
pub(crate) fn clone_segments(segments: &[Segment]) -> Vec<Segment> {
    segments
        .iter()
        .map(|seg| match seg {
            Segment::Transform(f) => Segment::Transform(Arc::clone(f)),
            Segment::Exchange(f) => Segment::Exchange(Arc::clone(f)),
            Segment::Operator { name, op } => Segment::Operator {
                name: name.clone(),
                op: op.clone_erased(),
            },
        })
        .collect()
}

/// Split a flat segment list at the first Exchange boundary.
///
/// Returns `(pre_segments, Option<(key_fn, post_segments)>)`.
pub(crate) fn split_at_first_exchange(
    segments: Vec<Segment>,
) -> (Vec<Segment>, Option<(KeyFn, Vec<Segment>)>) {
    let mut pre = Vec::new();
    let mut iter = segments.into_iter();
    for seg in iter.by_ref() {
        if let Segment::Exchange(key_fn) = seg {
            let post: Vec<Segment> = iter.collect();
            return (pre, Some((key_fn, post)));
        }
        pre.push(seg);
    }
    (pre, None)
}
