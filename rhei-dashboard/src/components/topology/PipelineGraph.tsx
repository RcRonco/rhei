import { useMemo, useCallback } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { Topology } from "../../api/types";
import GraphNode from "./GraphNode";

const nodeTypes = { custom: GraphNode };

interface PipelineGraphProps {
  topology?: Topology;
}

function layoutNodes(topology: Topology): { nodes: Node[]; edges: Edge[] } {
  const nodeMap = new Map(topology.nodes.map((n) => [n.id, n]));

  const depths = new Map<number, number>();
  const childrenOf = new Map<number, number[]>();

  for (const node of topology.nodes) {
    childrenOf.set(node.id, []);
  }
  for (const [from, to] of topology.edges) {
    childrenOf.get(from)?.push(to);
  }

  const hasIncoming = new Set(topology.edges.map(([, to]) => to));
  const roots = topology.nodes.filter((n) => !hasIncoming.has(n.id)).map((n) => n.id);

  const queue = roots.map((id) => ({ id, depth: 0 }));
  while (queue.length > 0) {
    const { id, depth } = queue.shift()!;
    const current = depths.get(id) ?? -1;
    if (depth > current) {
      depths.set(id, depth);
      for (const child of childrenOf.get(id) ?? []) {
        queue.push({ id: child, depth: depth + 1 });
      }
    }
  }

  const byDepth = new Map<number, number[]>();
  for (const [id, depth] of depths) {
    const arr = byDepth.get(depth) ?? [];
    arr.push(id);
    byDepth.set(depth, arr);
  }

  const nodes: Node[] = [];
  for (const [depth, ids] of byDepth) {
    ids.forEach((id, i) => {
      const node = nodeMap.get(id);
      if (!node) return;
      nodes.push({
        id: String(id),
        type: "custom",
        position: {
          x: i * 220 - ((ids.length - 1) * 220) / 2 + 300,
          y: depth * 130 + 50,
        },
        data: { label: node.name, kind: node.kind },
      });
    });
  }

  const edges: Edge[] = topology.edges.map(([from, to]) => ({
    id: `${from}-${to}`,
    source: String(from),
    target: String(to),
    animated: true,
    style: { stroke: "#374151", strokeWidth: 2 },
  }));

  return { nodes, edges };
}

export default function PipelineGraph({ topology }: PipelineGraphProps) {
  const { initialNodes, initialEdges } = useMemo(() => {
    if (!topology) return { initialNodes: [], initialEdges: [] };
    const { nodes, edges } = layoutNodes(topology);
    return { initialNodes: nodes, initialEdges: edges };
  }, [topology]);

  const [nodes, , onNodesChange] = useNodesState(initialNodes);
  const [edges, , onEdgesChange] = useEdgesState(initialEdges);

  const onInit = useCallback(() => {}, []);

  if (!topology) {
    return (
      <div className="card h-[400px] flex items-center justify-center text-gray-600 text-sm">
        Loading topology...
      </div>
    );
  }

  return (
    <div className="card overflow-hidden h-[400px]">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onInit={onInit}
        nodeTypes={nodeTypes}
        fitView
        proOptions={{ hideAttribution: true }}
        className="bg-gray-950"
      >
        <Background color="#1f2937" gap={24} size={1} />
        <Controls
          className="!bg-gray-900 !border-gray-800 !rounded-lg !shadow-lg"
          showInteractive={false}
        />
      </ReactFlow>
    </div>
  );
}
