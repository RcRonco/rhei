import { Handle, Position, type NodeProps } from "@xyflow/react";

const KIND_STYLES: Record<string, { border: string; bg: string; accent: string }> = {
  source: { border: "border-emerald-500/40", bg: "bg-emerald-500/5", accent: "bg-emerald-500" },
  transform: { border: "border-cyan-500/40", bg: "bg-cyan-500/5", accent: "bg-cyan-500" },
  operator: { border: "border-cyan-500/40", bg: "bg-cyan-500/5", accent: "bg-cyan-500" },
  filter: { border: "border-amber-500/40", bg: "bg-amber-500/5", accent: "bg-amber-500" },
  key_by: { border: "border-blue-500/40", bg: "bg-blue-500/5", accent: "bg-blue-500" },
  sink: { border: "border-fuchsia-500/40", bg: "bg-fuchsia-500/5", accent: "bg-fuchsia-500" },
  merge: { border: "border-purple-500/40", bg: "bg-purple-500/5", accent: "bg-purple-500" },
};

const KIND_LABELS: Record<string, string> = {
  source: "Source",
  transform: "Transform",
  operator: "Operator",
  filter: "Filter",
  key_by: "KeyBy",
  sink: "Sink",
  merge: "Merge",
};

interface GraphNodeData {
  label: string;
  kind: string;
  [key: string]: unknown;
}

export default function GraphNode({ data }: NodeProps & { data: GraphNodeData }) {
  const kind = data.kind ?? "transform";
  const style = KIND_STYLES[kind] ?? KIND_STYLES.transform;
  const kindLabel = KIND_LABELS[kind] ?? kind;

  return (
    <div
      className={`rounded-xl border ${style.border} ${style.bg} px-4 py-2.5 min-w-[140px] backdrop-blur-sm`}
    >
      <Handle type="target" position={Position.Top} className="!bg-gray-600 !w-2 !h-2 !border-0" />
      <div className="flex items-center gap-1.5">
        <span className={`w-1.5 h-1.5 rounded-full ${style.accent}`} />
        <span className="text-[10px] text-gray-500 uppercase tracking-wider font-medium">
          {kindLabel}
        </span>
      </div>
      <div className="text-sm font-medium text-white mt-1">{data.label}</div>
      <Handle
        type="source"
        position={Position.Bottom}
        className="!bg-gray-600 !w-2 !h-2 !border-0"
      />
    </div>
  );
}
