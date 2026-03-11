import type { OperatorInfo } from "../../api/types";

interface OperatorListProps {
  operators: OperatorInfo[];
  selected: string | null;
  onSelect: (name: string) => void;
  checkpointId: number;
  timestampMs: number;
}

function formatAge(timestampMs: number): string {
  const ageMs = Date.now() - timestampMs;
  if (ageMs < 1000) return "just now";
  const ageSecs = Math.floor(ageMs / 1000);
  if (ageSecs < 60) return `${ageSecs}s ago`;
  const ageMins = Math.floor(ageSecs / 60);
  return `${ageMins}m ago`;
}

export default function OperatorList({
  operators,
  selected,
  onSelect,
  checkpointId,
  timestampMs,
}: OperatorListProps) {
  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-2 border-b border-gray-800">
        <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wider">Operators</h3>
      </div>
      <div className="flex-1 overflow-y-auto p-2 space-y-0.5">
        {operators.map((op) => (
          <button
            key={op.name}
            onClick={() => onSelect(op.name)}
            className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-colors relative ${
              selected === op.name
                ? "bg-gray-800 text-white"
                : "text-gray-400 hover:bg-gray-800/50 hover:text-gray-200"
            }`}
          >
            {selected === op.name && (
              <div className="absolute left-0 top-1/2 -translate-y-1/2 w-0.5 h-4 bg-blue-500 rounded-r" />
            )}
            <div className="truncate font-medium">{op.name}</div>
            <div className="text-xs text-gray-600 mt-0.5">
              {op.entry_count.toLocaleString()} key{op.entry_count !== 1 ? "s" : ""}
            </div>
          </button>
        ))}
        {operators.length === 0 && (
          <p className="px-3 py-4 text-xs text-gray-600 text-center">No operators</p>
        )}
      </div>
      <div className="px-3 py-2 border-t border-gray-800 text-[11px] text-gray-600">
        Checkpoint #{checkpointId} · {formatAge(timestampMs)}
      </div>
    </div>
  );
}
