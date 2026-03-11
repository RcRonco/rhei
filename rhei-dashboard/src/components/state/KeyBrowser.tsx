import { RiSearchLine } from "@remixicon/react";
import type { StateEntry } from "../../api/types";

interface KeyBrowserProps {
  entries: StateEntry[];
  total: number;
  selectedKey: string | null;
  onSelectKey: (key: string) => void;
  search: string;
  onSearchChange: (search: string) => void;
  decode: string;
  onDecodeChange: (decode: string) => void;
  onLoadMore: () => void;
  hasMore: boolean;
}

const DECODE_OPTIONS = [
  { value: "json", label: "JSON" },
  { value: "utf8", label: "UTF-8" },
  { value: "hex", label: "Hex" },
];

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
}

export default function KeyBrowser({
  entries,
  total,
  selectedKey,
  onSelectKey,
  search,
  onSearchChange,
  decode,
  onDecodeChange,
  onLoadMore,
  hasMore,
}: KeyBrowserProps) {
  return (
    <div className="flex flex-col h-full">
      {/* Search + decode bar */}
      <div className="flex items-center gap-2 px-3 py-2 border-b border-gray-800">
        <div className="flex-1 relative">
          <RiSearchLine
            size={14}
            className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-600"
          />
          <input
            type="text"
            value={search}
            onChange={(e) => onSearchChange(e.target.value)}
            placeholder="Search keys (prefix or /regex/)..."
            className="w-full bg-gray-800 ring-1 ring-gray-700 rounded-lg pl-8 pr-2.5 py-1.5 text-xs text-white placeholder-gray-600 focus:outline-none focus:ring-blue-500 transition-shadow"
          />
        </div>
        <select
          value={decode}
          onChange={(e) => onDecodeChange(e.target.value)}
          className="bg-gray-800 ring-1 ring-gray-700 rounded-lg px-2 py-1.5 text-xs text-white focus:outline-none focus:ring-blue-500"
        >
          {DECODE_OPTIONS.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>
        <span className="text-[11px] text-gray-600 shrink-0">
          {total.toLocaleString()} key{total !== 1 ? "s" : ""}
        </span>
      </div>

      {/* Key list */}
      <div className="flex-1 overflow-y-auto">
        {entries.map((entry) => (
          <button
            key={entry.key}
            onClick={() => onSelectKey(entry.key)}
            className={`w-full text-left px-3 py-1.5 flex items-center justify-between text-xs transition-colors ${
              selectedKey === entry.key
                ? "bg-gray-800 text-white"
                : "text-gray-400 hover:bg-gray-800/50 hover:text-gray-200"
            }`}
          >
            <span className="font-mono truncate">{entry.key}</span>
            <span className="text-gray-600 shrink-0 ml-2">{formatSize(entry.size_bytes)}</span>
          </button>
        ))}
        {entries.length === 0 && (
          <p className="px-3 py-6 text-xs text-gray-600 text-center">No entries</p>
        )}
        {hasMore && (
          <button
            onClick={onLoadMore}
            className="w-full px-3 py-2 text-xs text-blue-400 hover:text-blue-300 transition-colors"
          >
            Load more...
          </button>
        )}
      </div>
    </div>
  );
}
