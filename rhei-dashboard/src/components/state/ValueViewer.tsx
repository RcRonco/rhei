import { useState } from "react";

interface ValueViewerProps {
  keyName: string | null;
  decodings: Record<string, unknown> | null;
  sizeBytes: number;
}

const DECODE_TABS = ["json", "utf8", "hex"];

function syntaxHighlight(json: string): string {
  return json
    .replace(
      /("(\\u[\da-fA-F]{4}|\\[^u]|[^"\\])*")\s*:/g,
      '<span class="text-blue-400">$1</span>:',
    )
    .replace(
      /("(\\u[\da-fA-F]{4}|\\[^u]|[^"\\])*")/g,
      '<span class="text-emerald-400">$1</span>',
    )
    .replace(/\b(true|false)\b/g, '<span class="text-amber-400">$1</span>')
    .replace(/\b(null)\b/g, '<span class="text-gray-500">$1</span>')
    .replace(/\b(\d+\.?\d*)\b/g, '<span class="text-purple-400">$1</span>');
}

export default function ValueViewer({ keyName, decodings, sizeBytes }: ValueViewerProps) {
  const [activeTab, setActiveTab] = useState("json");

  if (!keyName || !decodings) {
    return (
      <div className="h-full flex items-center justify-center text-xs text-gray-600">
        Select a key to view its value
      </div>
    );
  }

  const availableTabs = DECODE_TABS.filter((t) => t in decodings);
  const currentTab = availableTabs.includes(activeTab) ? activeTab : availableTabs[0] ?? "hex";
  const value = decodings[currentTab];

  let displayContent: string;
  let isHighlighted = false;

  if (currentTab === "json" && value !== undefined) {
    displayContent = JSON.stringify(value, null, 2);
    isHighlighted = true;
  } else if (typeof value === "string") {
    displayContent = value;
  } else {
    displayContent = JSON.stringify(value);
  }

  return (
    <div className="flex flex-col h-full">
      {/* Tab bar + key info */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-gray-800">
        <div className="flex items-center gap-1">
          {availableTabs.map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-2 py-0.5 text-[11px] font-medium rounded-md transition-colors ${
                currentTab === tab
                  ? "bg-gray-800 text-white ring-1 ring-gray-700"
                  : "text-gray-500 hover:text-gray-300"
              }`}
            >
              {tab.toUpperCase()}
            </button>
          ))}
        </div>
        <span className="text-[11px] text-gray-600">{sizeBytes} bytes</span>
      </div>

      {/* Value display */}
      <div className="flex-1 overflow-auto p-3">
        {isHighlighted ? (
          <pre
            className="text-xs font-mono leading-relaxed whitespace-pre-wrap break-all"
            dangerouslySetInnerHTML={{ __html: syntaxHighlight(displayContent) }}
          />
        ) : (
          <pre className="text-xs font-mono text-gray-300 leading-relaxed whitespace-pre-wrap break-all">
            {displayContent}
          </pre>
        )}
      </div>
    </div>
  );
}
