import { useRef, useEffect, useState, useMemo } from "react";
import { RiArrowUpSLine, RiArrowDownSLine } from "@remixicon/react";
import type { LogEntry } from "../../api/types";
import LogFilters from "./LogFilters";

interface LogViewerProps {
  logs?: LogEntry[];
}

const LEVEL_COLORS: Record<string, string> = {
  ERROR: "text-red-400",
  WARN: "text-amber-400",
  INFO: "text-blue-400",
  DEBUG: "text-gray-400",
  TRACE: "text-gray-600",
};

function formatTimestamp(ms: number): string {
  const d = new Date(ms);
  return `${d.getHours().toString().padStart(2, "0")}:${d.getMinutes().toString().padStart(2, "0")}:${d.getSeconds().toString().padStart(2, "0")}.${d.getMilliseconds().toString().padStart(3, "0")}`;
}

export default function LogViewer({ logs }: LogViewerProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const [expanded, setExpanded] = useState(false);
  const [levelFilter, setLevelFilter] = useState<Set<string>>(
    new Set(["ERROR", "WARN", "INFO", "DEBUG", "TRACE"]),
  );
  const [search, setSearch] = useState("");

  const filteredLogs = useMemo(() => {
    if (!logs) return [];
    return logs.filter((log) => {
      if (!levelFilter.has(log.level)) return false;
      if (search && !log.message.toLowerCase().includes(search.toLowerCase())) return false;
      return true;
    });
  }, [logs, levelFilter, search]);

  const errorWarnCount = useMemo(() => {
    if (!logs) return 0;
    return logs.filter((l) => l.level === "ERROR" || l.level === "WARN").length;
  }, [logs]);

  useEffect(() => {
    if (autoScroll && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [filteredLogs, autoScroll]);

  const handleScroll = () => {
    if (!containerRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = containerRef.current;
    setAutoScroll(scrollHeight - scrollTop - clientHeight < 50);
  };

  return (
    <div className="border-t border-gray-800 bg-gray-900 flex flex-col shrink-0">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center justify-between px-4 py-2 hover:bg-gray-800/50 transition-colors"
      >
        <div className="flex items-center gap-2">
          <h3 className="text-xs font-medium text-gray-400 uppercase tracking-wider">Logs</h3>
          {errorWarnCount > 0 && (
            <span className="px-1.5 py-0.5 text-[10px] font-medium bg-red-500/15 text-red-400 ring-1 ring-red-500/30 rounded-full">
              {errorWarnCount}
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          {expanded && (
            <div onClick={(e) => e.stopPropagation()}>
              <LogFilters
                levelFilter={levelFilter}
                onLevelFilterChange={setLevelFilter}
                search={search}
                onSearchChange={setSearch}
              />
            </div>
          )}
          {expanded ? (
            <RiArrowDownSLine size={16} className="text-gray-500" />
          ) : (
            <RiArrowUpSLine size={16} className="text-gray-500" />
          )}
        </div>
      </button>

      {expanded && (
        <div
          ref={containerRef}
          onScroll={handleScroll}
          className="h-60 overflow-y-auto px-4 pb-2 font-mono text-xs space-y-px"
        >
          {filteredLogs.map((log) => (
            <div key={log.seq} className="flex gap-2.5 hover:bg-gray-800/30 px-1.5 py-0.5 rounded">
              <span className="text-gray-600 shrink-0 tabular-nums">
                {formatTimestamp(log.timestamp_ms)}
              </span>
              <span
                className={`shrink-0 w-12 text-right font-medium ${LEVEL_COLORS[log.level] ?? "text-gray-400"}`}
              >
                {log.level}
              </span>
              {log.worker !== null && (
                <span className="text-gray-600 shrink-0">w{log.worker}</span>
              )}
              <span className="text-gray-300 break-all">{log.message}</span>
            </div>
          ))}
          {filteredLogs.length === 0 && (
            <div className="text-gray-600 text-center py-6">No logs</div>
          )}
        </div>
      )}
    </div>
  );
}
