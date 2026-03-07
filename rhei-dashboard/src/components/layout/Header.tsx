import { Link } from "react-router-dom";
import { RiArrowRightSLine } from "@remixicon/react";
import type { PipelineInfo, HealthResponse } from "../../api/types";

interface HeaderProps {
  info?: PipelineInfo;
  health?: HealthResponse;
}

function formatUptime(secs: number): string {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = Math.floor(secs % 60);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    running: "bg-emerald-500/10 text-emerald-400 ring-emerald-500/20",
    starting: "bg-amber-500/10 text-amber-400 ring-amber-500/20",
    unknown: "bg-red-500/10 text-red-400 ring-red-500/20",
  };
  const cls = styles[status] ?? styles.unknown;

  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium ring-1 ${cls}`}
    >
      <span
        className={`w-1.5 h-1.5 rounded-full ${
          status === "running"
            ? "bg-emerald-400"
            : status === "starting"
              ? "bg-amber-400"
              : "bg-red-400"
        }`}
      />
      {status}
    </span>
  );
}

export default function Header({ info, health }: HeaderProps) {
  const status = health?.status ?? "unknown";

  return (
    <header className="h-12 bg-gray-900 border-b border-gray-800 flex items-center justify-between px-4 shrink-0">
      <div className="flex items-center gap-1.5 text-sm">
        <Link to="/" className="text-gray-500 hover:text-gray-300 transition-colors">
          Overview
        </Link>
        <RiArrowRightSLine size={14} className="text-gray-600" />
        <span className="font-medium text-white">{info?.name ?? "Pipeline"}</span>
        <div className="ml-2">
          <StatusBadge status={status} />
        </div>
      </div>
      <div className="flex items-center gap-4 text-xs text-gray-500">
        {info && (
          <>
            <span>{info.workers} worker{info.workers !== 1 ? "s" : ""}</span>
            <span className="text-gray-700">|</span>
            <span>v{info.version}</span>
            <span className="text-gray-700">|</span>
            <span>{formatUptime(info.uptime_secs)} uptime</span>
          </>
        )}
      </div>
    </header>
  );
}
