import { useState } from "react";
import { Link, useLocation } from "react-router-dom";
import {
  RiDashboardLine,
  RiMenuFoldLine,
  RiMenuUnfoldLine,
  RiAddLine,
  RiDeleteBinLine,
} from "@remixicon/react";
import { usePipelineStore, type Pipeline } from "../../stores/pipelines";
import { useHealth } from "../../api/hooks";

function ConnectionDot({ baseUrl }: { baseUrl: string }) {
  const { data, isError, isLoading } = useHealth(baseUrl);

  const color = isLoading
    ? "bg-amber-500"
    : isError
      ? "bg-red-500"
      : data?.status === "running"
        ? "bg-emerald-500"
        : "bg-amber-500";

  return (
    <span className={`relative inline-block w-2 h-2 rounded-full shrink-0 ${color}`}>
      {!isLoading && !isError && data?.status === "running" && (
        <span className={`absolute inset-0 rounded-full ${color} animate-ping opacity-40`} />
      )}
    </span>
  );
}

function PipelineItem({
  pipeline,
  collapsed,
}: {
  pipeline: Pipeline;
  collapsed: boolean;
}) {
  const location = useLocation();
  const removePipeline = usePipelineStore((s) => s.removePipeline);
  const isActive = location.pathname === `/pipeline/${pipeline.id}`;

  return (
    <div
      className={`group relative flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm transition-colors ${
        isActive
          ? "bg-gray-800 text-white"
          : "text-gray-400 hover:bg-gray-800/50 hover:text-gray-200"
      }`}
    >
      {isActive && (
        <div className="absolute left-0 top-1/2 -translate-y-1/2 w-0.5 h-4 bg-blue-500 rounded-r" />
      )}
      <ConnectionDot baseUrl={pipeline.url} />
      {!collapsed && (
        <>
          <Link to={`/pipeline/${pipeline.id}`} className="flex-1 truncate">
            {pipeline.name}
          </Link>
          <button
            onClick={() => removePipeline(pipeline.id)}
            className="opacity-0 group-hover:opacity-100 text-gray-500 hover:text-red-400 transition-opacity"
            title="Remove pipeline"
          >
            <RiDeleteBinLine size={14} />
          </button>
        </>
      )}
      {collapsed && (
        <Link
          to={`/pipeline/${pipeline.id}`}
          className="absolute inset-0"
          title={pipeline.name}
        />
      )}
    </div>
  );
}

export default function Sidebar() {
  const pipelines = usePipelineStore((s) => s.pipelines);
  const addPipeline = usePipelineStore((s) => s.addPipeline);
  const [url, setUrl] = useState("");
  const [collapsed, setCollapsed] = useState(false);

  const handleAdd = () => {
    if (!url.trim()) return;
    const normalized = url.startsWith("http") ? url : `http://${url}`;
    addPipeline(normalized);
    setUrl("");
  };

  return (
    <aside
      className={`${
        collapsed ? "w-14" : "w-60"
      } bg-gray-900 border-r border-gray-800 flex flex-col h-full transition-all duration-200 shrink-0`}
    >
      <div className="flex items-center justify-between p-3 border-b border-gray-800">
        {!collapsed && (
          <Link to="/" className="text-base font-bold text-white tracking-wide">
            Rhei
          </Link>
        )}
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="p-1 text-gray-500 hover:text-gray-300 transition-colors"
          title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          {collapsed ? <RiMenuUnfoldLine size={18} /> : <RiMenuFoldLine size={18} />}
        </button>
      </div>

      <nav className="flex-1 overflow-y-auto p-2 space-y-1">
        <Link
          to="/"
          className="flex items-center gap-2.5 px-3 py-2 text-sm text-gray-400 hover:text-gray-200 hover:bg-gray-800/50 rounded-lg transition-colors"
          title="Overview"
        >
          <RiDashboardLine size={16} className="shrink-0" />
          {!collapsed && <span>Overview</span>}
        </Link>

        {!collapsed && (
          <div className="px-3 pt-4 pb-1 text-[11px] font-semibold text-gray-600 uppercase tracking-wider">
            Pipelines
          </div>
        )}

        {pipelines.map((p) => (
          <PipelineItem key={p.id} pipeline={p} collapsed={collapsed} />
        ))}

        {!collapsed && pipelines.length === 0 && (
          <p className="px-3 text-xs text-gray-600">No pipelines added</p>
        )}
      </nav>

      <div className="p-2 border-t border-gray-800">
        {collapsed ? (
          <button
            onClick={() => setCollapsed(false)}
            className="w-full flex items-center justify-center p-2 text-gray-500 hover:text-gray-300 transition-colors"
            title="Add pipeline"
          >
            <RiAddLine size={16} />
          </button>
        ) : (
          <div className="flex gap-1.5">
            <input
              type="text"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleAdd()}
              placeholder="host:port"
              className="flex-1 bg-gray-800 ring-1 ring-gray-700 rounded-lg px-2.5 py-1.5 text-sm text-white placeholder-gray-600 focus:outline-none focus:ring-blue-500 transition-shadow"
            />
            <button
              onClick={handleAdd}
              className="px-2.5 py-1.5 bg-blue-600 hover:bg-blue-500 rounded-lg text-sm text-white transition-colors"
            >
              Add
            </button>
          </div>
        )}
      </div>
    </aside>
  );
}
