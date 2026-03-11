import { Link } from "react-router-dom";
import Sidebar from "../components/layout/Sidebar";
import { usePipelineStore, type Pipeline } from "../stores/pipelines";
import { useHealth, useMetrics, useInfo } from "../api/hooks";

function formatRate(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toFixed(0);
}

function PipelineCard({ pipeline }: { pipeline: Pipeline }) {
  const { data: health } = useHealth(pipeline.url);
  const { data: metrics } = useMetrics(pipeline.url);
  const { data: info } = useInfo(pipeline.url);

  const status = health?.status ?? "unknown";

  const borderTint =
    status === "running"
      ? "ring-emerald-500/20"
      : status === "starting"
        ? "ring-amber-500/20"
        : "ring-red-500/20";

  const dotColor =
    status === "running"
      ? "bg-emerald-500"
      : status === "starting"
        ? "bg-amber-500"
        : "bg-red-500";

  return (
    <Link
      to={`/pipeline/${pipeline.id}`}
      className={`card block p-5 ring-1 ${borderTint}`}
    >
      <div className="flex items-center gap-2 mb-4">
        <span className={`w-2 h-2 rounded-full ${dotColor}`} />
        <h3 className="font-medium text-white text-sm">{info?.name ?? pipeline.name}</h3>
      </div>

      <div className="mb-3">
        <div className="text-2xl font-semibold font-mono text-white">
          {metrics ? formatRate(metrics.elements_per_second) : "--"}
          <span className="text-sm font-normal text-gray-500 ml-1.5">elem/s</span>
        </div>
      </div>

      <div className="flex items-center gap-4 text-xs text-gray-500">
        <span>
          {metrics ? `${(metrics.element_duration_p99 * 1000).toFixed(1)}ms` : "--"}{" "}
          <span className="text-gray-600">p99</span>
        </span>
        <span>
          {info?.workers ?? "-"} worker{info?.workers !== 1 ? "s" : ""}
        </span>
        <span className="capitalize">{status}</span>
      </div>
    </Link>
  );
}

export default function Overview() {
  const pipelines = usePipelineStore((s) => s.pipelines);

  return (
    <div className="flex h-screen">
      <Sidebar />
      <main className="flex-1 overflow-y-auto bg-gray-950 p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-lg font-semibold text-white">Pipelines</h2>
          {pipelines.length > 0 && (
            <span className="text-xs text-gray-500">{pipelines.length} total</span>
          )}
        </div>

        {pipelines.length === 0 ? (
          <div className="text-gray-500 text-center py-20">
            <p className="text-base mb-1.5">No pipelines connected</p>
            <p className="text-sm text-gray-600">
              Add a pipeline URL in the sidebar to get started.
            </p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {pipelines.map((p) => (
              <PipelineCard key={p.id} pipeline={p} />
            ))}
          </div>
        )}
      </main>
    </div>
  );
}
