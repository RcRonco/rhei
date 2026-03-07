import type { MetricsSnapshot } from "../../api/types";

interface SecondaryMetricsProps {
  metrics?: MetricsSnapshot;
}

function MiniStat({ label, value, unit }: { label: string; value: string; unit?: string }) {
  return (
    <div className="card px-3 py-2.5">
      <div className="text-[11px] text-gray-500 uppercase tracking-wider">{label}</div>
      <div className="mt-0.5 text-sm font-mono text-white">
        {value}
        {unit && <span className="text-gray-500 ml-1 text-xs">{unit}</span>}
      </div>
    </div>
  );
}

export default function SecondaryMetrics({ metrics }: SecondaryMetricsProps) {
  if (!metrics) {
    return (
      <div className="grid grid-cols-2 gap-2">
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} className="card px-3 py-2.5 h-[52px] animate-pulse" />
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-2 gap-2">
      <MiniStat
        label="p50 Latency"
        value={(metrics.element_duration_p50 * 1000).toFixed(2)}
        unit="ms"
      />
      <MiniStat label="L2 Cache" value={`${metrics.l2_hit_rate.toFixed(1)}%`} />
      <MiniStat label="L3 Cache" value={`${metrics.l3_hit_rate.toFixed(1)}%`} />
      <MiniStat label="Stash Depth" value={metrics.stash_depth.toFixed(0)} />
      <MiniStat label="Pending" value={metrics.pending_futures.toFixed(0)} />
      <MiniStat
        label="Checkpoint"
        value={(metrics.checkpoint_duration_secs * 1000).toFixed(1)}
        unit="ms"
      />
    </div>
  );
}
