import {
  AreaChart,
  Area,
  ResponsiveContainer,
} from "recharts";
import type { MetricsSnapshot, TimestampedSnapshot } from "../../api/types";

interface HeroKPIsProps {
  metrics?: MetricsSnapshot;
  history?: TimestampedSnapshot[];
}

function formatRate(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toFixed(0);
}

interface HeroCardProps {
  label: string;
  value: string;
  unit?: string;
  sparkData: number[];
  sparkColor: string;
}

function Sparkline({ data, color }: { data: number[]; color: string }) {
  if (data.length < 2) return null;
  const chartData = data.map((v, i) => ({ i, v }));

  return (
    <div className="h-8 w-full mt-2">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={chartData} margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
          <defs>
            <linearGradient id={`spark-${color.replace('#', '')}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={0.3} />
              <stop offset="100%" stopColor={color} stopOpacity={0} />
            </linearGradient>
          </defs>
          <Area
            type="monotone"
            dataKey="v"
            stroke={color}
            strokeWidth={1.5}
            fill={`url(#spark-${color.replace('#', '')})`}
            dot={false}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

function HeroCard({ label, value, unit, sparkData, sparkColor }: HeroCardProps) {
  return (
    <div className="card p-4">
      <div className="text-xs font-medium text-gray-500 uppercase tracking-wider">{label}</div>
      <div className="mt-1 flex items-baseline gap-1.5">
        <span className="text-2xl font-semibold text-white font-mono">{value}</span>
        {unit && <span className="text-sm text-gray-500">{unit}</span>}
      </div>
      <Sparkline data={sparkData} color={sparkColor} />
    </div>
  );
}

export default function HeroKPIs({ metrics, history }: HeroKPIsProps) {
  if (!metrics) {
    return (
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="card p-4 h-[108px] animate-pulse" />
        ))}
      </div>
    );
  }

  const last60 = (history ?? []).slice(-60);
  const throughputData = last60.map(([, s]) => s.elements_per_second);
  const latencyData = last60.map(([, s]) => s.element_duration_p99 * 1000);
  const elementsData = last60.map(([, s]) => s.elements_total);
  const cacheData = last60.map(([, s]) => s.l1_hit_rate);

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
      <HeroCard
        label="Throughput"
        value={formatRate(metrics.elements_per_second)}
        unit="elem/s"
        sparkData={throughputData}
        sparkColor="#3b82f6"
      />
      <HeroCard
        label="p99 Latency"
        value={(metrics.element_duration_p99 * 1000).toFixed(2)}
        unit="ms"
        sparkData={latencyData}
        sparkColor="#ef4444"
      />
      <HeroCard
        label="Elements"
        value={metrics.elements_total.toLocaleString()}
        sparkData={elementsData}
        sparkColor="#22c55e"
      />
      <HeroCard
        label="L1 Cache"
        value={`${metrics.l1_hit_rate.toFixed(1)}%`}
        sparkData={cacheData}
        sparkColor="#a855f7"
      />
    </div>
  );
}
