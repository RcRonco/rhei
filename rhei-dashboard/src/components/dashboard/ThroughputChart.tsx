import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import type { TimestampedSnapshot } from "../../api/types";

interface ThroughputChartProps {
  history?: TimestampedSnapshot[];
}

function formatTime(ms: number): string {
  const d = new Date(ms);
  return `${d.getHours().toString().padStart(2, "0")}:${d.getMinutes().toString().padStart(2, "0")}:${d.getSeconds().toString().padStart(2, "0")}`;
}

export default function ThroughputChart({ history }: ThroughputChartProps) {
  const data = (history ?? []).map(([ts, snap]) => ({
    time: ts,
    throughput: snap.elements_per_second,
  }));

  return (
    <div className="card p-5">
      <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-4">
        Throughput
      </h3>
      <div className="h-52">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data}>
            <defs>
              <linearGradient id="throughputGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.2} />
                <stop offset="100%" stopColor="#3b82f6" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" vertical={false} />
            <XAxis
              dataKey="time"
              tickFormatter={formatTime}
              stroke="#374151"
              tick={{ fontSize: 10, fill: "#6b7280" }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              stroke="#374151"
              tick={{ fontSize: 10, fill: "#6b7280" }}
              axisLine={false}
              tickLine={false}
              width={40}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "#111827",
                border: "1px solid #1f2937",
                borderRadius: "0.5rem",
                boxShadow: "0 10px 25px rgba(0,0,0,0.4)",
              }}
              labelFormatter={formatTime}
              labelStyle={{ color: "#9ca3af", fontSize: "0.75rem" }}
              itemStyle={{ color: "#d1d5db", fontSize: "0.75rem" }}
            />
            <Area
              type="monotone"
              dataKey="throughput"
              name="elem/s"
              stroke="#3b82f6"
              strokeWidth={2}
              fill="url(#throughputGrad)"
              dot={false}
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
