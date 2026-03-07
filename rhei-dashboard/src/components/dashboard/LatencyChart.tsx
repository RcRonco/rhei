import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Legend,
} from "recharts";
import type { TimestampedSnapshot } from "../../api/types";

interface LatencyChartProps {
  history?: TimestampedSnapshot[];
}

function formatTime(ms: number): string {
  const d = new Date(ms);
  return `${d.getHours().toString().padStart(2, "0")}:${d.getMinutes().toString().padStart(2, "0")}:${d.getSeconds().toString().padStart(2, "0")}`;
}

export default function LatencyChart({ history }: LatencyChartProps) {
  const data = (history ?? []).map(([ts, snap]) => ({
    time: ts,
    p50: snap.element_duration_p50 * 1000,
    p99: snap.element_duration_p99 * 1000,
  }));

  return (
    <div className="card p-5">
      <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-4">
        Latency
      </h3>
      <div className="h-52">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data}>
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
              formatter={(value: number) => [`${value.toFixed(2)} ms`]}
            />
            <Legend
              iconType="plainline"
              wrapperStyle={{ fontSize: "0.75rem", color: "#9ca3af" }}
            />
            <Line
              type="monotone"
              dataKey="p50"
              name="p50"
              stroke="#22c55e"
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="p99"
              name="p99"
              stroke="#ef4444"
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
