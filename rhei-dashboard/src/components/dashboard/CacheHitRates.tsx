import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Legend,
} from "recharts";
import type { TimestampedSnapshot } from "../../api/types";

interface CacheHitRatesProps {
  history?: TimestampedSnapshot[];
}

function formatTime(ms: number): string {
  const d = new Date(ms);
  return `${d.getHours().toString().padStart(2, "0")}:${d.getMinutes().toString().padStart(2, "0")}:${d.getSeconds().toString().padStart(2, "0")}`;
}

export default function CacheHitRates({ history }: CacheHitRatesProps) {
  const data = (history ?? []).map(([ts, snap]) => ({
    time: ts,
    L1: snap.l1_hit_rate,
    L2: snap.l2_hit_rate,
    L3: snap.l3_hit_rate,
  }));

  return (
    <div className="card p-5">
      <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-4">
        Cache Hit Rates
      </h3>
      <div className="h-52">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data}>
            <defs>
              <linearGradient id="cacheL1" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#22c55e" stopOpacity={0.3} />
                <stop offset="100%" stopColor="#22c55e" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="cacheL2" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.3} />
                <stop offset="100%" stopColor="#3b82f6" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="cacheL3" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#eab308" stopOpacity={0.3} />
                <stop offset="100%" stopColor="#eab308" stopOpacity={0} />
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
              domain={[0, 100]}
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
              formatter={(value: number) => [`${value.toFixed(1)}%`]}
            />
            <Legend
              iconType="plainline"
              wrapperStyle={{ fontSize: "0.75rem", color: "#9ca3af" }}
            />
            <Area
              type="monotone"
              dataKey="L1"
              stroke="#22c55e"
              strokeWidth={2}
              fill="url(#cacheL1)"
              dot={false}
              isAnimationActive={false}
            />
            <Area
              type="monotone"
              dataKey="L2"
              stroke="#3b82f6"
              strokeWidth={2}
              fill="url(#cacheL2)"
              dot={false}
              isAnimationActive={false}
            />
            <Area
              type="monotone"
              dataKey="L3"
              stroke="#eab308"
              strokeWidth={2}
              fill="url(#cacheL3)"
              dot={false}
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
