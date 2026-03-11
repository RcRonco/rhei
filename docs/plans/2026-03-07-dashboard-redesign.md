# Dashboard Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Redesign the Rhei dashboard frontend from a dated, flat UI to a Vercel/Linear-style developer infrastructure aesthetic with clear visual hierarchy, collapsible sidebar, hero KPIs with sparklines, promoted topology graph, and a collapsible log drawer.

**Architecture:** Custom Tremor-inspired components built with Tailwind v4 (no Tremor dependency due to TW4 incompatibility). Recharts stays but gets heavy restyling. Layout restructured into zones: hero KPIs -> topology + secondary metrics -> time-series charts -> log drawer. Collapsible sidebar with icon-only mode.

**Tech Stack:** React 19, Tailwind CSS v4, Recharts (restyled), React Flow, Headless UI (transitions), Remix Icons, TanStack Query, Zustand.

---

### Task 1: Install dependencies and update base styles

**Files:**
- Modify: `rhei-dashboard/package.json`
- Modify: `rhei-dashboard/index.html`
- Modify: `rhei-dashboard/src/index.css`

**Step 1: Install new packages**

Run:
```bash
cd rhei-dashboard && npm install @headlessui/react @remixicon/react
```

**Step 2: Update `index.html` to load Inter font**

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />
    <title>Rhei Dashboard</title>
  </head>
  <body class="bg-gray-950 text-gray-100 antialiased">
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

**Step 3: Rewrite `src/index.css` with design system tokens**

```css
@import "tailwindcss";

@layer base {
  :root {
    --node-source: #22c55e;
    --node-transform: #06b6d4;
    --node-filter: #eab308;
    --node-keyby: #3b82f6;
    --node-sink: #d946ef;
    --node-operator: #06b6d4;
    --node-merge: #a855f7;
  }

  body {
    font-family: "Inter", system-ui, -apple-system, sans-serif;
  }

  code, .font-mono {
    font-family: "JetBrains Mono", ui-monospace, monospace;
  }
}

@layer components {
  /* Card base — used across all dashboard cards */
  .card {
    @apply bg-gray-900 rounded-xl ring-1 ring-gray-800 transition-shadow;
  }
  .card:hover {
    @apply shadow-lg shadow-black/20;
  }

  /* Recharts overrides for dark theme consistency */
  .recharts-cartesian-grid line {
    stroke: #1f2937;
  }
  .recharts-tooltip-wrapper .recharts-default-tooltip {
    background-color: #111827 !important;
    border: 1px solid #1f2937 !important;
    border-radius: 0.5rem !important;
    box-shadow: 0 10px 25px rgba(0, 0, 0, 0.4) !important;
  }
  .recharts-tooltip-item {
    color: #d1d5db !important;
    font-size: 0.75rem !important;
  }
  .recharts-legend-item-text {
    color: #9ca3af !important;
    font-size: 0.75rem !important;
  }
}
```

**Step 4: Verify build**

Run:
```bash
cd rhei-dashboard && npx tsc --noEmit && npx vite build
```
Expected: builds without errors.

**Step 5: Commit**

```bash
git add rhei-dashboard/package.json rhei-dashboard/package-lock.json rhei-dashboard/index.html rhei-dashboard/src/index.css
git commit -m "feat(dashboard): install deps and update base design system"
```

---

### Task 2: Rewrite Sidebar — collapsible with icon nav

**Files:**
- Rewrite: `rhei-dashboard/src/components/layout/Sidebar.tsx`

**Step 1: Rewrite Sidebar.tsx**

```tsx
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
    <span className={`inline-block w-2 h-2 rounded-full shrink-0 ${color}`}>
      {data?.status === "running" && (
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
      <span className="relative">
        <ConnectionDot baseUrl={pipeline.url} />
      </span>
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
      {/* Logo + collapse toggle */}
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

      {/* Nav */}
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

      {/* Add pipeline input */}
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
```

**Step 2: Verify build**

Run:
```bash
cd rhei-dashboard && npx tsc --noEmit
```
Expected: no type errors.

**Step 3: Commit**

```bash
git add rhei-dashboard/src/components/layout/Sidebar.tsx
git commit -m "feat(dashboard): rewrite sidebar with collapse and icon nav"
```

---

### Task 3: Rewrite Header — breadcrumbs and status badge

**Files:**
- Rewrite: `rhei-dashboard/src/components/layout/Header.tsx`

**Step 1: Rewrite Header.tsx**

```tsx
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
```

**Step 2: Verify build**

Run:
```bash
cd rhei-dashboard && npx tsc --noEmit
```
Expected: no type errors.

**Step 3: Commit**

```bash
git add rhei-dashboard/src/components/layout/Header.tsx
git commit -m "feat(dashboard): rewrite header with breadcrumbs and status badge"
```

---

### Task 4: Create HeroKPIs component with sparklines

**Files:**
- Create: `rhei-dashboard/src/components/dashboard/HeroKPIs.tsx`

**Step 1: Create HeroKPIs.tsx**

This component shows the 4 most important metrics with large values and inline mini sparklines.

```tsx
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
            <linearGradient id={`spark-${color}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={0.3} />
              <stop offset="100%" stopColor={color} stopOpacity={0} />
            </linearGradient>
          </defs>
          <Area
            type="monotone"
            dataKey="v"
            stroke={color}
            strokeWidth={1.5}
            fill={`url(#spark-${color})`}
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
```

**Step 2: Verify build**

Run:
```bash
cd rhei-dashboard && npx tsc --noEmit
```
Expected: no type errors.

**Step 3: Commit**

```bash
git add rhei-dashboard/src/components/dashboard/HeroKPIs.tsx
git commit -m "feat(dashboard): add HeroKPIs component with sparklines"
```

---

### Task 5: Create SecondaryMetrics component

**Files:**
- Create: `rhei-dashboard/src/components/dashboard/SecondaryMetrics.tsx`

**Step 1: Create SecondaryMetrics.tsx**

Compact 2x3 grid for the non-hero metrics.

```tsx
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
      <MiniStat label="p50 Latency" value={(metrics.element_duration_p50 * 1000).toFixed(2)} unit="ms" />
      <MiniStat label="L2 Cache" value={`${metrics.l2_hit_rate.toFixed(1)}%`} />
      <MiniStat label="L3 Cache" value={`${metrics.l3_hit_rate.toFixed(1)}%`} />
      <MiniStat label="Stash Depth" value={metrics.stash_depth.toFixed(0)} />
      <MiniStat label="Pending" value={metrics.pending_futures.toFixed(0)} />
      <MiniStat label="Checkpoint" value={(metrics.checkpoint_duration_secs * 1000).toFixed(1)} unit="ms" />
    </div>
  );
}
```

**Step 2: Verify build**

Run:
```bash
cd rhei-dashboard && npx tsc --noEmit
```
Expected: no type errors.

**Step 3: Commit**

```bash
git add rhei-dashboard/src/components/dashboard/SecondaryMetrics.tsx
git commit -m "feat(dashboard): add SecondaryMetrics compact grid"
```

---

### Task 6: Restyle time-series charts

**Files:**
- Rewrite: `rhei-dashboard/src/components/dashboard/ThroughputChart.tsx`
- Rewrite: `rhei-dashboard/src/components/dashboard/LatencyChart.tsx`
- Rewrite: `rhei-dashboard/src/components/dashboard/CacheHitRates.tsx`
- Rewrite: `rhei-dashboard/src/components/dashboard/BackpressureGauge.tsx`

All four charts get the same treatment: card wrapper with `.card` class, refined colors, cleaner grid, and consistent styling. The Recharts components stay but with updated props.

**Step 1: Rewrite ThroughputChart.tsx**

```tsx
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
```

**Step 2: Rewrite LatencyChart.tsx**

```tsx
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
```

**Step 3: Rewrite CacheHitRates.tsx**

```tsx
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
```

**Step 4: Rewrite BackpressureGauge.tsx**

```tsx
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

interface BackpressureGaugeProps {
  history?: TimestampedSnapshot[];
}

function formatTime(ms: number): string {
  const d = new Date(ms);
  return `${d.getHours().toString().padStart(2, "0")}:${d.getMinutes().toString().padStart(2, "0")}:${d.getSeconds().toString().padStart(2, "0")}`;
}

export default function BackpressureGauge({ history }: BackpressureGaugeProps) {
  const data = (history ?? []).map(([ts, snap]) => ({
    time: ts,
    stash: snap.stash_depth,
    pending: snap.pending_futures,
  }));

  return (
    <div className="card p-5">
      <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-4">
        Backpressure
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
            />
            <Legend
              iconType="plainline"
              wrapperStyle={{ fontSize: "0.75rem", color: "#9ca3af" }}
            />
            <Line
              type="monotone"
              dataKey="stash"
              name="Stash Depth"
              stroke="#f59e0b"
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="pending"
              name="Pending Futures"
              stroke="#8b5cf6"
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
```

**Step 5: Verify build**

Run:
```bash
cd rhei-dashboard && npx tsc --noEmit
```
Expected: no type errors.

**Step 6: Commit**

```bash
git add rhei-dashboard/src/components/dashboard/ThroughputChart.tsx rhei-dashboard/src/components/dashboard/LatencyChart.tsx rhei-dashboard/src/components/dashboard/CacheHitRates.tsx rhei-dashboard/src/components/dashboard/BackpressureGauge.tsx
git commit -m "feat(dashboard): restyle all time-series charts"
```

---

### Task 7: Restyle topology graph and nodes

**Files:**
- Rewrite: `rhei-dashboard/src/components/topology/PipelineGraph.tsx`
- Rewrite: `rhei-dashboard/src/components/topology/GraphNode.tsx`

**Step 1: Rewrite GraphNode.tsx**

```tsx
import { Handle, Position, type NodeProps } from "@xyflow/react";

const KIND_STYLES: Record<string, { border: string; bg: string; accent: string }> = {
  source: { border: "border-emerald-500/40", bg: "bg-emerald-500/5", accent: "bg-emerald-500" },
  transform: { border: "border-cyan-500/40", bg: "bg-cyan-500/5", accent: "bg-cyan-500" },
  operator: { border: "border-cyan-500/40", bg: "bg-cyan-500/5", accent: "bg-cyan-500" },
  filter: { border: "border-amber-500/40", bg: "bg-amber-500/5", accent: "bg-amber-500" },
  key_by: { border: "border-blue-500/40", bg: "bg-blue-500/5", accent: "bg-blue-500" },
  sink: { border: "border-fuchsia-500/40", bg: "bg-fuchsia-500/5", accent: "bg-fuchsia-500" },
  merge: { border: "border-purple-500/40", bg: "bg-purple-500/5", accent: "bg-purple-500" },
};

const KIND_LABELS: Record<string, string> = {
  source: "Source",
  transform: "Transform",
  operator: "Operator",
  filter: "Filter",
  key_by: "KeyBy",
  sink: "Sink",
  merge: "Merge",
};

interface GraphNodeData {
  label: string;
  kind: string;
  [key: string]: unknown;
}

export default function GraphNode({ data }: NodeProps & { data: GraphNodeData }) {
  const kind = data.kind ?? "transform";
  const style = KIND_STYLES[kind] ?? KIND_STYLES.transform;
  const kindLabel = KIND_LABELS[kind] ?? kind;

  return (
    <div
      className={`rounded-xl border ${style.border} ${style.bg} px-4 py-2.5 min-w-[140px] backdrop-blur-sm`}
    >
      <Handle type="target" position={Position.Top} className="!bg-gray-600 !w-2 !h-2 !border-0" />
      <div className="flex items-center gap-1.5">
        <span className={`w-1.5 h-1.5 rounded-full ${style.accent}`} />
        <span className="text-[10px] text-gray-500 uppercase tracking-wider font-medium">
          {kindLabel}
        </span>
      </div>
      <div className="text-sm font-medium text-white mt-1">{data.label}</div>
      <Handle type="source" position={Position.Bottom} className="!bg-gray-600 !w-2 !h-2 !border-0" />
    </div>
  );
}
```

**Step 2: Rewrite PipelineGraph.tsx**

The component stays mostly the same structurally. Key changes: taller (400px), refined background, updated edge style.

```tsx
import { useMemo, useCallback } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { Topology } from "../../api/types";
import GraphNode from "./GraphNode";

const nodeTypes = { custom: GraphNode };

interface PipelineGraphProps {
  topology?: Topology;
}

function layoutNodes(topology: Topology): { nodes: Node[]; edges: Edge[] } {
  const nodeMap = new Map(topology.nodes.map((n) => [n.id, n]));

  const depths = new Map<number, number>();
  const childrenOf = new Map<number, number[]>();

  for (const node of topology.nodes) {
    childrenOf.set(node.id, []);
  }
  for (const [from, to] of topology.edges) {
    childrenOf.get(from)?.push(to);
  }

  const hasIncoming = new Set(topology.edges.map(([, to]) => to));
  const roots = topology.nodes.filter((n) => !hasIncoming.has(n.id)).map((n) => n.id);

  const queue = roots.map((id) => ({ id, depth: 0 }));
  while (queue.length > 0) {
    const { id, depth } = queue.shift()!;
    const current = depths.get(id) ?? -1;
    if (depth > current) {
      depths.set(id, depth);
      for (const child of childrenOf.get(id) ?? []) {
        queue.push({ id: child, depth: depth + 1 });
      }
    }
  }

  const byDepth = new Map<number, number[]>();
  for (const [id, depth] of depths) {
    const arr = byDepth.get(depth) ?? [];
    arr.push(id);
    byDepth.set(depth, arr);
  }

  const nodes: Node[] = [];
  for (const [depth, ids] of byDepth) {
    ids.forEach((id, i) => {
      const node = nodeMap.get(id);
      if (!node) return;
      nodes.push({
        id: String(id),
        type: "custom",
        position: {
          x: i * 220 - ((ids.length - 1) * 220) / 2 + 300,
          y: depth * 130 + 50,
        },
        data: { label: node.name, kind: node.kind },
      });
    });
  }

  const edges: Edge[] = topology.edges.map(([from, to]) => ({
    id: `${from}-${to}`,
    source: String(from),
    target: String(to),
    animated: true,
    style: { stroke: "#374151", strokeWidth: 2 },
  }));

  return { nodes, edges };
}

export default function PipelineGraph({ topology }: PipelineGraphProps) {
  const { initialNodes, initialEdges } = useMemo(() => {
    if (!topology) return { initialNodes: [], initialEdges: [] };
    const { nodes, edges } = layoutNodes(topology);
    return { initialNodes: nodes, initialEdges: edges };
  }, [topology]);

  const [nodes, , onNodesChange] = useNodesState(initialNodes);
  const [edges, , onEdgesChange] = useEdgesState(initialEdges);

  const onInit = useCallback(() => {}, []);

  if (!topology) {
    return (
      <div className="card h-[400px] flex items-center justify-center text-gray-600 text-sm">
        Loading topology...
      </div>
    );
  }

  return (
    <div className="card overflow-hidden h-[400px]">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onInit={onInit}
        nodeTypes={nodeTypes}
        fitView
        proOptions={{ hideAttribution: true }}
        className="bg-gray-950"
      >
        <Background color="#1f2937" gap={24} size={1} />
        <Controls
          className="!bg-gray-900 !border-gray-800 !rounded-lg !shadow-lg"
          showInteractive={false}
        />
      </ReactFlow>
    </div>
  );
}
```

**Step 3: Verify build**

Run:
```bash
cd rhei-dashboard && npx tsc --noEmit
```
Expected: no type errors.

**Step 4: Commit**

```bash
git add rhei-dashboard/src/components/topology/GraphNode.tsx rhei-dashboard/src/components/topology/PipelineGraph.tsx
git commit -m "feat(dashboard): restyle topology graph and nodes"
```

---

### Task 8: Rewrite LogViewer as collapsible drawer

**Files:**
- Rewrite: `rhei-dashboard/src/components/logs/LogViewer.tsx`
- Rewrite: `rhei-dashboard/src/components/logs/LogFilters.tsx`

**Step 1: Rewrite LogFilters.tsx**

```tsx
interface LogFiltersProps {
  levelFilter: Set<string>;
  onLevelFilterChange: (filter: Set<string>) => void;
  search: string;
  onSearchChange: (search: string) => void;
}

const LEVELS = ["ERROR", "WARN", "INFO", "DEBUG", "TRACE"];
const LEVEL_STYLES: Record<string, { active: string; inactive: string }> = {
  ERROR: {
    active: "bg-red-500/15 text-red-400 ring-red-500/30",
    inactive: "text-gray-600 ring-gray-800 hover:text-gray-400",
  },
  WARN: {
    active: "bg-amber-500/15 text-amber-400 ring-amber-500/30",
    inactive: "text-gray-600 ring-gray-800 hover:text-gray-400",
  },
  INFO: {
    active: "bg-blue-500/15 text-blue-400 ring-blue-500/30",
    inactive: "text-gray-600 ring-gray-800 hover:text-gray-400",
  },
  DEBUG: {
    active: "bg-gray-500/15 text-gray-400 ring-gray-500/30",
    inactive: "text-gray-600 ring-gray-800 hover:text-gray-400",
  },
  TRACE: {
    active: "bg-gray-600/15 text-gray-500 ring-gray-600/30",
    inactive: "text-gray-600 ring-gray-800 hover:text-gray-400",
  },
};

export default function LogFilters({
  levelFilter,
  onLevelFilterChange,
  search,
  onSearchChange,
}: LogFiltersProps) {
  const toggleLevel = (level: string) => {
    const next = new Set(levelFilter);
    if (next.has(level)) {
      next.delete(level);
    } else {
      next.add(level);
    }
    onLevelFilterChange(next);
  };

  return (
    <div className="flex items-center gap-2">
      <div className="flex gap-1">
        {LEVELS.map((level) => {
          const style = LEVEL_STYLES[level];
          const isActive = levelFilter.has(level);
          return (
            <button
              key={level}
              onClick={() => toggleLevel(level)}
              className={`px-2 py-0.5 text-[11px] font-medium rounded-md ring-1 transition-colors ${
                isActive ? style.active : style.inactive
              }`}
            >
              {level}
            </button>
          );
        })}
      </div>
      <input
        type="text"
        value={search}
        onChange={(e) => onSearchChange(e.target.value)}
        placeholder="Search logs..."
        className="bg-gray-800 ring-1 ring-gray-700 rounded-lg px-2.5 py-1 text-xs text-white placeholder-gray-600 w-36 focus:outline-none focus:ring-blue-500 transition-shadow"
      />
    </div>
  );
}
```

**Step 2: Rewrite LogViewer.tsx as a drawer**

```tsx
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
      {/* Drawer header — always visible */}
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

      {/* Drawer body — collapsible */}
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
```

**Step 3: Verify build**

Run:
```bash
cd rhei-dashboard && npx tsc --noEmit
```
Expected: no type errors.

**Step 4: Commit**

```bash
git add rhei-dashboard/src/components/logs/LogViewer.tsx rhei-dashboard/src/components/logs/LogFilters.tsx
git commit -m "feat(dashboard): rewrite log viewer as collapsible drawer"
```

---

### Task 9: Rewrite Overview page

**Files:**
- Rewrite: `rhei-dashboard/src/pages/Overview.tsx`

**Step 1: Rewrite Overview.tsx**

```tsx
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

      {/* Hero metric */}
      <div className="mb-3">
        <div className="text-2xl font-semibold font-mono text-white">
          {metrics ? formatRate(metrics.elements_per_second) : "--"}
          <span className="text-sm font-normal text-gray-500 ml-1.5">elem/s</span>
        </div>
      </div>

      {/* Secondary */}
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
```

**Step 2: Verify build**

Run:
```bash
cd rhei-dashboard && npx tsc --noEmit
```
Expected: no type errors.

**Step 3: Commit**

```bash
git add rhei-dashboard/src/pages/Overview.tsx
git commit -m "feat(dashboard): rewrite overview page with hero metric cards"
```

---

### Task 10: Rewrite PipelineDashboard page with zone layout

**Files:**
- Rewrite: `rhei-dashboard/src/pages/PipelineDashboard.tsx`
- Delete: `rhei-dashboard/src/components/dashboard/MetricsPanel.tsx` (replaced by HeroKPIs + SecondaryMetrics)

**Step 1: Rewrite PipelineDashboard.tsx**

```tsx
import { useParams } from "react-router-dom";
import Sidebar from "../components/layout/Sidebar";
import Header from "../components/layout/Header";
import HeroKPIs from "../components/dashboard/HeroKPIs";
import SecondaryMetrics from "../components/dashboard/SecondaryMetrics";
import ThroughputChart from "../components/dashboard/ThroughputChart";
import LatencyChart from "../components/dashboard/LatencyChart";
import CacheHitRates from "../components/dashboard/CacheHitRates";
import BackpressureGauge from "../components/dashboard/BackpressureGauge";
import PipelineGraph from "../components/topology/PipelineGraph";
import LogViewer from "../components/logs/LogViewer";
import { usePipelineStore } from "../stores/pipelines";
import {
  useMetrics,
  useLogs,
  useTopology,
  useHealth,
  useInfo,
  useMetricsHistory,
} from "../api/hooks";

export default function PipelineDashboard() {
  const { id } = useParams<{ id: string }>();
  const pipelines = usePipelineStore((s) => s.pipelines);
  const pipeline = pipelines.find((p) => p.id === id);

  const baseUrl = pipeline?.url ?? "";

  const { data: metrics } = useMetrics(baseUrl);
  const { data: logs } = useLogs(baseUrl);
  const { data: topology } = useTopology(baseUrl);
  const { data: health } = useHealth(baseUrl);
  const { data: info } = useInfo(baseUrl);
  const { data: history } = useMetricsHistory(baseUrl);

  if (!pipeline) {
    return (
      <div className="flex h-screen">
        <Sidebar />
        <main className="flex-1 flex items-center justify-center bg-gray-950">
          <p className="text-gray-500 text-sm">Pipeline not found</p>
        </main>
      </div>
    );
  }

  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <Header info={info} health={health} />

        <main className="flex-1 overflow-y-auto bg-gray-950 p-6 space-y-6">
          {/* Zone 1: Hero KPIs */}
          <HeroKPIs metrics={metrics} history={history} />

          {/* Zone 2: Topology + Secondary Metrics */}
          <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
            <div className="lg:col-span-3">
              <PipelineGraph topology={topology} />
            </div>
            <div className="lg:col-span-2">
              <SecondaryMetrics metrics={metrics} />
            </div>
          </div>

          {/* Zone 3: Time-series Charts */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <ThroughputChart history={history} />
            <LatencyChart history={history} />
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <CacheHitRates history={history} />
            <BackpressureGauge history={history} />
          </div>
        </main>

        {/* Zone 4: Log Drawer */}
        <LogViewer logs={logs} />
      </div>
    </div>
  );
}
```

**Step 2: Delete MetricsPanel.tsx**

Run:
```bash
rm rhei-dashboard/src/components/dashboard/MetricsPanel.tsx
```

**Step 3: Verify build**

Run:
```bash
cd rhei-dashboard && npx tsc --noEmit && npx vite build
```
Expected: builds without errors. No remaining imports of `MetricsPanel`.

**Step 4: Commit**

```bash
git add rhei-dashboard/src/pages/PipelineDashboard.tsx && git rm rhei-dashboard/src/components/dashboard/MetricsPanel.tsx
git commit -m "feat(dashboard): rewrite pipeline page with zone layout"
```

---

### Task 11: Final verification and cleanup

**Step 1: Full build check**

Run:
```bash
cd rhei-dashboard && npx tsc --noEmit && npx vite build
```
Expected: clean build, no errors.

**Step 2: Check for unused imports or files**

Run:
```bash
cd rhei-dashboard && grep -r "MetricsPanel" src/
```
Expected: no results.

**Step 3: Run dev server for visual check**

Run:
```bash
cd rhei-dashboard && npx vite --host
```
Open in browser: verify sidebar collapses, overview cards show hero metrics, pipeline detail shows zone layout, log drawer expands/collapses.

**Step 4: Final commit if any cleanup was needed**

```bash
git add -A rhei-dashboard/ && git commit -m "chore(dashboard): cleanup after redesign"
```
