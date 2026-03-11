# Dashboard Redesign Design

## Context

The Rhei web dashboard (`rhei-dashboard/`) is functional but visually dated. It uses flat gray cards with no visual hierarchy, buries the topology graph at the bottom, presents 12 metrics at equal prominence, and stacks everything in a long vertical scroll. The goal is a Vercel/Linear-style developer infrastructure aesthetic: clean, information-dense, professional.

## Decision

Redesign the frontend using Tremor components on top of the existing Tailwind v4 setup. Restructure the layout into distinct zones with clear visual hierarchy. No backend changes.

### Layout Architecture

Full-height app with collapsible sidebar + main content area + log drawer:

- **Sidebar** (240px expanded, 56px collapsed): Rhei logo, "Overview" nav link, pipeline list with status dots and active accent bar, "Add pipeline" input at bottom. Toggle button to collapse to icon-only mode.
- **Header** (48px, pipeline detail only): Breadcrumb nav (`Overview / pipeline-name`), status Badge, worker count, version, uptime.
- **Log drawer** (bottom of main area, pipeline detail only): Collapsed by default showing "Logs" label + unread count badge. Expandable to ~250px with drag handle to resize.

### Overview Page

Pipeline cards with clear hierarchy:
- Hero metric: throughput displayed large (Tremor Metric)
- Secondary: p99 latency + worker count in smaller muted text
- Status dot + faint border tint matching status color
- Responsive grid: 1-col mobile, 2-col tablet, 3-col desktop

### Pipeline Detail Page

Four zones replacing the current vertical stack:

**Zone 1 — Hero KPIs (top row):**
4 cards with Tremor Card + Metric + SparkAreaChart showing: Throughput, p99 Latency, Elements Total, L1 Cache Hit Rate. Each has current value (large), label (muted), and inline sparkline from last ~60 data points.

**Zone 2 — Topology + Secondary Metrics (side by side):**
- Left ~60%: Pipeline DAG graph (React Flow), promoted to 400px height
- Right ~40%: Compact 2x3 grid of remaining metrics (p50 latency, L2/L3 hit rates, stash depth, pending futures, checkpoint duration)

**Zone 3 — Time-series Charts (2x2 grid):**
Tremor AreaChart/LineChart replacing Recharts: throughput, latency (p50+p99), cache hit rates, backpressure.

**Zone 4 — Log Drawer:**
Collapsible bottom panel with log level filter badges, search input, auto-scroll, monospace log entries.

### Visual Design System

- **Font:** Inter (Google Fonts), monospace for metric values
- **Background layers:** gray-950 (page) -> gray-900 (sidebar, cards) -> gray-800 (inputs, nested)
- **Text:** white (primary) -> gray-400 (secondary) -> gray-500 (tertiary)
- **Accent:** blue-500 for active states
- **Status:** emerald-500 (healthy), amber-500 (warning), red-500 (error)
- **Cards:** Tremor Card with subtle ring-1 ring-gray-800 border, hover shadow lift
- **Interactions:** transition-shadow on hover, transition-all on sidebar collapse, no page transitions
- **Spacing:** p-6 page padding, gap-4 between cards, gap-6 between zones

### Dependency Changes

**Add:**
- `@tremor/react` — dashboard components
- `@headlessui/react` — drawer/transitions
- `@remixicon/react` — icon set

**Remove:**
- `recharts` — replaced by Tremor charts

**Keep:**
- `@xyflow/react`, `@tanstack/react-query`, `zustand`, `react-router-dom`, `tailwindcss` v4

### Component Changes

| File | Action |
|------|--------|
| `layout/Sidebar.tsx` | Rewrite — collapsible, icon nav, accent bar |
| `layout/Header.tsx` | Rewrite — breadcrumbs, Tremor Badge |
| `dashboard/MetricsPanel.tsx` | Split into `HeroKPIs.tsx` + `SecondaryMetrics.tsx` |
| `dashboard/ThroughputChart.tsx` | Rewrite with Tremor AreaChart |
| `dashboard/LatencyChart.tsx` | Rewrite with Tremor LineChart |
| `dashboard/CacheHitRates.tsx` | Rewrite with Tremor AreaChart |
| `dashboard/BackpressureGauge.tsx` | Rewrite with Tremor LineChart |
| `topology/PipelineGraph.tsx` | Restyle (taller, updated background) |
| `topology/GraphNode.tsx` | Minor style refresh |
| `logs/LogViewer.tsx` | Rewrite as drawer component |
| `logs/LogFilters.tsx` | Restyle with Tremor inputs |
| `pages/Overview.tsx` | Rewrite with new card layout |
| `pages/PipelineDashboard.tsx` | Rewrite with zone layout |
| `index.css` | Update with Inter font import |
| `api/*` | No changes |
| `stores/*` | No changes |

## Alternatives Considered

1. **Pure Tailwind redesign (no new libs):** Full control, smaller bundle, but Recharts is hard to style well and requires much more custom CSS work.
2. **Grafana-style with uPlot:** Maximum information density and fastest charts, but lower-level API and less visual polish.

Chose Tremor because it provides the most dramatic visual upgrade with least custom work, and its aesthetic matches the Vercel/Linear target.

## Consequences

### Positive
- Professional, modern aesthetic matching developer infrastructure tools
- Clear visual hierarchy (hero KPIs vs secondary metrics)
- Topology graph promoted to prominent position
- Log drawer reduces vertical scroll
- Tremor components are consistent and well-maintained

### Negative
- Adds ~50KB to bundle (Tremor + Headless UI + Remix Icons)
- Opinionated component library — harder to deviate from Tremor's aesthetic later
- Recharts still bundled transitively (Tremor uses it internally) but no longer a direct dependency
