import type {
  MetricsSnapshot,
  LogEntry,
  Topology,
  HealthResponse,
  PipelineInfo,
  TimestampedSnapshot,
} from "./types";

async function fetchJson<T>(baseUrl: string, path: string): Promise<T> {
  const res = await fetch(`${baseUrl}${path}`);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export function fetchMetrics(baseUrl: string): Promise<MetricsSnapshot> {
  return fetchJson(baseUrl, "/api/metrics");
}

export function fetchLogs(
  baseUrl: string,
  after: number = 0,
): Promise<LogEntry[]> {
  return fetchJson(baseUrl, `/api/logs?after=${after}`);
}

export function fetchTopology(baseUrl: string): Promise<Topology> {
  return fetchJson(baseUrl, "/api/topology");
}

export function fetchHealth(baseUrl: string): Promise<HealthResponse> {
  return fetchJson(baseUrl, "/api/health");
}

export function fetchInfo(baseUrl: string): Promise<PipelineInfo> {
  return fetchJson(baseUrl, "/api/info");
}

export function fetchMetricsHistory(
  baseUrl: string,
  sinceMs: number = 0,
): Promise<TimestampedSnapshot[]> {
  return fetchJson(baseUrl, `/api/metrics/history?since=${sinceMs}`);
}
