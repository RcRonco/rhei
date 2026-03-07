import type {
  MetricsSnapshot,
  LogEntry,
  Topology,
  HealthResponse,
  PipelineInfo,
  TimestampedSnapshot,
  StateOperatorsResponse,
  StateEntriesResponse,
  StateKeyResponse,
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

export function fetchStateOperators(baseUrl: string): Promise<StateOperatorsResponse> {
  return fetchJson(baseUrl, "/api/state/operators");
}

export function fetchStateEntries(
  baseUrl: string,
  operator: string,
  opts: { prefix?: string; pattern?: string; limit?: number; offset?: number; decode?: string } = {},
): Promise<StateEntriesResponse> {
  const params = new URLSearchParams();
  if (opts.prefix) params.set("prefix", opts.prefix);
  if (opts.pattern) params.set("pattern", opts.pattern);
  if (opts.limit) params.set("limit", String(opts.limit));
  if (opts.offset) params.set("offset", String(opts.offset));
  if (opts.decode) params.set("decode", opts.decode);
  const qs = params.toString();
  return fetchJson(baseUrl, `/api/state/operators/${encodeURIComponent(operator)}${qs ? `?${qs}` : ""}`);
}

export function fetchStateKey(
  baseUrl: string,
  operator: string,
  key: string,
): Promise<StateKeyResponse> {
  return fetchJson(
    baseUrl,
    `/api/state/operators/${encodeURIComponent(operator)}/keys/${encodeURIComponent(key)}`,
  );
}
