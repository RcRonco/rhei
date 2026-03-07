export interface MetricsSnapshot {
  elements_total: number;
  batches_total: number;
  elements_per_second: number;
  state_get_total: number;
  state_put_total: number;
  l1_hits: number;
  l2_hits: number;
  l3_hits: number;
  l1_hit_rate: number;
  l2_hit_rate: number;
  l3_hit_rate: number;
  checkpoint_duration_secs: number;
  element_duration_p50: number;
  element_duration_p99: number;
  stash_depth: number;
  pending_futures: number;
  workers: number;
  uptime: number;
}

export interface LogEntry {
  seq: number;
  timestamp_ms: number;
  level: string;
  target: string;
  message: string;
  worker: number | null;
}

export interface TopologyNode {
  id: number;
  kind: string;
  name: string;
}

export interface Topology {
  nodes: TopologyNode[];
  edges: [number, number][];
}

export interface HealthResponse {
  status: string;
  uptime_secs: number;
}

export interface PipelineInfo {
  name: string;
  version: string;
  workers: number;
  uptime_secs: number;
}

export type TimestampedSnapshot = [number, MetricsSnapshot];
