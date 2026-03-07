import { useQuery } from "@tanstack/react-query";
import { useRef, useCallback } from "react";
import {
  fetchMetrics,
  fetchLogs,
  fetchTopology,
  fetchHealth,
  fetchInfo,
  fetchMetricsHistory,
} from "./client";
import type { LogEntry, TimestampedSnapshot } from "./types";

export function useMetrics(baseUrl: string) {
  return useQuery({
    queryKey: ["metrics", baseUrl],
    queryFn: () => fetchMetrics(baseUrl),
    refetchInterval: 500,
    enabled: !!baseUrl,
  });
}

export function useLogs(baseUrl: string) {
  const lastSeq = useRef(0);
  const allLogs = useRef<LogEntry[]>([]);

  const query = useQuery({
    queryKey: ["logs", baseUrl, lastSeq.current],
    queryFn: async () => {
      const newEntries = await fetchLogs(baseUrl, lastSeq.current);
      if (newEntries.length > 0) {
        lastSeq.current = newEntries[newEntries.length - 1].seq;
        allLogs.current = [...allLogs.current, ...newEntries].slice(-1000);
      }
      return allLogs.current;
    },
    refetchInterval: 250,
    enabled: !!baseUrl,
  });

  const clearLogs = useCallback(() => {
    allLogs.current = [];
    lastSeq.current = 0;
  }, []);

  return { ...query, clearLogs };
}

export function useTopology(baseUrl: string) {
  return useQuery({
    queryKey: ["topology", baseUrl],
    queryFn: () => fetchTopology(baseUrl),
    staleTime: Infinity,
    enabled: !!baseUrl,
  });
}

export function useHealth(baseUrl: string) {
  return useQuery({
    queryKey: ["health", baseUrl],
    queryFn: () => fetchHealth(baseUrl),
    refetchInterval: 2000,
    enabled: !!baseUrl,
  });
}

export function useInfo(baseUrl: string) {
  return useQuery({
    queryKey: ["info", baseUrl],
    queryFn: () => fetchInfo(baseUrl),
    refetchInterval: 5000,
    enabled: !!baseUrl,
  });
}

export function useMetricsHistory(baseUrl: string) {
  const clientHistory = useRef<TimestampedSnapshot[]>([]);

  return useQuery({
    queryKey: ["metrics-history", baseUrl],
    queryFn: async () => {
      const serverData = await fetchMetricsHistory(baseUrl);
      // Merge with client-side accumulation
      const serverTimestamps = new Set(serverData.map(([ts]) => ts));
      const unique = clientHistory.current.filter(
        ([ts]) => !serverTimestamps.has(ts),
      );
      const merged = [...unique, ...serverData].sort((a, b) => a[0] - b[0]);
      // Keep ~30 min (3600 entries at 500ms)
      clientHistory.current = merged.slice(-3600);
      return clientHistory.current;
    },
    refetchInterval: 2000,
    enabled: !!baseUrl,
  });
}
