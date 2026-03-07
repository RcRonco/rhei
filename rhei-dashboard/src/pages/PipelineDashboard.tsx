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
