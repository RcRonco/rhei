import { useState, useCallback } from "react";
import { useStateOperators, useStateEntries, useStateKey } from "../../api/hooks";
import OperatorList from "./OperatorList";
import KeyBrowser from "./KeyBrowser";
import ValueViewer from "./ValueViewer";

interface StateExplorerProps {
  baseUrl: string;
}

const PAGE_SIZE = 50;

export default function StateExplorer({ baseUrl }: StateExplorerProps) {
  const [selectedOp, setSelectedOp] = useState<string | null>(null);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [decode, setDecode] = useState("json");
  const [limit, setLimit] = useState(PAGE_SIZE);

  const { data: opsData } = useStateOperators(baseUrl);
  const searchOpts = search.startsWith("/") && search.endsWith("/") && search.length > 2
    ? { pattern: search.slice(1, -1), decode, limit }
    : { prefix: search || undefined, decode, limit };
  const { data: entriesData } = useStateEntries(
    baseUrl,
    selectedOp ?? "",
    searchOpts,
  );
  const { data: keyData } = useStateKey(baseUrl, selectedOp ?? "", selectedKey ?? "");

  const handleSelectOp = useCallback((name: string) => {
    setSelectedOp(name);
    setSelectedKey(null);
    setSearch("");
    setLimit(PAGE_SIZE);
  }, []);

  const handleSearchChange = useCallback((value: string) => {
    setSearch(value);
    setSelectedKey(null);
    setLimit(PAGE_SIZE);
  }, []);

  const handleLoadMore = useCallback(() => {
    setLimit((prev) => prev + PAGE_SIZE);
  }, []);

  if (!opsData) {
    return (
      <div className="flex items-center justify-center h-full text-sm text-gray-600">
        No checkpoint data available. State will appear after the first checkpoint.
      </div>
    );
  }

  const entries = entriesData?.entries ?? [];
  const total = entriesData?.total ?? 0;

  return (
    <div className="flex h-full gap-4">
      {/* Left panel: Operator list */}
      <div className="w-56 card shrink-0 overflow-hidden">
        <OperatorList
          operators={opsData.operators}
          selected={selectedOp}
          onSelect={handleSelectOp}
          checkpointId={opsData.checkpoint_id}
          timestampMs={opsData.timestamp_ms}
        />
      </div>

      {/* Right panel: Key browser + value viewer */}
      <div className="flex-1 flex flex-col gap-4 min-w-0">
        {/* Key browser */}
        <div className="card flex-1 min-h-0 overflow-hidden">
          {selectedOp ? (
            <KeyBrowser
              entries={entries}
              total={total}
              selectedKey={selectedKey}
              onSelectKey={setSelectedKey}
              search={search}
              onSearchChange={handleSearchChange}
              decode={decode}
              onDecodeChange={setDecode}
              onLoadMore={handleLoadMore}
              hasMore={entries.length < total}
            />
          ) : (
            <div className="h-full flex items-center justify-center text-xs text-gray-600">
              Select an operator to browse its state
            </div>
          )}
        </div>

        {/* Value viewer */}
        <div className="card h-64 shrink-0 overflow-hidden">
          <ValueViewer
            keyName={selectedKey}
            decodings={keyData?.decodings ?? null}
            sizeBytes={keyData?.size_bytes ?? 0}
          />
        </div>
      </div>
    </div>
  );
}
