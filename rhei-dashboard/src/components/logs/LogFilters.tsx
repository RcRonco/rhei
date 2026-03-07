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
