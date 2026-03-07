import { create } from "zustand";

export interface Pipeline {
  id: string;
  url: string;
  name: string;
}

interface PipelineStore {
  pipelines: Pipeline[];
  addPipeline: (url: string, name?: string) => void;
  removePipeline: (id: string) => void;
}

function loadPipelines(): Pipeline[] {
  try {
    const stored = localStorage.getItem("rhei-pipelines");
    if (stored) {
      return JSON.parse(stored) as Pipeline[];
    }
  } catch {
    // ignore
  }
  return [];
}

function savePipelines(pipelines: Pipeline[]) {
  localStorage.setItem("rhei-pipelines", JSON.stringify(pipelines));
}

export const usePipelineStore = create<PipelineStore>((set) => ({
  pipelines: loadPipelines(),

  addPipeline: (url: string, name?: string) =>
    set((state) => {
      if (state.pipelines.some((p) => p.url === url)) return state;
      const id = crypto.randomUUID();
      const pipeline: Pipeline = {
        id,
        url: url.replace(/\/$/, ""),
        name: name || url,
      };
      const pipelines = [...state.pipelines, pipeline];
      savePipelines(pipelines);
      return { pipelines };
    }),

  removePipeline: (id: string) =>
    set((state) => {
      const pipelines = state.pipelines.filter((p) => p.id !== id);
      savePipelines(pipelines);
      return { pipelines };
    }),
}));
