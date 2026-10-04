import type { DivergenceAnalysis, ExperimentView, ForkView, InterventionRef, Scenario, Selector, State } from './types';

async function call<T>(path: string, body?: unknown): Promise<T> {
  const response = await fetch(path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}

export const api = {
  state: () => call<State>('/api/state'),
  advance: (ticks: number) => call<State>('/api/advance', { ticks }),
  client: (node: number, data: string) => call<State>('/api/client', { node, data }),
  partition: (groups: number[][]) => call<State>('/api/partition', { groups }),
  link: (from: number, to: number, patch: { up?: boolean; delay?: number }) =>
    call<State>('/api/link', { from, to, ...patch }),
  heal: () => call<State>('/api/heal'),
  replay: (scenario: Scenario) => call<State>('/api/replay', { scenario }),
  currentScenario: () => call<Scenario>('/api/scenario'),
  save: (name: string, scenario: Scenario) => call<{ saved: boolean }>('/api/save', { name, scenario }),
  armPause: (selector: Selector) => call<State>('/api/arm-pause', { selector }),
  clearPause: () => call<State>('/api/clear-pause'),
  fork: (selector: Selector) => call<{ a: ExperimentView; b: ExperimentView }>('/api/fork', { selector }),
  experiments: () => call<ExperimentView[]>('/api/experiments'),
  analyses: () => call<DivergenceAnalysis[]>('/api/analyses'),
  analyze: (experimentAId: string, experimentBId: string, candidate: InterventionRef) =>
    call<DivergenceAnalysis>('/api/analyses', { experimentAId, experimentBId, candidate }),
  branch: (id: string, op: string, body?: unknown) => call<ExperimentView>(`/api/experiments/${id}/${op}`, body ?? {}),
  forks: () => call<ForkView[]>('/api/forks'),
  recoverFork: (key: string) => call<{ a: ExperimentView; b: ExperimentView }>(`/api/forks/${encodeURIComponent(key)}/recover`, {}),
  cancelFork: (key: string) => call<ForkView>(`/api/forks/${encodeURIComponent(key)}/cancel`, {}),
};
