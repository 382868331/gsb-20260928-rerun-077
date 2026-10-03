import type { DivergenceAnalysis, ExperimentView, ForkPairView, InterventionRef, RecoveryPreview, RecoveryResult, Scenario, Selector, State } from './types';

async function call<T>(path: string, body?: unknown): Promise<T> {
  const response = await fetch(path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) {
    const text = await response.text();
    try {
      const parsed = JSON.parse(text) as { error?: string };
      if (parsed.error) throw new Error(parsed.error);
    } catch (err) {
      if (err instanceof SyntaxError) throw new Error(text);
      throw err;
    }
    throw new Error(text);
  }
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
  forkPairs: () => call<ForkPairView[]>('/api/fork-pairs'),
  recoveryPreview: (forkKey: string) => call<RecoveryPreview>('/api/fork-recovery/preview', { forkKey }),
  recoveryConfirm: (forkKey: string, token: string, action: 'repair' | 'cancel') =>
    call<RecoveryResult>('/api/fork-recovery/confirm', { forkKey, token, action }),
  experiments: () => call<ExperimentView[]>('/api/experiments'),
  analyses: () => call<DivergenceAnalysis[]>('/api/analyses'),
  analyze: (experimentAId: string, experimentBId: string, candidate: InterventionRef) =>
    call<DivergenceAnalysis>('/api/analyses', { experimentAId, experimentBId, candidate }),
  branch: (id: string, op: string, body?: unknown) => call<ExperimentView>(`/api/experiments/${id}/${op}`, body ?? {}),
};
