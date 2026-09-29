import type { Scenario, State } from './types';

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
};
