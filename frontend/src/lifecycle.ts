import type { EntryLifecycle, EntryStage, Event, State } from './types';

export function normalizeState(raw: State): State {
  return {
    ...raw,
    nodes: raw.nodes ?? [],
    inFlight: raw.inFlight ?? [],
    events: raw.events ?? [],
    committed: raw.committed ?? {},
    links: raw.links ?? {},
  };
}

// Build per-entry, per-node lifecycle from the simulator's own event stream.
// Nothing here is inferred from a shared view: append/replicate/commit/abandon
// all come from events recorded while that node handled a message.
export function buildLifecycle(events: Event[]): Map<string, EntryLifecycle> {
  const map = new Map<string, EntryLifecycle>();
  const keyOf = (data: string, term: number, index: number) => `${term}#${index}:${data}`;
  const touch = (event: Event, stage: EntryStage): void => {
    if (event.node === undefined || event.index === undefined || event.data === undefined) return;
    const key = keyOf(event.data, event.term ?? 0, event.index);
    let lifecycle = map.get(key);
    if (!lifecycle) {
      lifecycle = { data: event.data, index: event.index, term: event.term ?? 0, perNode: {} };
      map.set(key, lifecycle);
    }
    const previous = lifecycle.perNode[event.node];
    if (!previous || event.t >= previous.at) {
      lifecycle.perNode[event.node] = { stage, at: event.t };
    }
  };
  for (const event of events) {
    if (event.kind === 'append' || event.kind === 'replicate') touch(event, 'replicated');
    if (event.kind === 'commit') touch(event, 'committed');
    if (event.kind === 'abandon') touch(event, 'abandoned');
  }
  return map;
}

export const stageLabel: Record<EntryStage, string> = {
  appended: '本地追加',
  replicated: '已复制',
  committed: '已提交',
  abandoned: '已放弃',
};

export const stageClass: Record<EntryStage, string> = {
  appended: 'uncommitted',
  replicated: 'uncommitted',
  committed: 'committed',
  abandoned: 'abandoned',
};

// A cluster-wide commit is announced only when the node itself reported a commit
// event for that entry AND a majority of nodes hold it committed. The minority
// leader can paint its local card, but it can never produce this success view.
export function committedEntries(state: State): Set<string> {
  const result = new Set<string>();
  const total = state.nodes.length;
  const tally = new Map<string, number>();
  for (const node of state.nodes) {
    for (const entry of node.log) {
      if (entry.index > node.commitIndex) continue;
      const key = `${entry.term}#${entry.index}:${entry.data}`;
      tally.set(key, (tally.get(key) ?? 0) + 1);
    }
  }
  for (const [key, count] of tally) {
    if (count * 2 > total) result.add(key);
  }
  return result;
}
