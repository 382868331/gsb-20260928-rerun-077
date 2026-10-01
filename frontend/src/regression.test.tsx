// @vitest-environment jsdom
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import App from './App';

const nullQueueState = {
  now: 1000,
  nodes: [
    { id: 1, role: 'follower', term: 3, votedFor: 4, commitIndex: 2, deadline: 1200, log: [
      { index: 1, term: 1, data: 'committed-before-split' },
      { index: 2, term: 3, data: 'majority-after-split' },
    ] },
  ],
  links: {},
  inFlight: null,
  events: [],
  committed: { 1: 'committed-before-split' },
} as never;

const scenario = { name: 'old-leader-catches-up', description: '', nodes: [1], seed: 11, initialLeader: 1, actions: [] };

describe('empty queue replay', () => {
  afterEach(() => vi.restoreAllMocks());

  it('renders instead of blanking the workspace when inFlight is null', async () => {
    vi.stubGlobal('fetch', vi.fn(async (path: string) => {
      let payload = JSON.stringify(nullQueueState);
      if (path === '/api/scenario') payload = JSON.stringify(scenario);
      if (path === '/api/experiments') payload = '[]';
      return new Response(payload, { headers: { 'Content-Type': 'application/json' } });
    }));
    document.body.innerHTML = '<div id="root"></div>';
    const root = createRoot(document.getElementById('root')!);
    await act(async () => { root.render(<App />); await new Promise((r) => setTimeout(r, 0)); });
    const html = document.getElementById('root')!.innerHTML;
    expect(html).toContain('RaftLab');
    expect(html).toContain('队列已空');
    expect(html).toContain('majority-after-split');
  });
});
