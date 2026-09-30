export type Role = 'leader' | 'candidate' | 'follower';

export interface Entry {
  index: number;
  term: number;
  data: string;
}

export interface NodeView {
  id: number;
  role: Role;
  term: number;
  votedFor: number | null;
  commitIndex: number;
  log: Entry[];
  deadline: number;
}

export interface Message {
  id: number;
  type: string;
  from: number;
  to: number;
  term: number;
  data?: string;
  deliverAt: number;
}

export interface Event {
  t: number;
  kind: string;
  node?: number;
  term?: number;
  role?: Role;
  detail?: string;
  data?: string;
  index?: number;
  from?: number;
  message?: Message;
  to?: number;
  commit?: number;
  delay?: number;
}

export interface State {
  now: number;
  nodes: NodeView[];
  links?: Record<string, { from: number; to: number; delay: number; up: boolean }>;
  inFlight: Message[];
  events: Event[];
  committed: Record<string, string>;
}

export interface ScenarioAction {
  op: 'advance' | 'delay' | 'link' | 'partition' | 'heal' | 'client';
  at: number;
  from?: number;
  to?: number;
  node?: number;
  delay?: number;
  up?: boolean;
  data?: string;
  groups?: number[][];
}

export interface Scenario {
  name: string;
  description: string;
  nodes: number[];
  seed: number;
  initialLeader?: number;
  actions: ScenarioAction[];
}

export type EntryStage = 'appended' | 'replicated' | 'committed' | 'abandoned';

export interface EntryLifecycle {
  data: string;
  index: number;
  term: number;
  perNode: Record<number, { stage: EntryStage; at: number }>;
}

export interface Branch {
  label: string;
  scenarioName: string;
  state: State;
  lifecycle: Map<string, EntryLifecycle>;
}
