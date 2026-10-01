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
  message?: Message;
  from?: number;
  to?: number;
}

export interface State {
  now: number;
  nodes: NodeView[];
  inFlight: Message[];
  events: Event[];
  committed: Record<string, string>;
}

export interface Decision {
  at: number;
  tick?: number;
  op: string;
  detail: string;
}

export interface BranchView {
  name: string;
  state: State;
  decisions: Decision[];
}

export interface ExperimentView {
  id: string;
  label: string;
  messageId: number;
  messageType: string;
  from: number;
  to: number;
  pausedAt: number;
  createdAt: string;
  branches: Record<string, BranchView>;
}

export interface Scenario {
  name: string;
  description: string;
  nodes: number[];
  seed: number;
  initialLeader?: number;
  actions: Array<Record<string, unknown>>;
}
