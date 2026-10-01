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
  inFlight: Message[] | null;
  events: Event[];
  committed: Record<string, string>;
  paused: boolean;
  pause?: Selector | null;
}

export interface Selector {
  type?: string;
  from?: number;
  to?: number;
  term?: number;
  data?: string;
}

export interface Scenario {
  name: string;
  description: string;
  nodes: number[];
  seed: number;
  initialLeader?: number;
  actions: Array<Record<string, unknown>>;
}

export interface Action {
  op: string;
  at: number;
  from?: number;
  to?: number;
  node?: number;
  delay?: number;
  up?: boolean;
  data?: string;
  groups?: number[][];
  term?: number;
  msgType?: string;
}

export interface Decision {
  id: string;
  kind: string;
  label: string;
  at: number;
  actions: Action[];
}

export interface ExperimentDoc {
  id: string;
  name: string;
  base: Scenario;
  decisions: Decision[];
  createdAt: string;
}

export interface ExperimentView extends ExperimentDoc {
  state: State;
}
