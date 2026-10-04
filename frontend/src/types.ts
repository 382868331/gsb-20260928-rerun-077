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
  committed: Record<string, string> | null;
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
  readOnly?: boolean;
  sourcePause?: Selector | null;
  sourceAt?: number;
  forkKey?: string;
}

export interface InterventionRef {
  experimentId: string;
  decisionId: string;
  actionIndex: number;
  at: number;
  label: string;
}

export interface DivergenceAnalysis {
  id: string;
  experimentAId: string;
  experimentBId: string;
  createdAt: string;
  pausePoint?: Selector | null;
  candidate: InterventionRef;
  controlId: string;
  classification: 'arrival-order-only' | 'different-acknowledgements' | 'incomplete';
  sufficient: boolean;
  complete: boolean;
  explanation: string;
  firstDecisionA: Decision;
  firstDecisionB: Decision;
  messageA?: Message | null;
  messageB?: Message | null;
  controlState?: State | null;
  treatedState?: State | null;
  oppositeState?: State | null;
  controlDoc: ExperimentDoc;
  treatedDoc: ExperimentDoc;
}

export interface ExperimentView extends ExperimentDoc {
  state: State | null;
  loadError?: string;
}

export interface ForkRecovery {
  healthySide: string;
  healthyId: string;
  healthyDecisions: Decision[];
  damagedSide: string;
  damagedId: string;
  damagedError: string;
  planned?: ExperimentDoc | null;
  recoverable: boolean;
  reason?: string;
}

export interface ForkView {
  forkKey: string;
  phase: 'partial' | 'recovering' | 'complete' | 'canceled';
  sourcePause: Selector;
  sourceAt: number;
  branchAId: string;
  branchBId: string;
  createdAt: string;
  updatedAt: string;
  recovery?: ForkRecovery | null;
}
