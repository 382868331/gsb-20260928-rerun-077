export type Role = "follower" | "candidate" | "leader";

export interface Entry {
  index: number;
  term: number;
  data: string;
}

export interface NodeSnapshot {
  id: number;
  role: Role;
  term: number;
  votedFor: number;
  leaderId: number;
  commitIdx: number;
  log: { entries: Entry[]; commit: number };
}

export interface LinkState {
  up: boolean;
  delayMin: number;
  delayMax: number;
  lossPct: number;
}

export interface Snapshot {
  tick: number;
  nodes: NodeSnapshot[];
  links: Record<string, LinkState>;
  leader: number;
  quorum: number;
}

export interface Event {
  tick: number;
  kind: string;
  a: number;
  b: number;
  term: number;
  text: string;
}

export interface Action {
  tick: number;
  kind: string;
  data?: string;
  clientId?: string;
  a?: number;
  b?: number;
  delayMin?: number;
  delayMax?: number;
  lossPct?: number;
  nodes?: number;
  seed?: number;
}

export interface StateMsg {
  type: "state";
  snapshot: Snapshot;
  events: Event[];
  running: boolean;
  tickMS: number;
  speed: number;
  scenario: Action[];
}
