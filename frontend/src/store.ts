import { create } from "zustand";
import type { Action, Event, Snapshot, StateMsg } from "./types";

export interface Command {
  type: string;
  node?: number;
  nodeB?: number;
  data?: string;
  delayMin?: number;
  delayMax?: number;
  lossPct?: number;
  ticks?: number;
  tickMS?: number;
  speed?: number;
  nodes?: number;
  seed?: number;
  name?: string;
  scenario?: unknown;
}

interface LabState {
  connected: boolean;
  running: boolean;
  tickMS: number;
  speed: number;
  snapshot: Snapshot | null;
  events: Event[];
  actions: Action[];
  send: (cmd: Command) => void;
}

let ws: WebSocket | null = null;

export const useLab = create<LabState>((set) => ({
  connected: false,
  running: false,
  tickMS: 60,
  speed: 1,
  snapshot: null,
  events: [],
  actions: [],
  send: (cmd: Command) => {
    ws?.send(JSON.stringify(cmd));
  },
}));

export function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen = () => useLab.setState({ connected: true });
  ws.onclose = () => {
    useLab.setState({ connected: false });
    setTimeout(connect, 1000);
  };
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data) as StateMsg;
    if (msg.type !== "state") return;
    useLab.setState({
      snapshot: msg.snapshot,
      running: msg.running,
      tickMS: msg.tickMS,
      speed: msg.speed,
      actions: msg.scenario,
      events: msg.events,
    });
  };
}
