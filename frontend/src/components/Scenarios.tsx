import { useEffect, useState } from "react";
import { useLab } from "../store";
import type { Action } from "../types";

interface Scenario {
  name: string;
  nodes: number;
  seed: number;
  actions: Action[];
}

export function Scenarios() {
  const { send, actions } = useLab();
  const [name, setName] = useState("split-brain");
  const [list, setList] = useState<string[]>([]);

  async function refresh() {
    const res = await fetch("/api/scenarios");
    setList(await res.json());
  }

  useEffect(() => {
    refresh();
  }, []);

  async function save() {
    await fetch("/api/scenarios", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    });
    refresh();
  }

  async function load(scenarioName: string) {
    const res = await fetch(`/api/scenario?name=${encodeURIComponent(scenarioName)}`);
    const scenario: Scenario = await res.json();
    // Loaded into the engine paused at tick 0; press 运行 to reproduce.
    send({ type: "loadScenario", scenario });
  }

  return (
    <div className="scenarios">
      <div className="btn-row">
        <input value={name} onChange={(e) => setName(e.target.value)} placeholder="场景名" />
        <button onClick={save} disabled={actions.length === 0}>
          保存场景
        </button>
        <button onClick={() => send({ type: "play" })} disabled={actions.length === 0}>
          ▶ 复演已载入场景
        </button>
      </div>
      <div className="scenario-list">
        {list.length === 0 && <span className="hint">还没有保存的场景</span>}
        {list.map((n) => (
          <button key={n} className="scenario-chip" onClick={() => load(n)}>
            {n}
          </button>
        ))}
      </div>
    </div>
  );
}
