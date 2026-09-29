import { useState } from "react";
import { useLab } from "./store";
import { ClusterView } from "./components/ClusterView";
import { NodeLogs } from "./components/NodeLog";
import { Controls } from "./components/Controls";
import { Timeline } from "./components/Timeline";
import { Scenarios } from "./components/Scenarios";

export default function App() {
  const { snapshot, events, connected } = useLab();
  const [selected, setSelected] = useState<number | null>(0);

  return (
    <div className="app">
      <header>
        <h1>Raft 交互式实验室</h1>
        <div className="status">
          <span className={connected ? "dot online" : "dot offline"} />
          {connected ? "已连接 Go 仿真进程" : "连接中…"}
          {snapshot && <span className="tick-display">逻辑时钟 t={snapshot.tick}</span>}
        </div>
      </header>

      {!snapshot && <div className="loading">等待仿真状态…</div>}

      {snapshot && (
        <main>
          <div className="grid-top">
            <section className="panel">
              <h2>集群拓扑</h2>
              <ClusterView snapshot={snapshot} selected={selected} onSelect={(id) => setSelected(id)} />
            </section>
            <section className="panel controls-panel">
              <h2>控制台</h2>
              <Controls selected={selected} onSelect={setSelected} />
            </section>
          </div>

          <div className="grid-bottom">
            <section className="panel">
              <h2>各节点日志（绿色 = 多数派已提交）</h2>
              <NodeLogs nodes={snapshot.nodes} />
            </section>
            <section className="panel">
              <h2>事件时间线</h2>
              <Timeline events={events} />
            </section>
          </div>

          <section className="panel">
            <h2>场景保存与复演</h2>
            <Scenarios />
          </section>

          <footer className="lab-notes">
            <p>
              节点仅通过网络中的 RequestVote / AppendEntries 消息获知彼此；没有任何共享的“上帝状态”参与决策。
              红色虚线为断开的链路，半透明领导者表示它已失去多数派联系、其主张不再被集群承认。
            </p>
          </footer>
        </main>
      )}
    </div>
  );
}
