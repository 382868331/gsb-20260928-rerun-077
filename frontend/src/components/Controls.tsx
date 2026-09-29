import { useState } from "react";
import { useLab } from "../store";

interface Props {
  selected: number | null;
  onSelect: (id: number | null) => void;
}

export function Controls({ selected }: Props) {
  const { send, running, speed, tickMS, snapshot } = useLab();
  const [data, setData] = useState("");
  const [clusterSize, setClusterSize] = useState(5);
  const [seedInput, setSeedInput] = useState("");
  const [dmin, setDmin] = useState(1);
  const [dmax, setDmax] = useState(4);
  const [loss, setLoss] = useState(0);

  const disabled = selected === null || !snapshot;

  return (
    <div className="controls">
      <section>
        <h3>时间</h3>
        <div className="btn-row">
          <button onClick={() => send({ type: running ? "pause" : "play" })}>
            {running ? "⏸ 暂停" : "▶ 运行"}
          </button>
          <button onClick={() => send({ type: "step", ticks: 1 })}>单步 +1</button>
          <button onClick={() => send({ type: "step", ticks: 10 })}>快进 10 tick</button>
        </div>
        <div className="btn-row">
          <label>
            速度 x
            <input
              type="number"
              min={1}
              max={16}
              value={speed}
              onChange={(e) => send({ type: "setTiming", speed: Number(e.target.value) })}
            />
          </label>
          <label>
            tick 毫秒
            <input
              type="number"
              min={10}
              step={10}
              value={tickMS}
              onChange={(e) => send({ type: "setTiming", tickMS: Number(e.target.value) })}
            />
          </label>
        </div>
      </section>

      <section>
        <h3>客户端提案</h3>
        <div className="btn-row">
          <input
            placeholder="写入内容，例如 x=42"
            value={data}
            onChange={(e) => setData(e.target.value)}
            disabled={disabled}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !disabled && data.trim()) {
                send({ type: "propose", node: selected!, data: data.trim() });
                setData("");
              }
            }}
          />
          <button
            disabled={disabled || !data.trim()}
            onClick={() => {
              send({ type: "propose", node: selected!, data: data.trim() });
              setData("");
            }}
          >
            发给 N{selected}
          </button>
        </div>
        <p className="hint">
          发给被隔离的旧领导者：它收不到多数派确认，提案只会停留在未提交状态或被拒绝。
        </p>
      </section>

      <section>
        <h3>网络分区 / 恢复</h3>
        <div className="btn-row">
          <button disabled={disabled} onClick={() => send({ type: "isolate", node: selected! })}>
            断开 N{selected}
          </button>
          <button disabled={disabled} onClick={() => send({ type: "heal", node: selected! })}>
            恢复 N{selected}
          </button>
        </div>
        <div className="btn-row">
          <label>
            延迟 {dmin}-{dmax} tick
            <span className="range-wrap">
              <input type="range" min={0} max={10} value={dmin} onChange={(e) => setDmin(Number(e.target.value))} />
              <input type="range" min={1} max={30} value={dmax} onChange={(e) => setDmax(Number(e.target.value))} />
            </span>
          </label>
          <label>
            丢包 {loss}%
            <input type="range" min={0} max={90} step={5} value={loss} onChange={(e) => setLoss(Number(e.target.value))} />
          </label>
          <button
            disabled={disabled}
            onClick={() =>
              send({
                type: "setLink",
                node: -1,
                nodeB: selected!,
                delayMin: Math.min(dmin, dmax),
                delayMax: Math.max(dmin, dmax),
                lossPct: loss,
              })
            }
          >
            应用到 N{selected} 所有链路
          </button>
        </div>
      </section>

      <section>
        <h3>集群</h3>
        <div className="btn-row">
          <label>
            节点数
            <select value={clusterSize} onChange={(e) => setClusterSize(Number(e.target.value))}>
              {[3, 5, 7].map((k) => (
                <option key={k} value={k}>
                  {k}
                </option>
              ))}
            </select>
          </label>
          <input placeholder="随机种子（可空）" value={seedInput} onChange={(e) => setSeedInput(e.target.value)} />
          <button
            onClick={() =>
              send({ type: "reset", nodes: clusterSize, seed: seedInput ? Number(seedInput) : 0 })
            }
          >
            重置
          </button>
        </div>
      </section>
    </div>
  );
}
