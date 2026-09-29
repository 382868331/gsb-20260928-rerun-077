import { useMemo } from "react";
import type { Snapshot } from "../types";

interface Props {
  snapshot: Snapshot;
  selected: number | null;
  onSelect: (id: number) => void;
}

export function linkKey(a: number, b: number) {
  return a < b ? `${a}-${b}` : `${b}-${a}`;
}

const roleColor: Record<string, string> = {
  leader: "#22c55e",
  candidate: "#f59e0b",
  follower: "#64748b",
};

export function ClusterView({ snapshot, selected, onSelect }: Props) {
  const n = snapshot.nodes.length;
  const positions = useMemo(() => {
    const cx = 300;
    const cy = 220;
    const r = 150;
    return snapshot.nodes.map((_, i) => {
      const angle = (2 * Math.PI * i) / n - Math.PI / 2;
      return { x: cx + r * Math.cos(angle), y: cy + r * Math.sin(angle) };
    });
  }, [snapshot.nodes, n]);

  return (
    <svg viewBox="0 0 600 440" className="cluster-svg">
      {snapshot.nodes.map((a, i) =>
        snapshot.nodes.slice(i + 1).map((b) => {
          const pa = positions[a.id];
          const pb = positions[b.id];
          const link = snapshot.links[linkKey(a.id, b.id)];
          const up = link?.up ?? true;
          return (
            <g key={`${a.id}-${b.id}`}>
              <line
                x1={pa.x}
                y1={pa.y}
                x2={pb.x}
                y2={pb.y}
                stroke={up ? "#94a3b8" : "#ef4444"}
                strokeWidth={up ? 2 : 2}
                strokeDasharray={up ? undefined : "6 6"}
                opacity={up ? 0.8 : 0.9}
              />
              {link && up && (link.delayMax > 2 || link.lossPct > 0) && (
                <text
                  x={(pa.x + pb.x) / 2}
                  y={(pa.y + pb.y) / 2 - 4}
                  textAnchor="middle"
                  className="link-label"
                >
                  {link.delayMin}-{link.delayMax}t
                  {link.lossPct > 0 ? ` ${link.lossPct}%丢` : ""}
                </text>
              )}
            </g>
          );
        }),
      )}
      {snapshot.nodes.map((node) => {
        const p = positions[node.id];
        const isLeader = node.id === snapshot.leader;
        const reachable = snapshot.nodes.every(
          (other) => other.id === node.id || snapshot.links[linkKey(node.id, other.id)]?.up,
        );
        return (
          <g
            key={node.id}
            transform={`translate(${p.x},${p.y})`}
            onClick={() => onSelect(node.id)}
            className="node-group"
          >
            {isLeader && <circle r={36} fill="none" stroke="#22c55e" strokeWidth={3} className="leader-ring" />}
            <circle
              r={28}
              fill={roleColor[node.role]}
              stroke={selected === node.id ? "#0ea5e9" : "#1e293b"}
              strokeWidth={selected === node.id ? 4 : 2}
              opacity={node.role === "leader" && !reachable ? 0.45 : 1}
            />
            <text textAnchor="middle" dy={6} className="node-label">
              N{node.id}
            </text>
            <text textAnchor="middle" y={48} className="node-term">
              T{node.term} · {node.role === "leader" ? (reachable ? "领导者" : "失联领导") : node.role === "candidate" ? "候选" : "跟随"}
            </text>
          </g>
        );
      })}
      <text x={12} y={22} className="svg-title">
        多数派 = {snapshot.quorum} 节点{" "}
        {snapshot.leader >= 0 ? `· 当前有效领导者：N${snapshot.leader}` : "· 无有效领导者（选举中或无多数派）"}
      </text>
    </svg>
  )
}
