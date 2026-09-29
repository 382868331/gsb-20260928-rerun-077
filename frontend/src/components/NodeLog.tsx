import type { NodeSnapshot } from "../types";

interface Props {
  nodes: NodeSnapshot[];
}

const roleBadge: Record<string, string> = {
  leader: "badge-leader",
  candidate: "badge-candidate",
  follower: "badge-follower",
};

export function NodeLogs({ nodes }: Props) {
  const maxLen = Math.max(...nodes.map((n) => n.log.entries.length), 0);
  return (
    <div className="node-logs">
      {nodes.map((node) => (
        <div key={node.id} className="node-card">
          <div className="node-card-head">
            <span className={`badge ${roleBadge[node.role]}`}>N{node.id}</span>
            <span className="meta">任期 {node.term}</span>
            <span className="meta">投票: {node.votedFor < 0 ? "-" : `N${node.votedFor}`}</span>
            <span className="meta">已提交: {node.commitIdx}</span>
          </div>
          <div className="log-rows">
            {Array.from({ length: Math.max(maxLen, 1) }, (_, i) => {
              const entry = node.log.entries[i];
              const idx = i + 1;
              const committed = idx <= node.commitIdx;
              if (!entry) {
                return (
                  <div key={idx} className="log-row empty">
                    <span className="log-idx">{idx}</span>
                    <span className="log-data">—</span>
                  </div>
                );
              }
              return (
                <div key={idx} className={`log-row ${committed ? "committed" : "uncommitted"}`}>
                  <span className="log-idx">#{entry.index}</span>
                  <span className="log-term">T{entry.term}</span>
                  <span className="log-data" title={entry.data}>{entry.data}</span>
                  <span className="log-state">{committed ? "✓ 已提交" : "未提交"}</span>
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}
