import type { Branch, EntryLifecycle, EntryStage, State } from './types';
import { stageClass, stageLabel } from './lifecycle';

function lifecycleEntries(branch: Branch | null): EntryLifecycle[] {
  if (!branch) return [];
  return Array.from(branch.lifecycle.values()).sort((a, b) => a.index - b.index || a.term - b.term);
}

function nodeStage(branch: Branch | null, lifecycle: EntryLifecycle, nodeId: number): EntryStage | null {
  if (!branch) return null;
  const seen = branch.lifecycle.get(`${lifecycle.term}#${lifecycle.index}:${lifecycle.data}`);
  return seen?.perNode[nodeId]?.stage ?? null;
}

function branchHas(branch: Branch | null, lifecycle: EntryLifecycle, stage: EntryStage, nodeId?: number): boolean {
  if (!branch) return false;
  const seen = branch.lifecycle.get(`${lifecycle.term}#${lifecycle.index}:${lifecycle.data}`);
  if (!seen) return false;
  return Object.entries(seen.perNode).some(([id, info]) =>
    info.stage === stage && (nodeId === undefined || Number(id) === nodeId));
}

function StageCell({ branch, lifecycle, nodeId }: { branch: Branch | null; lifecycle: EntryLifecycle; nodeId: number }) {
  const stage = nodeStage(branch, lifecycle, nodeId);
  if (!stage) return <td className="stage-missing">—</td>;
  return <td className={stageClass[stage]} title={`t=${lifecycle.perNode[nodeId]?.at ?? ''}`}>{stageLabel[stage]}</td>;
}

export function BranchCompare({
  before,
  after,
  nodes,
}: {
  before: Branch | null;
  after: Branch | null;
  nodes: number[];
}) {
  if (!before && !after) {
    return <p className="hint">用工具栏的「捕获分支」分别保存分区前/后或两条复演分支，即可在此比较每条日志在每个节点上的复制、提交与放弃时刻。</p>;
  }
  const all = new Map<string, EntryLifecycle>();
  for (const lifecycle of [...lifecycleEntries(before), ...lifecycleEntries(after)]) {
    all.set(`${lifecycle.term}#${lifecycle.index}:${lifecycle.data}`, lifecycle);
  }
  const entriesList = Array.from(all.values()).sort((a, b) => a.index - b.index || a.term - b.term);
  return (
    <div className="branches">
      <table className="branch-table">
        <thead>
          <tr>
            <th rowSpan={2}>#</th><th rowSpan={2}>term</th><th rowSpan={2}>数据</th>
            {before && <th colSpan={nodes.length}>{before.label}：{before.scenarioName}（t={before.state.now}）</th>}
            {after && <th colSpan={nodes.length}>{after.label}：{after.scenarioName}（t={after.state.now}）</th>}
          </tr>
          <tr>
            {before && nodes.map((id) => <th key={`b-${id}`}>N{id}</th>)}
            {after && nodes.map((id) => <th key={`a-${id}`}>N{id}</th>)}
          </tr>
        </thead>
        <tbody>
          {entriesList.map((lifecycle) => {
            const abandonedBefore = branchHas(before, lifecycle, 'abandoned');
            const abandonedAfter = branchHas(after, lifecycle, 'abandoned');
            const committedBefore = branchHas(before, lifecycle, 'committed');
            const committedAfter = branchHas(after, lifecycle, 'committed');
            const note = committedAfter && !committedBefore ? ' → 已在新分支提交'
              : abandonedAfter && !abandonedBefore ? ' → 在新分支被放弃' : '';
            return (
              <tr key={`${lifecycle.term}-${lifecycle.index}-${lifecycle.data}`}>
                <td>{lifecycle.index}</td>
                <td>{lifecycle.term}</td>
                <td><code>{lifecycle.data}</code>{note && <em className="branch-note">{note}</em>}</td>
                {before && nodes.map((id) => <StageCell key={`cb-${id}`} branch={before} lifecycle={lifecycle} nodeId={id} />)}
                {after && nodes.map((id) => <StageCell key={`ca-${id}`} branch={after} lifecycle={lifecycle} nodeId={id} />)}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function captureBranch(label: string, state: State, scenarioName: string, build: (events: State['events']) => Map<string, EntryLifecycle>): Branch {
  return { label, scenarioName, state, lifecycle: build(state.events) };
}
