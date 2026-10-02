import { useState } from 'react';
import { api } from './api';
import type { ExperimentView, Selector, State } from './types';

interface Props {
  title: string;
  experiment: ExperimentView;
  onChange: (next: ExperimentView) => void;
  onError: (message: string) => void;
}

function inFlight(state: State) {
  return state.inFlight ?? [];
}

export function messageSelector(message: { type: string; from: number; to: number; term: number; data?: string }): Selector {
  return { type: message.type, from: message.from, to: message.to, term: message.term, data: message.data };
}

export function BranchPanel({ title, experiment, onChange, onError }: Props) {
  const [ticks, setTicks] = useState(100);
  const [groups, setGroups] = useState('1,2\n3,4,5');
  const state = experiment.state;
  const decisions = experiment.decisions ?? [];

  if (!state) {
    return (
      <section className="branch broken" id={`branch-${experiment.id}`}>
        <header>
          <h3>{title}（实验 {experiment.id}）</h3>
        </header>
        <div className="error" role="alert">
          {experiment.loadError || '该实验数据缺失，无法展示；其他实验不受影响。'}
        </div>
        <h4>分支决策追溯（{decisions.length}）</h4>
        <ol className="decisions">
          {decisions.map((decision) => (
            <li key={decision.id}>t {decision.at} · {decision.label}</li>
          ))}
        </ol>
      </section>
    );
  }

  const run = async (promise: Promise<ExperimentView>) => {
    try {
      onChange(await promise);
    } catch (err) {
      onError(err instanceof Error ? err.message : String(err));
    }
  };

  const parsedGroups = groups
    .split('\n')
    .map((line) => line.split(',').map((value) => Number(value.trim())).filter(Boolean))
    .filter((group) => group.length > 0);

  return (
    <section className="branch" id={`branch-${experiment.id}`}>
      <header>
        <h3>{title}（实验 {experiment.id}）</h3>
        <span>t {state.now}{state.paused ? ' · 已暂停在投递点' : ''}</span>
      </header>
      <div className="branch-controls">
        <label>推进 <input type="number" value={ticks} onChange={(e) => setTicks(Number(e.target.value))} /></label>
        <button onClick={() => run(api.branch(experiment.id, 'advance', { ticks }))}>推进</button>
        <button onClick={() => run(api.branch(experiment.id, 'partition', { groups: parsedGroups }))}>分区</button>
        <button onClick={() => run(api.branch(experiment.id, 'heal'))}>恢复网络</button>
      </div>
      <textarea
        className="branch-groups"
        value={groups}
        onChange={(e) => setGroups(e.target.value)}
        placeholder="每行一组，如 1,2"
      />
      <h4>暂停点队列</h4>
      <ul className="messages">
        {inFlight(state).map((message) => (
          <li key={message.id}>
            {message.deliverAt}: {message.type} {message.from}→{message.to} term {message.term}
            {state.paused && (
              <span className="queue-actions">
                <button onClick={() => run(api.branch(experiment.id, 'deliver-first', { selector: messageSelector(message) }))}>
                  优先投递
                </button>
                <button onClick={() => run(api.branch(experiment.id, 'postpone', { selector: messageSelector(message), delay: 300 }))}>
                  延后 300
                </button>
              </span>
            )}
          </li>
        ))}
      </ul>
      <h4>节点确认记录</h4>
      <table>
        <thead><tr><th>节点</th><th>角色</th><th>term</th><th>commit</th><th>日志数据</th></tr></thead>
        <tbody>
          {state.nodes.map((node) => (
            <tr key={node.id}>
              <td>{node.id}</td>
              <td>{node.role}</td>
              <td>{node.term}</td>
              <td>{node.commitIndex}</td>
              <td>{node.log.map((entry) => `${entry.index}:${entry.data}`).join(' | ') || '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <h4>分支决策追溯（{decisions.length}）</h4>
      <ol className="decisions">
        {decisions.map((decision) => (
          <li key={decision.id}>t {decision.at} · {decision.label}
            <ul>
              {(decision.actions ?? []).map((action, i) => (
                <li key={i}>{JSON.stringify(action)}</li>
              ))}
            </ul>
          </li>
        ))}
      </ol>
    </section>
  );
}
