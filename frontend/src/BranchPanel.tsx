import { useState } from 'react';
import { api } from './api';
import type { ExperimentView, InterventionRef, Selector, State } from './types';

interface Props {
  title: string;
  experiment: ExperimentView;
  pairId: string;
  onChange: (next: ExperimentView) => void;
  onAnalyze: (pairId: string, candidate: InterventionRef) => void;
  onError: (message: string) => void;
}

function inFlight(state: State) {
  return state.inFlight ?? [];
}

export function messageSelector(message: { type: string; from: number; to: number; term: number; data?: string }): Selector {
  return { type: message.type, from: message.from, to: message.to, term: message.term, data: message.data };
}

const networkActions = new Set(['deliver-first', 'postpone', 'delay', 'link', 'partition', 'heal']);

function fallbackState(state: State | null): State {
  return state ?? { now: 0, nodes: [], inFlight: [], events: [], committed: {}, paused: false };
}

function formatSelector(selector: Selector): string {
  return [selector.type, `${selector.from ?? '?'}→${selector.to ?? '?'}`, `term ${selector.term ?? '?'}`].join(' ');
}

export function BranchPanel({ title, experiment, pairId, onChange, onAnalyze, onError }: Props) {
  const [ticks, setTicks] = useState(100);
  const [groups, setGroups] = useState('1,2\n3,4,5');
  const state = fallbackState(experiment.state);
  const decisions = experiment.decisions ?? [];

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
    <section className="branch">
      <header>
        <h3>{title}（实验 {experiment.id}）</h3>
        <span>t {state.now}{state.paused ? ' · 已暂停在投递点' : ''}{experiment.readOnly ? ' · 只读对照' : ''}</span>
      </header>
      {experiment.sourcePause && (
        <div className="source-pause">
          来源暂停状态：t {experiment.sourceAt ?? state.now} · {formatSelector(experiment.sourcePause)}
        </div>
      )}
      {experiment.loadError && <div className="error" role="alert">{experiment.loadError}</div>}
      <div className="branch-controls">
        <label>推进 <input type="number" value={ticks} onChange={(e) => setTicks(Number(e.target.value))} /></label>
        <button disabled={experiment.readOnly} onClick={() => run(api.branch(experiment.id, 'advance', { ticks }))}>推进</button>
        <button disabled={experiment.readOnly} onClick={() => run(api.branch(experiment.id, 'partition', { groups: parsedGroups }))}>分区</button>
        <button disabled={experiment.readOnly} onClick={() => run(api.branch(experiment.id, 'heal'))}>恢复网络</button>
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
          <li key={decision.id} id={`decision-${experiment.id}-${decision.id}`}>t {decision.at} · {decision.label}
            <ul>
              {decision.actions.map((action, i) => (
                <li key={i}>
                  <code>{JSON.stringify(action)}</code>
                  {networkActions.has(action.op) && !experiment.readOnly && pairId && (
                    <button
                      className="analysis-btn"
                      onClick={() => onAnalyze(pairId, {
                        experimentId: experiment.id,
                        decisionId: decision.id,
                        actionIndex: i,
                        at: decision.at,
                        label: decision.label,
                      })}
                    >
                      建立不施加该干预的对照
                    </button>
                  )}
                </li>
              ))}
            </ul>
          </li>
        ))}
      </ol>
    </section>
  );
}
