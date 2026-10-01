import { useState } from 'react';
import { api } from './api';
import type { ExperimentView, State } from './types';

function BranchPanel({
  experiment,
  branchKey,
  branch,
  onChange,
}: {
  experiment: ExperimentView;
  branchKey: string;
  branch: ExperimentView['branches'][string];
  onChange: (next: ExperimentView) => void;
}) {
  const [ticks, setTicks] = useState(50);
  const [from, setFrom] = useState(1);
  const [to, setTo] = useState(2);
  const [delay, setDelay] = useState(80);
  const [error, setError] = useState('');
  const state: State = branch.state;
  const queued = state.inFlight ?? [];
  const run = async (promise: Promise<ExperimentView>) => {
    try {
      onChange(await promise);
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <div className="branch">
      <h3>{branch.name}</h3>
      <div className="branch-controls">
        <label>推进 <input type="number" value={ticks} onChange={(e) => setTicks(Number(e.target.value))} /></label>
        <button onClick={() => run(api.expAdvance(experiment.id, branchKey, ticks))}>推进</button>
        <label>链路 <input type="number" value={from} onChange={(e) => setFrom(Number(e.target.value))} /></label>
        <span>→</span>
        <label><input type="number" value={to} onChange={(e) => setTo(Number(e.target.value))} /></label>
        <label>延迟 <input type="number" value={delay} onChange={(e) => setDelay(Number(e.target.value))} /></label>
        <button onClick={() => run(api.expLink(experiment.id, branchKey, { from, to, delay }))}>设置延迟</button>
        <button onClick={() => run(api.expLink(experiment.id, branchKey, { from, to, up: false }))}>断开</button>
        <button onClick={() => run(api.expLink(experiment.id, branchKey, { from, to, up: true }))}>恢复</button>
      </div>
      {error && <div className="error">{error}</div>}
      <h4>在途消息（可调整投递顺序）</h4>
      <ul className="messages">
        {queued.length === 0 && <li className="empty">队列已空，所有消息都已投递。</li>}
        {queued.map((message) => (
          <li key={message.id}>
            {message.deliverAt}: {message.type} {message.from}→{message.to} term {message.term}
            <button onClick={() => run(api.expReorder(experiment.id, branchKey, message.id))}>优先投递</button>
          </li>
        ))}
      </ul>
      <h4>节点确认记录</h4>
      <table>
        <thead><tr><th>节点</th><th>角色</th><th>term</th><th>commitIndex</th><th>日志（绿=已确认）</th></tr></thead>
        <tbody>
          {(state.nodes ?? []).map((node) => (
            <tr key={node.id}>
              <td>{node.id}</td>
              <td>{node.role}</td>
              <td>{node.term}</td>
              <td>{node.commitIndex}</td>
              <td>
                {node.log.map((entry) => (
                  <span key={entry.index} className={entry.index <= node.commitIndex ? 'tag committed' : 'tag uncommitted'}>
                    #{entry.index} {entry.data}
                  </span>
                ))}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <h4>决定追溯</h4>
      <ol className="decisions">
        {branch.decisions.map((decision, index) => (
          <li key={index}>t={decision.at}{decision.tick ? ` (+${decision.tick})` : ''} · {decision.op} · {decision.detail}</li>
        ))}
      </ol>
    </div>
  );
}

export function ExperimentCard({
  experiment,
  onChange,
}: {
  experiment: ExperimentView;
  onChange: (next: ExperimentView) => void;
}) {
  return (
    <section className="experiment">
      <h3>
        {experiment.label} <small>（暂停点 t={experiment.pausedAt}，消息 {experiment.messageType} {experiment.from}→{experiment.to}，id={experiment.messageId}）</small>
      </h3>
      <div className="branch-grid">
        {['a', 'b'].map((key) => {
          const branch = experiment.branches[key];
          if (!branch) return null;
          return <BranchPanel key={key} experiment={experiment} branchKey={key} branch={branch} onChange={onChange} />;
        })}
      </div>
    </section>
  );
}
