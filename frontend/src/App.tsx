import { useEffect, useMemo, useState } from 'react';
import { api } from './api';
import type { Event, NodeView, Scenario, State } from './types';

function roleClass(role: string) {
  return `role ${role}`;
}

function NodeCard({ node }: { node: NodeView }) {
  return (
    <section className="node">
      <header>
        <strong>节点 {node.id}</strong>
        <span className={roleClass(node.role)}>{node.role}</span>
        <span>term {node.term}</span>
      </header>
      <div className="meta">投票给 {node.votedFor ?? '—'} · commitIndex {node.commitIndex}</div>
      <ol>
        {node.log.map((entry) => (
          <li key={entry.index} className={entry.index <= node.commitIndex ? 'committed' : 'uncommitted'}>
            <b>#{entry.index}</b>
            <span>t{entry.term}</span>
            <code>{entry.data}</code>
            {entry.index <= node.commitIndex && <em>已提交</em>}
          </li>
        ))}
      </ol>
    </section>
  );
}

function EventLine({ event }: { event: Event }) {
  const message = event.message ? `${event.message.type} ${event.message.from}→${event.message.to}` : '';
  return (
    <tr>
      <td>{event.t}</td>
      <td>{event.kind}</td>
      <td>{event.node ?? ''}</td>
      <td>{event.term ?? ''}</td>
      <td>{message || event.detail || ''}</td>
      <td>{event.data || ''}</td>
    </tr>
  );
}

export default function App() {
  const [state, setState] = useState<State | null>(null);
  const [scenario, setScenario] = useState<Scenario | null>(null);
  const [node, setNode] = useState(1);
  const [data, setData] = useState('client-write');
  const [groups, setGroups] = useState('1,2\n3,4,5');
  const [from, setFrom] = useState(1);
  const [to, setTo] = useState(2);
  const [delay, setDelay] = useState(80);
  const [customJson, setCustomJson] = useState('');
  const [error, setError] = useState('');

  const refresh = async (promise: Promise<State>) => {
    try {
      setState(await promise);
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  useEffect(() => {
    Promise.all([api.state(), api.currentScenario()]).then(([nextState, nextScenario]) => {
      setState(nextState);
      setScenario(nextScenario);
      setCustomJson(JSON.stringify(nextScenario, null, 2));
    }).catch((err) => setError(String(err)));
  }, []);

  const parsedGroups = useMemo(() => {
    return groups.split('\n').map((line) => line.split(',').map((value) => Number(value.trim())).filter(Boolean));
  }, [groups]);

  const replayBuiltin = async (kind: 'minority' | 'old') => {
    const seed = kind === 'minority' ? 7 : 11;
    const next: Scenario = kind === 'minority'
      ? {
          name: 'minority-cannot-commit', description: '少数派旧领导者不能提交', nodes: [1, 2, 3, 4, 5], seed, initialLeader: 1,
          actions: [
            { op: 'client', at: 20, node: 1, data: 'initial-majority' },
            { op: 'partition', at: 260, groups: [[1], [2, 3, 4, 5]] },
            { op: 'client', at: 390, node: 1, data: 'minority-stale' },
            { op: 'client', at: 700, node: 4, data: 'new-majority' },
            { op: 'heal', at: 900 },
          ],
        }
      : {
          name: 'old-leader-catches-up', description: '旧领导者恢复后回滚分叉', nodes: [1, 2, 3, 4, 5], seed, initialLeader: 1,
          actions: [
            { op: 'client', at: 20, node: 1, data: 'committed-before-split' },
            { op: 'partition', at: 240, groups: [[1, 2], [3, 4, 5]] },
            { op: 'client', at: 390, node: 1, data: 'old-leader-unconfirmed' },
            { op: 'client', at: 700, node: 4, data: 'majority-after-split' },
            { op: 'heal', at: 900 },
          ],
        };
    setScenario(next);
    setCustomJson(JSON.stringify(next, null, 2));
    await refresh(api.replay(next));
  };

  const replayCustom = async () => {
    const next = JSON.parse(customJson) as Scenario;
    setScenario(next);
    await refresh(api.replay(next));
  };

  const saveCurrent = async () => {
    if (!scenario) return;
    await api.save(scenario.name, scenario);
    const saved = await api.currentScenario();
    setScenario(saved);
    setCustomJson(JSON.stringify(saved, null, 2));
  };

  return (
    <main>
      <h1>RaftLab：领导者选举与日志复演</h1>
      {error && <div className="error">{error}</div>}
      <section className="toolbar">
        <div>虚拟时间 <b>{state?.now ?? 0}</b> ticks</div>
        <button onClick={() => state && refresh(api.advance(50))}>推进 50</button>
        <button onClick={() => state && refresh(api.advance(250))}>推进 250</button>
        <button onClick={() => refresh(api.heal())}>恢复全部链路</button>
        <button onClick={() => replayBuiltin('minority')}>复演：少数派失败</button>
        <button onClick={() => replayBuiltin('old')}>复演：旧领导追平</button>
      </section>

      <section className="controls">
        <label>客户端请求发给节点
          <input type="number" value={node} min={1} max={5} onChange={(event) => setNode(Number(event.target.value))} />
        </label>
        <input value={data} onChange={(event) => setData(event.target.value)} />
        <button onClick={() => refresh(api.client(node, data))}>追加日志</button>
        <label>分组（每行一组）<textarea value={groups} onChange={(event) => setGroups(event.target.value)} /></label>
        <button onClick={() => refresh(api.partition(parsedGroups))}>制造分区</button>
        <label>链路 <input type="number" value={from} onChange={(e) => setFrom(Number(e.target.value))} /></label>
        <span>→</span>
        <label><input type="number" value={to} onChange={(e) => setTo(Number(e.target.value))} /></label>
        <label>延迟 <input type="number" value={delay} onChange={(e) => setDelay(Number(e.target.value))} /></label>
        <button onClick={() => refresh(api.link(from, to, { delay }))}>设置延迟</button>
        <button onClick={() => refresh(api.link(from, to, { up: false }))}>断开链路</button>
        <button onClick={() => refresh(api.link(from, to, { up: true }))}>恢复链路</button>
      </section>

      <section className="grid">{state?.nodes.map((nodeView) => <NodeCard key={nodeView.id} node={nodeView} />)}</section>

      <section className="split">
        <div>
          <h2>在途消息</h2>
          <ul className="messages">
            {state?.inFlight.map((message) => (
              <li key={message.id}>{message.deliverAt}: {message.type} {message.from}→{message.to} term {message.term}</li>
            ))}
          </ul>
          <h2>场景 JSON / 保存</h2>
          <textarea className="scenario" value={customJson} onChange={(event) => setCustomJson(event.target.value)} />
          <button onClick={replayCustom}>按 JSON 复演</button>
          <button onClick={saveCurrent}>保存当前场景</button>
        </div>
        <div>
          <h2>事件顺序（最近 300 条）</h2>
          <table>
            <thead><tr><th>t</th><th>事件</th><th>节点</th><th>term</th><th>消息/细节</th><th>数据</th></tr></thead>
            <tbody>{state?.events.slice().reverse().map((event, index) => <EventLine key={`${event.t}-${event.kind}-${index}`} event={event} />)}</tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
