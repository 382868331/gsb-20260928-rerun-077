import { useEffect, useMemo, useRef, useState } from 'react';
import { api } from './api';
import type { Branch, Event, NodeView, Scenario, State } from './types';
import { buildLifecycle, committedEntries, normalizeState } from './lifecycle';
import { BranchCompare, captureBranch } from './BranchCompare';

function roleClass(role: string) {
  return `role ${role}`;
}

function NodeCard({ node }: { node: NodeView }) {
  const empty = node.log.length === 0;
  return (
    <section className="node">
      <header>
        <strong>节点 {node.id}</strong>
        <span className={roleClass(node.role)}>{node.role}</span>
        <span>term {node.term}</span>
      </header>
      <div className="meta">投票给 {node.votedFor ?? '—'} · commitIndex {node.commitIndex} · 下次超时 t{node.deadline}</div>
      <ol>
        {empty && <li className="empty-log">暂无日志（尚未选出领导者 / 尚无客户端写入）</li>}
        {node.log.map((entry) => {
          const localCommit = entry.index <= node.commitIndex;
          return (
            <li key={entry.index} className={localCommit ? 'committed' : 'uncommitted'}>
              <b>#{entry.index}</b>
              <span>t{entry.term}</span>
              <code>{entry.data}</code>
              {localCommit
                ? <em>本地已提交</em>
                : <em className="local-only">仅本地未确认（未达成多数派）</em>}
            </li>
          );
        })}
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
  const [notice, setNotice] = useState('');
  const [paused, setPaused] = useState(false);
  const [busy, setBusy] = useState(false);
  const [branchBefore, setBranchBefore] = useState<Branch | null>(null);
  const [branchAfter, setBranchAfter] = useState<Branch | null>(null);
  const requestSeq = useRef(0);

  // Every mutation gets a monotonically increasing token. If the user switches
  // scenario while an older request is in flight, the stale response is dropped
  // instead of painting another branch's node state onto the new view.
  const refresh = async (promise: Promise<State>, silent = false) => {
    const seq = ++requestSeq.current;
    setBusy(true);
    try {
      const next = normalizeState(await promise);
      if (seq !== requestSeq.current) return;
      setState(next);
      setError('');
      if (!silent) {
        const rejected = next.events.slice(-30).some(
          (event) => event.kind === 'note' && event.detail === 'rejected-not-leader');
        setNotice(rejected
          ? '该节点不是当前任期的领导者：请求被节点自己拒绝，日志未追加、更未提交。'
          : '');
      }
    } catch (err) {
      if (seq === requestSeq.current) setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (seq === requestSeq.current) setBusy(false);
    }
  };

  useEffect(() => {
    Promise.all([api.state(), api.currentScenario()]).then(([nextState, nextScenario]) => {
      setState(normalizeState(nextState));
      setScenario(nextScenario);
      setCustomJson(JSON.stringify(nextScenario, null, 2));
    }).catch((err) => setError(String(err)));
  }, []);

  const parsedGroups = useMemo(() => {
    return groups.split('\n').map((line) => line.split(',').map((value) => Number(value.trim())).filter(Boolean));
  }, [groups]);

  const builtinScenario = (kind: 'minority' | 'old'): Scenario => kind === 'minority'
    ? {
        name: 'minority-cannot-commit', description: '少数派旧领导者不能提交', nodes: [1, 2, 3, 4, 5], seed: 7, initialLeader: 1,
        actions: [
          { op: 'client', at: 20, node: 1, data: 'initial-majority' },
          { op: 'partition', at: 260, groups: [[1], [2, 3, 4, 5]] },
          { op: 'client', at: 390, node: 1, data: 'minority-stale' },
          { op: 'client', at: 700, node: 4, data: 'new-majority' },
          { op: 'heal', at: 900 },
        ],
      }
    : {
        name: 'old-leader-catches-up', description: '旧领导者恢复后回滚分叉', nodes: [1, 2, 3, 4, 5], seed: 11, initialLeader: 1,
        actions: [
          { op: 'client', at: 20, node: 1, data: 'committed-before-split' },
          { op: 'partition', at: 240, groups: [[1, 2], [3, 4, 5]] },
          { op: 'client', at: 390, node: 1, data: 'old-leader-unconfirmed' },
          { op: 'client', at: 700, node: 4, data: 'majority-after-split' },
          { op: 'heal', at: 900 },
        ],
      };

  const replayBuiltin = async (kind: 'minority' | 'old') => {
    const next = builtinScenario(kind);
    setScenario(next);
    setCustomJson(JSON.stringify(next, null, 2));
    setNotice('');
    setBranchBefore(null);
    setBranchAfter(null);
    await refresh(api.replay(next), true);
  };

  const replayCustom = async () => {
    const next = JSON.parse(customJson) as Scenario;
    setScenario(next);
    setBranchBefore(null);
    setBranchAfter(null);
    await refresh(api.replay(next), true);
  };

  const saveCurrent = async () => {
    if (!scenario) return;
    await api.save(scenario.name, scenario);
    const saved = await api.currentScenario();
    setScenario(saved);
    setCustomJson(JSON.stringify(saved, null, 2));
  };

  const leader = state?.nodes.find((nodeView) => nodeView.role === 'leader');
  const committed = useMemo(() => (state ? committedEntries(state) : new Set<string>()), [state]);
  const localOnly = useMemo(() => {
    if (!state) return [] as Array<{ node: number; data: string }>;
    const rows: Array<{ node: number; data: string }> = [];
    for (const nodeView of state.nodes) {
      for (const entry of nodeView.log) {
        const key = `${entry.term}#${entry.index}:${entry.data}`;
        if (entry.index <= nodeView.commitIndex && !committed.has(key)) {
          rows.push({ node: nodeView.id, data: entry.data });
        }
      }
    }
    return rows;
  }, [state, committed]);

  const capture = (slot: 'before' | 'after') => {
    if (!state || !scenario) return;
    const branch = captureBranch(
      slot === 'before' ? '分支 A（分区前/旧领导者）' : '分支 B（分区后/新多数派）',
      state, scenario.name, buildLifecycle);
    if (slot === 'before') setBranchBefore(branch);
    else setBranchAfter(branch);
  };

  const disabled = paused || busy || !state;

  return (
    <main>
      <h1>RaftLab：领导者选举与日志复演</h1>
      {error && <div className="error">{error}</div>}
      {notice && <div className="notice">{notice}</div>}
      <section className="toolbar">
        <div>虚拟时间 <b>{state?.now ?? 0}</b> ticks</div>
        <button disabled={disabled} onClick={() => state && refresh(api.advance(50))}>推进 50</button>
        <button disabled={disabled} onClick={() => state && refresh(api.advance(250))}>推进 250</button>
        <button disabled={disabled} onClick={() => refresh(api.heal())}>恢复全部链路</button>
        <button className={paused ? 'resume' : 'pause'} onClick={() => setPaused((value) => !value)}>
          {paused ? '继续操作' : '暂停'}
        </button>
        <button disabled={disabled} onClick={() => replayBuiltin('minority')}>复演：少数派失败</button>
        <button disabled={disabled} onClick={() => replayBuiltin('old')}>复演：旧领导追平</button>
        <span className="sep" />
        <button disabled={!state} onClick={() => capture('before')}>捕获分支 A</button>
        <button disabled={!state} onClick={() => capture('after')}>捕获分支 B</button>
        <button onClick={() => { setBranchBefore(null); setBranchAfter(null); }}>清空比较</button>
      </section>

      <section className="status-bar">
        {leader
          ? <span>当前任期 {leader.term} 的领导者：<b>节点 {leader.id}</b></span>
          : <span className="no-leader">尚未选出领导者：节点会继续随机超时发起选举，页面保持可观察。</span>}
        {paused && <span className="paused-tag">已暂停：改变模拟器状态的操作已锁定，分支视图不会被新响应覆盖。</span>}
        {localOnly.length > 0 && (
          <span className="local-only-tag">
            ⚠ 节点 {Array.from(new Set(localOnly.map((row) => row.node))).join('、')} 仅有本地提交、未获得多数派确认（
            {Array.from(new Set(localOnly.map((row) => row.data))).join('、')}）；这不是集群成功提交。
          </span>
        )}
      </section>

      <section className={disabled ? 'controls disabled-panel' : 'controls'}>
        <label>客户端请求发给节点
          <input type="number" value={node} min={1} max={5} onChange={(event) => setNode(Number(event.target.value))} />
        </label>
        <input value={data} onChange={(event) => setData(event.target.value)} />
        <button disabled={disabled} onClick={() => refresh(api.client(node, data))}>追加日志</button>
        <label>分组（每行一组）<textarea value={groups} onChange={(event) => setGroups(event.target.value)} /></label>
        <button disabled={disabled} onClick={() => refresh(api.partition(parsedGroups))}>制造分区</button>
        <label>链路 <input type="number" value={from} onChange={(e) => setFrom(Number(e.target.value))} /></label>
        <span>→</span>
        <label><input type="number" value={to} onChange={(e) => setTo(Number(e.target.value))} /></label>
        <label>延迟 <input type="number" value={delay} onChange={(e) => setDelay(Number(e.target.value))} /></label>
        <button disabled={disabled} onClick={() => refresh(api.link(from, to, { delay }))}>设置延迟</button>
        <button disabled={disabled} onClick={() => refresh(api.link(from, to, { up: false }))}>断开链路</button>
        <button disabled={disabled} onClick={() => refresh(api.link(from, to, { up: true }))}>恢复链路</button>
      </section>

      <section className="committed-view">
        <h2>多数派已确认视图</h2>
        {committed.size === 0
          ? <span className="hint">目前没有任何日志获得多数派提交。少数派 / 无领导者时这里保持为空，不会显示成功。</span>
          : Array.from(committed).sort().map((key) => {
              const data = key.slice(key.indexOf(':') + 1);
              return <span key={key} className="committed-pill">✓ {data}</span>;
            })}
      </section>

      <section className="grid">{state?.nodes.map((nodeView) => <NodeCard key={nodeView.id} node={nodeView} />)}</section>

      <section className="compare">
        <h2>分区前后两条演练分支比较</h2>
        <BranchCompare
          before={branchBefore}
          after={branchAfter}
          nodes={scenario?.nodes ?? state?.nodes.map((nodeView) => nodeView.id) ?? []}
        />
      </section>

      <section className="split">
        <div>
          <h2>在途消息</h2>
          <ul className="messages">
            {state?.inFlight.length === 0 && <li className="hint">消息队列为空（分区中 / 心跳尚未发出），页面正常。</li>}
            {state?.inFlight.map((message) => (
              <li key={message.id}>{message.deliverAt}: {message.type} {message.from}→{message.to} term {message.term}</li>
            ))}
          </ul>
          <h2>场景 JSON / 保存</h2>
          <textarea className="scenario" value={customJson} onChange={(event) => setCustomJson(event.target.value)} />
          <button disabled={disabled} onClick={replayCustom}>按 JSON 复演</button>
          <button disabled={busy} onClick={saveCurrent}>保存当前场景</button>
        </div>
        <div>
          <h2>事件顺序（最近 300 条；复演返回完整事件）</h2>
          <table>
            <thead><tr><th>t</th><th>事件</th><th>节点</th><th>term</th><th>消息/细节</th><th>数据</th></tr></thead>
            <tbody>{state?.events.slice().reverse().map((event, index) => <EventLine key={`${event.t}-${event.kind}-${index}`} event={event} />)}</tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
