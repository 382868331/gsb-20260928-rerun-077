import { useEffect, useMemo, useState } from 'react';
import { api } from './api';
import { BranchPanel, messageSelector } from './BranchPanel';
import { ErrorBoundary } from './ErrorBoundary';
import type { DivergenceAnalysis, Event, ExperimentView, ForkView, InterventionRef, NodeView, Scenario, Selector, State } from './types';

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

const inFlight = (state: State | null) => state?.inFlight ?? [];

function formatMessage(message: NonNullable<DivergenceAnalysis['messageA']> | null | undefined) {
  return message ? `${message.type} ${message.from}→${message.to} t${message.term} @${message.deliverAt}` : '消息记录缺失';
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
  const [branches, setBranches] = useState<Record<string, ExperimentView>>({});
  const [analyses, setAnalyses] = useState<DivergenceAnalysis[]>([]);
  const [forks, setForks] = useState<ForkView[]>([]);

  const refresh = async (promise: Promise<State>) => {
    try {
      setState(await promise);
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const loadExperiments = async () => {
    try {
      const views = await api.experiments();
      const next: Record<string, ExperimentView> = {};
      for (const view of views) {
        next[view.id] = view;
      }
      setBranches(next);
      setAnalyses(await api.analyses());
      setForks(await api.forks());
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const confirmRecover = async (forkKey: string) => {
    try {
      await api.recoverFork(forkKey);
      await loadExperiments();
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const cancelRecover = async (forkKey: string) => {
    try {
      await api.cancelFork(forkKey);
      await loadExperiments();
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const previewRecover = async (forkKey: string) => {
    try {
      await api.previewFork(forkKey);
      await loadExperiments();
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  useEffect(() => {
    Promise.all([api.state(), api.currentScenario()])
      .then(([nextState, nextScenario]) => {
        setState(nextState);
        setScenario(nextScenario);
        setCustomJson(JSON.stringify(nextScenario, null, 2));
      })
      .catch((err) => setError(String(err)));
    void loadExperiments();
  }, []);

  const parsedGroups = useMemo(() => {
    return groups
      .split('\n')
      .map((line) => line.split(',').map((value) => Number(value.trim())).filter(Boolean))
      .filter((group) => group.length > 0);
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
    let next: Scenario;
    try {
      next = JSON.parse(customJson) as Scenario;
    } catch (err) {
      setError(`场景 JSON 无法解析，当前工作区保持不变：${err instanceof Error ? err.message : String(err)}`);
      return;
    }
    setScenario(next);
    await refresh(api.replay(next));
  };

  const saveCurrent = async () => {
    if (!scenario) return;
    try {
      await api.save(scenario.name, scenario);
      const saved = await api.currentScenario();
      setScenario(saved);
      setCustomJson(JSON.stringify(saved, null, 2));
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const armPause = async (selector: Selector) => {
    await refresh(api.armPause(selector));
  };

  const forkAtPause = async () => {
    if (!state?.paused || !state.pause) {
      setError('当前没有暂停点：请先在在途消息上选择“投递前暂停”。');
      return;
    }
    try {
      const pair = await api.fork(state.pause);
      setBranches((prev) => ({ ...prev, [pair.a.id]: pair.a, [pair.b.id]: pair.b }));
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const updateBranch = (next: ExperimentView) => {
    setBranches((prev) => ({ ...prev, [next.id]: next }));
  };

  const analyzeIntervention = async (otherId: string, candidate: InterventionRef) => {
    const other = branches[otherId];
    const current = branches[candidate.experimentId];
    if (!other || !current) {
      setError('缺少对照轨迹，无法建立分析。');
      return;
    }
    const [a, b] = current.name === 'a' ? [current, other] : [other, current];
    try {
      const analysis = await api.analyze(a.id, b.id, candidate);
      const views = await api.experiments();
      setBranches((prev) => {
        const next = { ...prev };
        for (const view of views) next[view.id] = view;
        return next;
      });
      setAnalyses((prev) => [...prev, analysis]);
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const pairedBranch = (experiment: ExperimentView) => {
    if (experiment.name === 'a') return experiment.id.replace(/-a$/, '-b');
    if (experiment.name === 'b') return experiment.id.replace(/-b$/, '-a');
    return '';
  };

  const safeState = (value: State | null): State => value ?? { now: 0, nodes: [], inFlight: [], events: [], committed: {}, paused: false };
  const branchList = Object.values(branches)
    .filter((experiment) => experiment.name === 'a' || experiment.name === 'b' || Boolean(experiment.loadError))
    .sort((a, b) => a.id.localeCompare(b.id));
  const controlList = Object.values(branches)
    .filter((experiment) => experiment.readOnly)
    .sort((a, b) => a.id.localeCompare(b.id));
  const classificationText: Record<DivergenceAnalysis['classification'], string> = {
    'arrival-order-only': '仅消息到达顺序不同',
    'different-acknowledgements': '节点已作不同确认',
    incomplete: '资料尚未完整',
  };
  const pendingForks = forks.filter((fork) => fork.phase !== 'complete');
  const formatPause = (selector: Selector) =>
    [selector.type, `${selector.from ?? '?'}→${selector.to ?? '?'}`, `term ${selector.term ?? '?'}`].join(' ');

  return (
    <main>
      <h1>RaftLab：领导者选举与日志复演</h1>
      {error && <div className="error" role="alert">{error}</div>}
      <section className="toolbar">
        <div>虚拟时间 <b>{state?.now ?? 0}</b> ticks{state?.paused ? ' · 已暂停在消息投递前' : ''}</div>
        <button onClick={() => state && refresh(api.advance(50))}>推进 50</button>
        <button onClick={() => state && refresh(api.advance(250))}>推进 250</button>
        <button onClick={() => refresh(api.heal())}>恢复全部链路</button>
        <button onClick={() => replayBuiltin('minority')}>复演：少数派失败</button>
        <button onClick={() => replayBuiltin('old')}>复演：旧领导追平</button>
        {state?.paused
          ? <button onClick={() => refresh(api.clearPause())}>取消暂停继续运行</button>
          : <span className="pause-hint">在在途消息上点“投递前暂停”可冻结指定消息</span>}
        <button className="fork-button" disabled={!state?.paused} onClick={forkAtPause}>在暂停点分出 A/B 两个实验</button>
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

      <ErrorBoundary onError={setError}>
        <section className="grid">{safeState(state).nodes.map((nodeView) => <NodeCard key={nodeView.id} node={nodeView} />)}</section>
      </ErrorBoundary>

      <section className="split">
        <div>
          <h2>在途消息{state?.paused ? '（已暂停）' : ''}</h2>
          <ul className="messages">
            {inFlight(state).length === 0 && <li className="empty">队列为空：没有在途消息，等待心跳或推进时钟产生消息。</li>}
            {inFlight(state).map((message) => (
              <li key={message.id}>
                {message.deliverAt}: {message.type} {message.from}→{message.to} term {message.term}
                {!state?.paused && (
                  <button className="pause-btn" onClick={() => armPause(messageSelector(message))}>投递前暂停</button>
                )}
              </li>
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
            <tbody>{safeState(state).events.slice().reverse().map((event, index) => <EventLine key={`${event.t}-${event.kind}-${index}`} event={event} />)}</tbody>
          </table>
        </div>
      </section>

      {pendingForks.length > 0 && (
        <section className="fork-recovery">
          <h2>分叉恢复（仅重建损坏侧，健康侧历史保持不变）</h2>
          {pendingForks.map((fork) => (
            <article className="recovery-card" key={fork.forkKey}>
              <header>
                <strong>分叉 {fork.forkKey.slice(0, 12)}…</strong>
                <span>{fork.phase === 'canceled' ? '已取消，可重新预览' : '记录不完整'}</span>
              </header>
              <p>共同起点：t {fork.sourceAt} · 暂停于 {formatPause(fork.sourcePause)}</p>
              {fork.recovery && (
                <div className="recovery-preview">
                  <p>
                    健康侧 {fork.recovery.healthySide}（{fork.recovery.healthyId}）已执行
                    {fork.recovery.healthyDecisions.length} 条决定：
                    {fork.recovery.healthyDecisions.map((d) => d.label).join('、') || '（无）'}
                  </p>
                  <p>损坏侧 {fork.recovery.damagedSide}（{fork.recovery.damagedId}）：{fork.recovery.damagedError}</p>
                  {fork.recovery.planned && (
                    <p>
                      将新建记录 {fork.recovery.planned.id}（{fork.recovery.planned.name}）：
                      从共同起点重建，决定列表为空（[]），不会补写损坏侧丢失的决定。
                    </p>
                  )}
                  {!fork.recovery.recoverable && <p className="error">{fork.recovery.reason}</p>}
                </div>
              )}
              {fork.phase === 'canceled' ? (
                <div className="recovery-actions">
                  <button
                    disabled={!fork.recovery?.recoverable}
                    onClick={() => previewRecover(fork.forkKey)}
                  >
                    重新发起恢复预览
                  </button>
                </div>
              ) : (
                <div className="recovery-actions">
                  <button
                    disabled={!fork.recovery?.recoverable}
                    onClick={() => confirmRecover(fork.forkKey)}
                  >
                    确认恢复
                  </button>
                  <button onClick={() => cancelRecover(fork.forkKey)}>取消恢复（保留证据）</button>
                </div>
              )}
            </article>
          ))}
        </section>
      )}
      {branchList.length > 0 && (
        <section className="experiments">
          <h2>实验分支比较（共同前缀一致，队列与计时互相独立；决策已持久化，重开后自动恢复）</h2>
          <ErrorBoundary onError={setError}>
            <div className="branch-grid">
              {branchList.map((experiment) => (
                <ErrorBoundary key={experiment.id} onError={setError}>
                  <BranchPanel
                    title={experiment.loadError ? '保留的损坏记录' : experiment.name === 'a' ? '实验 A' : '实验 B'}
                    experiment={experiment}
                    pairId={pairedBranch(experiment)}
                    onChange={updateBranch}
                    onAnalyze={analyzeIntervention}
                    onError={setError}
                  />
                </ErrorBoundary>
              ))}
            </div>
          </ErrorBoundary>
          {controlList.length > 0 && (
            <div className="control-grid">
              {controlList.map((experiment) => (
                <div id={experiment.id} key={experiment.id}>
                  <BranchPanel
                    title="只读对照"
                    experiment={experiment}
                    pairId=""
                    onChange={updateBranch}
                    onAnalyze={() => undefined}
                    onError={setError}
                  />
                </div>
              ))}
            </div>
          )}
        </section>
      )}
      {analyses.length > 0 && (
        <section className="analyses">
          <h2>首次决定分歧分析（保存后不可变）</h2>
          {analyses.map((analysis) => (
            <article className="analysis-card" key={analysis.id} id={analysis.id}>
              <header>
                <strong>{classificationText[analysis.classification]}</strong>
                <span>{analysis.sufficient ? '该干预足以解释目标分歧' : '该干预不足以单独解释或资料不完整'}</span>
              </header>
              <p>{analysis.explanation}</p>
              <div className="analysis-messages">
                <span>A：{formatMessage(analysis.messageA)}</span>
                <span>B：{formatMessage(analysis.messageB)}</span>
              </div>
              <div className="analysis-frozen">
                比较依据（建立分析时冻结，不随分支继续推进而改变）：
                A 冻结于决定 {analysis.firstDecisionA.id}（t {analysis.firstDecisionA.at}）
                {analysis.treatedState && `，干预侧快照 t ${analysis.treatedState.now}`}
                ；B 冻结于决定 {analysis.firstDecisionB.id}（t {analysis.firstDecisionB.at}）
                {analysis.oppositeState && `，对侧快照 t ${analysis.oppositeState.now}`}
                {analysis.controlState && `；对照（移除所选干预）快照 t ${analysis.controlState.now}`}
                。
              </div>
              <div className="analysis-links">
                <a href={`#decision-${analysis.experimentAId}-${analysis.firstDecisionA.id}`}>回到 A 的对应消息和节点记录</a>
                <a href={`#decision-${analysis.experimentBId}-${analysis.firstDecisionB.id}`}>回到 B 的对应消息和节点记录</a>
                {analysis.controlId && <a href={`#${analysis.controlId}`}>查看只读对照</a>}
              </div>
            </article>
          ))}
        </section>
      )}
    </main>
  );
}
