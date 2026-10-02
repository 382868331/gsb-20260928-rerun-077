import { useMemo, useState } from 'react';
import { api } from './api';
import type { Control, DivergenceReport, DivergenceStatus, ExperimentView } from './types';

interface Props {
  experiments: ExperimentView[];
  onChange: (next: ExperimentView) => void;
  onError: (message: string) => void;
}

const STATUS_LABEL: Record<DivergenceStatus, string> = {
  confirmed: '节点已作不同确认',
  'order-only': '仅消息到达顺序不同',
  insufficient: '资料尚未完整',
  identical: '两条轨迹目前完全一致',
};

const NETWORK_KINDS = new Set(['link-up', 'link-down', 'delay', 'partition', 'heal']);

function ReportView({ report }: { report: DivergenceReport }) {
  return (
    <div className={`report report-${report.status}`}>
      <strong>{STATUS_LABEL[report.status]}</strong>
      <span>{report.detail}</span>
      {(report.at !== undefined || report.node !== undefined) && (
        <span className="anchor">
          定位：t {report.at ?? '—'}
          {report.node !== undefined && ` · 节点 ${report.node}`}
          {report.index !== undefined && ` · 日志 #${report.index}`}
          {report.dataA !== undefined && report.dataA !== '' && ` · A 数据 ${report.dataA}`}
          {report.dataB !== undefined && report.dataB !== '' && ` · B 数据 ${report.dataB}`}
        </span>
      )}
      {report.deliveryA && (
        <span className="anchor">
          消息：A 侧 {report.deliveryA.type} {report.deliveryA.from}→{report.deliveryA.to}
          {report.deliveryB && ` · B 侧 ${report.deliveryB.type} ${report.deliveryB.from}→${report.deliveryB.to}`}
        </span>
      )}
    </div>
  );
}

function ControlCard({ control, onJump }: { control: Control; onJump: (id: string) => void }) {
  return (
    <li className="control">
      <div>
        <strong>{control.explains ? '足以解释目标分歧' : '不足以解释目标分歧'}</strong>
        <span>
          移除干预「{control.removedLabel}」（{control.sourceId} → 对照 {control.targetId}）· 保存于 {control.createdAt}
        </span>
      </div>
      <ReportView report={control.reportBefore} />
      <ReportView report={control.reportAfter} />
      <div className="control-links">
        <button onClick={() => onJump(control.sourceId)}>回到来源分支消息与节点记录</button>
        <button onClick={() => onJump(control.targetId)}>回到目标分支记录</button>
      </div>
    </li>
  );
}

export function AnalysisPanel({ experiments, onChange, onError }: Props) {
  const [a, setA] = useState('');
  const [b, setB] = useState('');
  const [decisionId, setDecisionId] = useState('');
  const [report, setReport] = useState<DivergenceReport | null>(null);
  const [savedControl, setSavedControl] = useState<Control | null>(null);

  const ids = experiments.map((exp) => exp.id);
  const sourceId = a || ids[0] || '';
  const targetId = b || ids[1] || '';
  const source = experiments.find((exp) => exp.id === sourceId);

  const interventions = useMemo(
    () => (source?.decisions ?? []).filter((decision) => NETWORK_KINDS.has(decision.kind)),
    [source],
  );
  const chosenDecision = decisionId || interventions[0]?.id || '';

  const controls = experiments.flatMap((exp) => exp.controls ?? []);

  const analyze = async () => {
    if (!sourceId || !targetId || sourceId === targetId) {
      onError('请选择两条不同的实验轨迹再分析。');
      return;
    }
    try {
      setReport(await api.divergence(sourceId, targetId));
    } catch (err) {
      onError(err instanceof Error ? err.message : String(err));
    }
  };

  const createControl = async () => {
    if (!chosenDecision) {
      onError('来源分支还没有网络干预（链路/延迟/分区/恢复）可供对照。');
      return;
    }
    try {
      const updated = await api.createControl(sourceId, targetId, chosenDecision);
      onChange(updated);
      const control = (updated.controls ?? []).find((item) => item.decisionId === chosenDecision) ?? null;
      setSavedControl(control);
    } catch (err) {
      onError(err instanceof Error ? err.message : String(err));
    }
  };

  const jump = (id: string) => {
    document.getElementById(`branch-${id}`)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };

  return (
    <section className="analysis">
      <h2>首次决定分歧分析</h2>
      <div className="analysis-controls">
        <label>轨迹 A
          <select value={sourceId} onChange={(e) => { setA(e.target.value); setDecisionId(''); }}>
            {ids.map((id) => <option key={id} value={id}>{id}</option>)}
          </select>
        </label>
        <label>轨迹 B
          <select value={targetId} onChange={(e) => setB(e.target.value)}>
            {ids.map((id) => <option key={id} value={id}>{id}</option>)}
          </select>
        </label>
        <button onClick={analyze}>分析首次分歧</button>
      </div>
      {report && <ReportView report={report} />}

      <h3>对照：不施加某次网络干预</h3>
      <div className="analysis-controls">
        <label>网络干预（来自轨迹 A，按记录顺序，不默认归因最后一次）
          <select value={chosenDecision} onChange={(e) => setDecisionId(e.target.value)}>
            {interventions.length === 0 && <option value="">（暂无网络干预记录）</option>}
            {interventions.map((decision) => (
              <option key={decision.id} value={decision.id}>
                {decision.id} · t {decision.at} · {decision.label}
              </option>
            ))}
          </select>
        </label>
        <button disabled={interventions.length === 0} onClick={createControl}>
          建立不施加该干预的对照并保存
        </button>
      </div>
      {savedControl && (
        <div className={savedControl.explains ? 'verdict explains' : 'verdict'}>
          {savedControl.explains
            ? `结论：移除「${savedControl.removedLabel}」后两条轨迹一致，该干预足以解释目标分歧。`
            : `结论：移除「${savedControl.removedLabel}」后分歧仍在，需继续检查其他干预。`}
        </div>
      )}

      {controls.length > 0 && (
        <>
          <h3>已保存对照（分支继续推进不会改写）</h3>
          <ul className="controls">
            {controls.map((control) => (
              <ControlCard key={control.id} control={control} onJump={jump} />
            ))}
          </ul>
        </>
      )}
    </section>
  );
}
