# 分布式系统实验室：领导者选举与日志复制

一个本地单机可交互的 Raft 演练环境：

- **Go 进程内运行多个虚拟节点**，每个节点是一个完整的 Raft 状态机（`backend/raft`）。
- **React 控制台**控制消息延迟、丢包、断开（隔离）与恢复，支持单步、暂停、调速。
- 节点**只能通过网络消息**（RequestVote / AppendEntries）获知彼此，没有任何共享的“上帝状态”参与一致性决策。
- **保存场景与事件顺序后可精确复演**：场景 = 初始（节点数 + 随机种子）+ 全部外部动作（提案、断网、恢复、链路参数）及其发生的逻辑时刻。
- 页面明确显示**哪些日志记录已被多数派提交**（绿色）与未提交（红色）。
- 强制保证：**旧领导者分区恢复后不能覆盖已确认内容**（日志匹配性质 + 提交索引回退），**少数派不能宣布成功**（只统计多数派、当前任期的提交规则）。

## 目录

```
backend/raft      Raft 节点状态机 + 确定性仿真引擎（纯库，无 IO）
backend/server    WebSocket/HTTP 服务（标准库实现 RFC6455）+ 场景存取
backend/cmd/raftlab  可执行入口，embed 前端构建产物
frontend/         React + Vite + zustand 控制台
```

## 运行

```bash
# 1) 构建前端
cd frontend && npm install && npm run build

# 2) 把产物放入 Go embed 目录并启动
cp -r dist/. ../backend/cmd/raftlab/webdist/
cd ../backend
go run ./cmd/raftlab -addr :8080 -scenarios ./scenarios
# 浏览器打开 http://localhost:8080
```

开发模式（热更新）：先 `go run ./cmd/raftlab` 启动后端，再在 `frontend/` 下 `npm run dev`，
Vite 会把 `/ws` 与 `/api` 代理到 8080。

## 交互演练建议（脑裂场景）

1. 运行到选出领导者（绿色光环节点）。
2. 选中领导者节点，点「断开」，再向它「客户端提案」写入 `old=1`。
   - 它只拥有少数派链路，收不到多数派 AppendEntries 确认，记录停留在**未提交**，时间线出现失败事件。
3. 多数派一侧会选出新领导者；向新领导者提案 `new=1`，可正常提交。
4. 点「恢复」旧领导者并快进：它发现自己的任期过期成为跟随者，
   冲突的未提交后缀被多数派日志覆盖，**已提交内容不变、不回退**。
5. 「保存场景」，之后随时点场景名载入并按运行，即可逐步看到完全相同的选举与提交顺序。

## 正确性测试

```bash
cd backend && go test ./...
```

覆盖：

- `TestElectsSingleLeader`：一个任期内只产生一个拥有多数派的领导者。
- `TestCommitMajority`：日志条目必须复制到多数派才标记提交。
- `TestMinorityCannotCommit`：2/5 少数派（含旧领导者）的提案永不提交，3/5 多数派选出新领导并可提交。
- `TestStaleLeaderHealsAndOverwritesNothing`：分区恢复后各节点已提交前缀完全一致，旧领导者的陈旧写入不会提交/覆盖。
- `TestReplayDeterministic`：相同种子 + 相同动作序列复演出逐节点角色/任期/日志/事件完全相同的结果。
- `server` 包测试通过**真实 WebSocket 握手与帧协议**（内存 net.Pipe，无需 OS 端口）端到端验证上述流程与场景存取。

## 仿真模型

- 逻辑时钟驱动：每 tick 各节点处理选举超时（随机 15–30 tick）或心跳（每 5 tick）。
- 消息经链路投递，可配置每 tick 的延迟区间与丢包率；断链时在途消息直接丢弃。
- 所有随机源（选举超时、链路延迟/丢包）均由场景种子派生，保证复演确定。
- 成员变更（membership change）是下一阶段扩展，本期集群规模在重置时固定。
