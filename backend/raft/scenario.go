package raft

import (
	"encoding/json"
	"sort"
)

type Action struct {
	Op      string  `json:"op"`
	At      int64   `json:"at"`
	From    int     `json:"from,omitempty"`
	To      int     `json:"to,omitempty"`
	Node    int     `json:"node,omitempty"`
	Delay   int64   `json:"delay,omitempty"`
	Up      *bool   `json:"up,omitempty"`
	Data    string  `json:"data,omitempty"`
	Groups  [][]int `json:"groups,omitempty"`
	Term    int     `json:"term,omitempty"`
	MsgType string  `json:"msgType,omitempty"`
}

const (
	ActionAdvance      = "advance"
	ActionPause        = "pause"
	ActionDeliverFirst = "deliver-first"
	ActionPostpone     = "postpone"
)

func (a Action) selector() Selector {
	return Selector{Type: MessageType(a.MsgType), From: a.From, To: a.To, Term: a.Term, Data: a.Data}
}

// Decision records one experiment choice made after a branch point so it
// can be traced and deterministically replayed after restart.
type Decision struct {
	ID      string   `json:"id"`
	Kind    string   `json:"kind"`
	Label   string   `json:"label"`
	At      int64    `json:"at"`
	Actions []Action `json:"actions"`
}

// ExperimentDoc is the persisted form of one forked experiment: the shared
// prefix scenario plus the branch's own ordered decisions.
type ExperimentDoc struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Base      Scenario   `json:"base"`
	Decisions []Decision `json:"decisions"`
	CreatedAt string     `json:"createdAt"`
	ReadOnly  bool       `json:"readOnly,omitempty"`
	// SourcePause, SourceAt and ForkKey record exactly which frozen pause
	// state a fork request started from, so a retried request is recognized
	// as the same operation instead of spawning look-alike branches.
	SourcePause *Selector `json:"sourcePause,omitempty"`
	SourceAt    int64     `json:"sourceAt,omitempty"`
	ForkKey     string    `json:"forkKey,omitempty"`
	// PairCancelled marks a branch whose fork pair was deliberately
	// cancelled after the other side was lost. The branch keeps its own
	// recorded history untouched; it only stops claiming the pause point,
	// so a fresh fork there gets a new, explicit identity.
	PairCancelled bool `json:"pairCancelled,omitempty"`
}

type Scenario struct {
	Name          string   `json:"name"`
	Description   string   `json:"description"`
	Nodes         []int    `json:"nodes"`
	Seed          uint64   `json:"seed"`
	InitialLeader int      `json:"initialLeader,omitempty"`
	Actions       []Action `json:"actions"`
}

// RunScenario executes a scenario and, when stopAtPause is true, halts with
// the simulator paused just before the first pause action's target message
// is delivered. The second return value reports that paused state.
func RunScenario(scenario Scenario, stopAtPause bool) (*Simulator, bool) {
	s := NewSimulator(scenario.Nodes, scenario.Seed)
	if scenario.InitialLeader != 0 {
		_ = s.ElectInitial(scenario.InitialLeader)
	}
	actions := append([]Action(nil), scenario.Actions...)
	sort.SliceStable(actions, func(i, j int) bool { return actions[i].At < actions[j].At })
	var current int64
	hasAdvance := false
	runTo := func(at int64) {
		if at > current {
			s.Advance(at - current)
			current = at
		}
	}
	for _, action := range actions {
		if stopAtPause && s.Paused() {
			return s, true
		}
		if stopAtPause {
			delta := action.At - current
			if delta > 0 {
				s.Advance(delta)
				if s.Paused() {
					return s, true
				}
				current = action.At
			}
		} else {
			runTo(action.At)
		}
		switch action.Op {
		case ActionAdvance:
			hasAdvance = true
			if stopAtPause {
				s.Advance(action.Delay)
				if s.Paused() {
					return s, true
				}
				current = action.At + action.Delay
			} else {
				runTo(action.At + action.Delay)
			}
		case "delay":
			_ = s.SetDelay(action.From, action.To, action.Delay)
		case "link":
			up := action.Up == nil || *action.Up
			s.SetLink(action.From, action.To, up)
		case "partition":
			s.Partition(action.Groups)
		case "heal":
			s.HealAll()
		case "client":
			_ = s.Client(action.Node, action.Data)
		case ActionPause:
			s.ArmPause(action.selector())
			if stopAtPause && s.Paused() {
				return s, true
			}
		case ActionDeliverFirst:
			_ = s.DeliverFirst(action.selector())
		case ActionPostpone:
			_ = s.Postpone(action.selector(), action.Delay)
		}
	}
	if !stopAtPause && !hasAdvance {
		runTo(current + 1000)
	}
	return s, s.Paused()
}

func Replay(scenario Scenario) *Simulator {
	s, _ := RunScenario(scenario, false)
	return s
}

// LoadExperiment rebuilds a forked experiment from its persisted document:
// the common prefix is replayed to the pause point, then every recorded
// branch decision is applied in order so the result is deterministic.
func LoadExperiment(doc ExperimentDoc) (*Simulator, error) {
	s, _ := RunScenario(doc.Base, true)
	for _, decision := range doc.Decisions {
		if decision.At > s.Now() {
			s.Advance(decision.At - s.Now())
		}
		for _, action := range decision.Actions {
			switch action.Op {
			case ActionPause:
				s.ArmPause(action.selector())
			case ActionDeliverFirst:
				if err := s.DeliverFirst(action.selector()); err != nil {
					return nil, err
				}
			case ActionPostpone:
				if err := s.Postpone(action.selector(), action.Delay); err != nil {
					return nil, err
				}
			case ActionAdvance:
				s.Advance(action.Delay)
				s.RecordAdvance(action.At, action.At+action.Delay)
			case "delay":
				_ = s.SetDelay(action.From, action.To, action.Delay)
			case "link":
				up := action.Up == nil || *action.Up
				s.SetLink(action.From, action.To, up)
			case "partition":
				s.Partition(action.Groups)
			case "heal":
				s.HealAll()
			case "client":
				_ = s.Client(action.Node, action.Data)
			}
		}
	}
	return s, nil
}

func BuiltinMinorityFails() Scenario {
	return Scenario{
		Name:          "minority-cannot-commit",
		Description:   "隔离旧领导者到少数派；其追加只能留在本地日志，多数派产生新领导者并独立提交。",
		Nodes:         []int{1, 2, 3, 4, 5},
		Seed:          7,
		InitialLeader: 1,
		Actions: []Action{
			{Op: "client", At: 20, Node: 1, Data: "initial-majority"},
			{Op: "partition", At: 260, Groups: [][]int{{1}, {2, 3, 4, 5}}},
			{Op: "client", At: 390, Node: 1, Data: "minority-stale"},
			{Op: "client", At: 700, Node: 4, Data: "new-majority"},
			{Op: "heal", At: 900},
		},
	}
}

func BuiltinOldLeaderRejoin() Scenario {
	return Scenario{
		Name:          "old-leader-catches-up",
		Description:   "网络恢复后，旧领导者通过 AppendEntries 一致性检查回滚未确认分叉，再追上已提交记录。",
		Nodes:         []int{1, 2, 3, 4, 5},
		Seed:          11,
		InitialLeader: 1,
		Actions: []Action{
			{Op: "client", At: 20, Node: 1, Data: "committed-before-split"},
			{Op: "partition", At: 240, Groups: [][]int{{1, 2}, {3, 4, 5}}},
			{Op: "client", At: 390, Node: 1, Data: "old-leader-unconfirmed"},
			{Op: "client", At: 700, Node: 4, Data: "majority-after-split"},
			{Op: "heal", At: 900},
		},
	}
}

func MustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
