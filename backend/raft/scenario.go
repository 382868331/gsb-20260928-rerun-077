package raft

import (
	"encoding/json"
	"sort"
)

type Action struct {
	Op     string  `json:"op"`
	At     int64   `json:"at"`
	From   int     `json:"from,omitempty"`
	To     int     `json:"to,omitempty"`
	Node   int     `json:"node,omitempty"`
	Delay  int64   `json:"delay,omitempty"`
	Up     *bool   `json:"up,omitempty"`
	Data   string  `json:"data,omitempty"`
	Groups [][]int `json:"groups,omitempty"`
}

const ActionAdvance = "advance"

type Scenario struct {
	Name          string   `json:"name"`
	Description   string   `json:"description"`
	Nodes         []int    `json:"nodes"`
	Seed          uint64   `json:"seed"`
	InitialLeader int      `json:"initialLeader,omitempty"`
	Actions       []Action `json:"actions"`
}

func Replay(scenario Scenario) *Simulator {
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
		runTo(action.At)
		switch action.Op {
		case ActionAdvance:
			hasAdvance = true
			runTo(action.At + action.Delay)
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
	if !hasAdvance {
		runTo(current + 1000)
	}
	return s
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
