package raft

import (
	"testing"
)

func advanceAsAPI(s *Simulator, ticks int64) {
	start := s.Now()
	s.Advance(ticks)
	s.RecordAdvance(start, s.Now())
}

func committedData(s *Simulator, index int) (string, int) {
	state := s.Snapshot(0)
	for _, node := range state.Nodes {
		if node.CommitIndex >= index && len(node.Log) >= index {
			return node.Log[index-1].Data, node.ID
		}
	}
	return "", 0
}

func TestMinorityLeaderCannotCommit(t *testing.T) {
	s := Replay(BuiltinMinorityFails())
	state := s.Snapshot(0)
	var staleLeader *NodeView
	for i := range state.Nodes {
		node := &state.Nodes[i]
		if node.ID == 1 {
			staleLeader = node
		}
	}
	if staleLeader == nil {
		t.Fatal("node 1 missing")
	}
	for _, entry := range staleLeader.Log {
		if entry.Data == "minority-stale" && entry.Index <= staleLeader.CommitIndex {
			t.Fatalf("minority record %d was committed by node 1", entry.Index)
		}
	}
	data, committedBy := committedData(s, 2)
	if data != "new-majority" {
		for _, node := range state.Nodes {
			t.Logf("node %d role=%s term=%d commit=%d log=%+v", node.ID, node.Role, node.Term, node.CommitIndex, node.Log)
		}
		t.Fatalf("index 2 = %q committed by %d, want new-majority", data, committedBy)
	}
}

func TestOldLeaderDoesNotOverwriteCommittedEntry(t *testing.T) {
	s := Replay(BuiltinOldLeaderRejoin())
	state := s.Snapshot(0)
	want := []string{"committed-before-split", "majority-after-split"}
	for _, view := range state.Nodes {
		if view.CommitIndex < 2 {
			t.Fatalf("node %d commitIndex=%d, want 2", view.ID, view.CommitIndex)
		}
		for index, data := range want {
			if got := view.Log[index].Data; got != data {
				t.Fatalf("node %d index %d = %q, want %q", view.ID, index+1, got, data)
			}
		}
		for _, entry := range view.Log {
			if entry.Data == "old-leader-unconfirmed" {
				t.Fatalf("node %d retained stale fork entry", view.ID)
			}
		}
	}
}

func TestDeterministicReplay(t *testing.T) {
	a := Replay(BuiltinOldLeaderRejoin()).Events()
	b := Replay(BuiltinOldLeaderRejoin()).Events()
	if len(a) != len(b) {
		t.Fatalf("event lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if string(MustJSON(a[i])) != string(MustJSON(b[i])) {
			t.Fatalf("event %d differs", i)
		}
	}
}

func TestRecordedInteractiveTimelineReplays(t *testing.T) {
	s := NewSimulator([]int{1, 2, 3}, 3)
	if err := s.ElectInitial(1); err != nil {
		t.Fatal(err)
	}
	advanceAsAPI(s, 80)
	_ = s.Client(1, "recorded-write")
	s.Partition([][]int{{1}, {2, 3}})
	advanceAsAPI(s, 300)
	s.HealAll()
	advanceAsAPI(s, 250)
	scenario := Scenario{Name: "recorded", Nodes: []int{1, 2, 3}, Seed: 3, InitialLeader: 1, Actions: s.Actions()}
	replayed := Replay(scenario)
	a := s.Events()
	b := replayed.Events()
	if len(a) != len(b) {
		t.Fatalf("recorded replay produced %d events, want %d", len(b), len(a))
	}
	for i := range a {
		if string(MustJSON(a[i])) != string(MustJSON(b[i])) {
			t.Fatalf("recorded event %d differs", i)
		}
	}
}
