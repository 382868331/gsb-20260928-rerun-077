package raft

import (
	"encoding/json"
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

func TestReplayActionsAreNotDuplicatedOnSave(t *testing.T) {
	scenario := BuiltinMinorityFails()
	s := Replay(scenario)
	clientActions := 0
	for _, action := range s.Actions() {
		if action.Op == "client" {
			clientActions++
		}
	}
	want := 0
	for _, action := range scenario.Actions {
		if action.Op == "client" {
			want++
		}
	}
	if clientActions != want {
		t.Fatalf("replay recorded %d client actions, want %d (saving must not duplicate them)", clientActions, want)
	}
}

func TestLogLifecycleEvents(t *testing.T) {
	s := Replay(BuiltinOldLeaderRejoin())
	abandoned := 0
	replicated := 0
	committed := map[string]int{}
	for _, event := range s.Events() {
		switch event.Kind {
		case "abandon":
			if event.Data == "old-leader-unconfirmed" {
				abandoned++
			}
		case "replicate", "append":
			if event.Data != "" {
				replicated++
			}
		case "commit":
			committed[event.Data]++
		}
	}
	if abandoned == 0 {
		t.Fatal("old leader fork entry was never reported as abandoned")
	}
	if replicated == 0 {
		t.Fatal("no replication events recorded")
	}
	if _, ok := committed["old-leader-unconfirmed"]; ok {
		t.Fatal("minority fork entry must never appear as committed")
	}
}

func TestNoLeaderSnapshotNeverReturnsNullSlices(t *testing.T) {
	s := NewSimulator([]int{1, 2, 3}, 5)
	state := s.Snapshot(0)
	if state.InFlight == nil || state.Events == nil {
		// Empty queue/event list must marshal as [] not null so UI lists render.
		b, _ := json.Marshal(state)
		t.Fatalf("snapshot contains null slices: %s", b)
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
