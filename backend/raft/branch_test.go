package raft

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCloneBranchesAreIndependent(t *testing.T) {
	base := NewSimulator([]int{1, 2, 3}, 5)
	if err := base.ElectInitial(1); err != nil {
		t.Fatal(err)
	}
	base.Advance(10)
	forkA := base.Clone()
	forkB := base.Clone()

	_ = forkA.Client(1, "branch-a-write")
	forkA.SetLink(1, 2, false)
	forkA.Advance(60)

	_ = forkB.Client(1, "branch-b-write")
	forkB.Advance(60)

	a := forkA.Snapshot(0)
	b := forkB.Snapshot(0)
	if a.Now != b.Now {
		t.Fatalf("clocks drifted independently but started equal: %d vs %d", a.Now, b.Now)
	}
	hasData := func(state State, data string) bool {
		for _, node := range state.Nodes {
			for _, entry := range node.Log {
				if entry.Data == data {
					return true
				}
			}
		}
		return false
	}
	if !hasData(a, "branch-a-write") || hasData(a, "branch-b-write") {
		t.Fatal("branch A saw branch B data or lost its own")
	}
	if !hasData(b, "branch-b-write") || hasData(b, "branch-a-write") {
		t.Fatal("branch B saw branch A data or lost its own")
	}
	if a.Links["1->2"].Up {
		t.Fatal("link change leaked across clones")
	}
	if !b.Links["1->2"].Up {
		t.Fatal("branch B inherited branch A partition")
	}
	if hasData(base.Snapshot(0), "branch-a-write") {
		t.Fatal("shared prefix root was mutated by branch")
	}
}

func TestRestoreCheckpointContinuesDeterministically(t *testing.T) {
	s := NewSimulator([]int{1, 2, 3}, 9)
	_ = s.ElectInitial(1)
	s.Advance(40)
	restored := Restore(s.Checkpoint())
	s.Advance(120)
	restored.Advance(120)
	a := MustJSON(s.Snapshot(0))
	b := MustJSON(restored.Snapshot(0))
	if string(a) != string(b) {
		t.Fatalf("restored checkpoint diverged from live simulation\n%s\n%s", a, b)
	}
}

func TestDeliverFirstChangesOrderAndReplays(t *testing.T) {
	s := NewSimulator([]int{1, 2, 3}, 13)
	_ = s.ElectInitial(1)
	s.Advance(30)
	queued := s.PeekQueue()
	if len(queued) < 2 {
		t.Skip("not enough queued messages")
	}
	target := queued[len(queued)-1].ID
	if err := s.DeliverFirst(target); err != nil {
		t.Fatal(err)
	}
	if s.PeekQueue()[0].ID != target {
		t.Fatal("target message was not moved to the front")
	}
	start := s.Now()
	s.Advance(60)
	s.RecordAdvance(start, s.Now())
	scenario := Scenario{Name: "reordered", Nodes: []int{1, 2, 3}, Seed: 13, InitialLeader: 1, Actions: s.Actions()}
	aEvents := s.Events()
	replayed := Replay(scenario)
	bEvents := replayed.Events()
	if len(aEvents) != len(bEvents) {
		t.Fatalf("replay events %d, want %d", len(bEvents), len(aEvents))
	}
	for i := range aEvents {
		if string(MustJSON(aEvents[i])) != string(MustJSON(bEvents[i])) {
			t.Fatalf("reorder replay differs at event %d: %s vs %s", i, MustJSON(aEvents[i]), MustJSON(bEvents[i]))
		}
	}
}

func TestEmptyQueueSerializesAsArray(t *testing.T) {
	s := Replay(BuiltinOldLeaderRejoin())
	state := s.Snapshot(0)
	if len(state.InFlight) != 0 {
		t.Skip("scenario still has queued messages")
	}
	b, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"inFlight":[]`) {
		t.Fatalf("empty queue was not serialized as JSON array: %s", b[0:200])
	}
}
