package raft

import "testing"

func TestPauseStopsBeforeMatchedDelivery(t *testing.T) {
	s := NewSimulator([]int{1, 2, 3}, 5)
	if err := s.ElectInitial(1); err != nil {
		t.Fatal(err)
	}
	s.ArmPause(Selector{Type: MsgAppend, From: 1, To: 2})
	s.Advance(500)
	if !s.Paused() {
		t.Fatal("simulator did not pause before selected message")
	}
	found := false
	for _, m := range s.PendingMessages() {
		if m.Type == MsgAppend && m.From == 1 && m.To == 2 {
			found = true
		}
	}
	if !found {
		t.Fatal("paused message missing from pending queue")
	}
	if term := s.nodes[2].Term; term != 0 {
		t.Fatalf("node 2 term changed to %d; message must not be delivered while paused", term)
	}
}

func TestBranchesSharePrefixThenDiverge(t *testing.T) {
	s := NewSimulator([]int{1, 2, 3}, 5)
	if err := s.ElectInitial(1); err != nil {
		t.Fatal(err)
	}
	s.Advance(60)
	if err := s.Client(1, "shared-prefix"); err != nil {
		t.Fatal(err)
	}
	s.ArmPause(Selector{Type: MsgAppendResp, To: 1, Data: ""})
	s.Advance(200)
	if !s.Paused() {
		t.Fatal("expected pause on first append response")
	}
	a := s.Clone()
	b := s.Clone()
	if err := a.DeliverFirst(Selector{Type: MsgAppendResp, From: 2, To: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.Postpone(Selector{Type: MsgAppendResp, From: 2, To: 1}, 400); err != nil {
		t.Fatal(err)
	}
	a.Advance(500)
	b.Advance(500)
	if a.Now() != b.Now() {
		t.Fatalf("clocks diverged unexpectedly: %d vs %d", a.Now(), b.Now())
	}
	// Mutating one branch must not touch the other.
	b.Partition([][]int{{1}, {2, 3}})
	aUp := a.links[LinkKey(1, 2)].Up
	if !aUp {
		t.Fatalf("branch B partition leaked into branch A: a 1->2 up=%v", aUp)
	}
	if b.links[LinkKey(1, 2)].Up {
		t.Fatal("branch B partition was not applied to branch B")
	}
	// Both branches still contain the identical committed prefix.
	for _, branch := range []*Simulator{a, b} {
		data, _ := committedData(branch, 1)
		if data != "shared-prefix" {
			t.Fatalf("prefix lost in branch: %q", data)
		}
	}
}

func TestExperimentReloadAcrossRestart(t *testing.T) {
	base := Scenario{
		Name: "live", Nodes: []int{1, 2, 3}, Seed: 9, InitialLeader: 1,
		Actions: []Action{
			{Op: "advance", At: 0, Delay: 120},
			{Op: "client", At: 120, Node: 1, Data: "before-pause"},
			{Op: "pause", At: 120, MsgType: string(MsgAppendResp), To: 1},
			{Op: "advance", At: 120, Delay: 1 << 40},
		},
	}
	paused, ok := RunScenario(base, true)
	if !ok || !paused.Paused() {
		t.Fatal("scenario should rebuild to the pause point")
	}
	pending := paused.PendingMessages()
	if len(pending) == 0 {
		t.Fatal("no messages held at pause point")
	}
	held := pending[0]
	doc := ExperimentDoc{ID: "reload", Name: "a", Base: base,
		Decisions: []Decision{{
			ID: "d1", Kind: "deliver-first", Label: "deliver-first", At: paused.Now(),
			Actions: []Action{{
				Op: ActionDeliverFirst, At: paused.Now(),
				MsgType: string(held.Type), From: held.From, To: held.To, Term: held.Term, Data: held.Data,
			}, {
				Op: ActionAdvance, At: paused.Now(), Delay: 400,
			}},
		}},
	}
	// Simulate restart: rebuild purely from the persisted document.
	reloaded, err := LoadExperiment(doc)
	if err != nil {
		t.Fatal(err)
	}
	data, node := committedData(reloaded, 1)
	if data != "before-pause" {
		t.Fatalf("reloaded prefix = %q (node %d)", data, node)
	}
}

func TestSnapshotNeverEmitsNullQueues(t *testing.T) {
	s := NewSimulator([]int{1, 2, 3}, 1)
	state := s.Snapshot(0)
	raw := string(MustJSON(state))
	if contains(raw, `"inFlight":null`) {
		t.Fatal("empty in-flight queue serializes as null and crashes the UI")
	}
}
