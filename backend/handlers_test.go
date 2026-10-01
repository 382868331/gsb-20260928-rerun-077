package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"raftlab/raft"
)

func newTestLab(t *testing.T) *Lab {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	live := raft.Scenario{Name: "live", Nodes: []int{1, 2, 3}, Seed: 3, InitialLeader: 1}
	sim := raft.NewSimulator(live.Nodes, live.Seed)
	_ = sim.ElectInitial(1)
	return &Lab{sim: sim, scenario: live, experiments: map[string]*experiment{}}
}

func TestOldLeaderReplayHasNoNullQueueAndKeepsWorkspace(t *testing.T) {
	lab := newTestLab(t)
	mux := lab.routes()

	bad := httptest.NewRequest(http.MethodPost, "/api/replay", bytes.NewReader([]byte("{not json")))
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, bad)
	if badRec.Code == http.StatusOK {
		t.Fatal("malformed replay was accepted")
	}
	before := lab.sim.Now()

	raw, _ := json.Marshal(map[string]any{"scenario": raft.BuiltinOldLeaderRejoin()})
	req := httptest.NewRequest(http.MethodPost, "/api/replay", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("old-leader replay failed: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), `"inFlight":null`) {
		t.Fatal("old-leader replay returns null inFlight queue -> blank page")
	}
	if lab.sim.Now() == before {
		t.Fatal("workspace simulator was not replaced by successful replay")
	}
}

func TestForkedExperimentsPersistAndReload(t *testing.T) {
	lab := newTestLab(t)
	lab.sim.Advance(30)
	pending := lab.sim.PendingMessages()
	if len(pending) == 0 {
		t.Fatal("no queued messages to pause on")
	}
	target := pending[0]
	lab.sim.ArmPause(raft.Selector{Type: target.Type, From: target.From, To: target.To})
	lab.sim.Advance(1 << 40)
	if !lab.sim.Paused() {
		t.Fatal("live simulator did not pause before selected delivery")
	}
	base := lab.currentScenario()
	base.Actions = append(base.Actions, raft.Action{Op: "advance", At: lab.sim.Now(), Delay: 1 << 40})
	makeBranch := func(name string) *experiment {
		doc := raft.ExperimentDoc{ID: "exp-test-" + name, Name: name, Base: base}
		sim := lab.sim.Clone()
		exp := &experiment{doc: doc, sim: sim}
		lab.experiments[doc.ID] = exp
		if err := lab.saveExperimentLocked(exp); err != nil {
			t.Fatal(err)
		}
		return exp
	}
	a := makeBranch("a")
	b := makeBranch("b")
	beforeA := len(a.sim.Actions())
	if err := a.sim.DeliverFirst(raft.Selector{Type: target.Type, From: target.From, To: target.To}); err != nil {
		t.Fatal(err)
	}
	a.recordDecision("deliver-first", "deliver-first", beforeA)
	if err := lab.saveExperimentLocked(a); err != nil {
		t.Fatal(err)
	}
	beforeB := len(b.sim.Actions())
	b.sim.Partition([][]int{{1}, {2, 3}})
	b.recordDecision("partition", "partition", beforeB)
	if err := lab.saveExperimentLocked(b); err != nil {
		t.Fatal(err)
	}

	restarted := &Lab{experiments: map[string]*experiment{}}
	restarted.loadExperiments()
	if len(restarted.experiments) != 2 {
		t.Fatalf("reloaded %d experiments, want 2", len(restarted.experiments))
	}
	gotB := restarted.experiments["exp-test-b"]
	if gotB == nil || gotB.sim.Snapshot(0).Links[raft.LinkKey(1, 2)].Up {
		t.Fatal("branch B network decision lost across restart")
	}
	gotA := restarted.experiments["exp-test-a"]
	if gotA == nil {
		t.Fatal("branch A missing after restart")
	}
	if !gotA.sim.Snapshot(0).Links[raft.LinkKey(1, 2)].Up {
		t.Fatal("branch B partition leaked into branch A")
	}
	if len(gotA.doc.Decisions) != 1 || gotA.doc.Decisions[0].Kind != "deliver-first" {
		t.Fatalf("branch A decision trace lost: %+v", gotA.doc.Decisions)
	}
}
