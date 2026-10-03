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

func armPauseAndFork(t *testing.T, lab *Lab, mux *http.ServeMux) *httptest.ResponseRecorder {
	t.Helper()
	lab.sim.Advance(30)
	pending := lab.sim.PendingMessages()
	if len(pending) == 0 {
		t.Fatal("no queued messages to pause on")
	}
	target := pending[0]
	sel := raft.Selector{Type: target.Type, From: target.From, To: target.To, Term: target.Term}
	lab.sim.ArmPause(sel)
	lab.sim.Advance(1 << 40)
	if !lab.sim.Paused() {
		t.Fatal("live simulator did not pause before selected delivery")
	}
	raw, _ := json.Marshal(map[string]any{"selector": sel})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/fork", bytes.NewReader(raw)))
	return rec
}

func TestForkEmitsEmptyDecisionListNotNull(t *testing.T) {
	lab := newTestLab(t)
	mux := lab.routes()
	rec := armPauseAndFork(t, lab, mux)
	if rec.Code != http.StatusOK {
		t.Fatalf("fork failed: %s", rec.Body.String())
	}
	if bytes.Contains(rec.Body.Bytes(), []byte("\"decisions\":null")) {
		t.Fatal("fresh fork serializes decisions as null and crashes the comparison UI")
	}
}

func TestForkRetryReturnsSamePair(t *testing.T) {
	lab := newTestLab(t)
	mux := lab.routes()
	first := armPauseAndFork(t, lab, mux)
	if first.Code != http.StatusOK {
		t.Fatalf("fork failed: %s", first.Body.String())
	}
	var pair1, pair2 map[string]experimentView
	if err := json.Unmarshal(first.Body.Bytes(), &pair1); err != nil {
		t.Fatal(err)
	}
	// Retrying the identical fork request must not mint look-alike branches.
	// The main simulator is still paused at the same point, so the retry
	// carries the identical source state.
	raw, _ := json.Marshal(map[string]any{"selector": pair1["a"].SourcePause})
	second := httptest.NewRecorder()
	mux.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/api/fork", bytes.NewReader(raw)))
	if err := json.Unmarshal(second.Body.Bytes(), &pair2); err != nil {
		t.Fatal(err)
	}
	if pair1["a"].ID != pair2["a"].ID || pair1["b"].ID != pair2["b"].ID {
		t.Fatalf("retry created new pair %s/%s, want original %s/%s",
			pair2["a"].ID, pair2["b"].ID, pair1["a"].ID, pair1["b"].ID)
	}
	if len(lab.experiments) != 2 {
		t.Fatalf("retry left %d experiments, want 2", len(lab.experiments))
	}
	if pair1["a"].SourcePause == nil || pair1["a"].ForkKey == "" {
		t.Fatal("fork did not record its source pause state")
	}

	// After a restart the original request's products are still recognized:
	// the same retry still maps to the persisted pair.
	restarted := &Lab{sim: lab.sim, scenario: lab.scenario, experiments: map[string]*experiment{}}
	restarted.loadExperiments()
	restartedMux := restarted.routes()
	third := httptest.NewRecorder()
	restartedMux.ServeHTTP(third, httptest.NewRequest(http.MethodPost, "/api/fork", bytes.NewReader(raw)))
	var pair3 map[string]experimentView
	if err := json.Unmarshal(third.Body.Bytes(), &pair3); err != nil {
		t.Fatal(err)
	}
	if pair3["a"].ID != pair1["a"].ID || pair3["b"].ID != pair1["b"].ID {
		t.Fatalf("post-restart retry created new pair %s/%s", pair3["a"].ID, pair3["b"].ID)
	}
}
