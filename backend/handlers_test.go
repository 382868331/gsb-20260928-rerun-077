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

func forkPair(t *testing.T, lab *Lab) (string, string) {
	t.Helper()
	lab.sim.Advance(30)
	pending := lab.sim.PendingMessages()
	if len(pending) == 0 {
		t.Fatal("no queued messages to pause on")
	}
	target := pending[0]
	sel := raft.Selector{Type: target.Type, From: target.From, To: target.To}
	lab.sim.ArmPause(sel)
	lab.sim.Advance(1 << 40)
	if !lab.sim.Paused() {
		t.Fatal("live simulator did not pause before selected delivery")
	}
	raw, _ := json.Marshal(map[string]any{"selector": sel})
	req := httptest.NewRequest(http.MethodPost, "/api/fork", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	lab.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("fork failed: %s", rec.Body.String())
	}
	var pair map[string]struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &pair); err != nil {
		t.Fatal(err)
	}
	return pair["a"].ID, pair["b"].ID
}

func TestBrokenExperimentDoesNotHideOthers(t *testing.T) {
	lab := newTestLab(t)
	idA, _ := forkPair(t, lab)
	if err := os.WriteFile("experiments/exp-broken.json", []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	restarted := &Lab{}
	restarted.loadExperiments()
	if len(restarted.experiments) != 3 {
		t.Fatalf("loaded %d experiments, want 3 (broken one kept as record)", len(restarted.experiments))
	}
	broken := restarted.experiments["exp-broken"]
	if broken == nil || broken.loadError == "" {
		t.Fatal("broken experiment should be kept with a load error")
	}
	req := httptest.NewRequest(http.MethodGet, "/api/experiments", nil)
	rec := httptest.NewRecorder()
	restarted.routes().ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), idA) {
		t.Fatal("healthy experiment hidden by broken sibling")
	}
	mutate := httptest.NewRequest(http.MethodPost, "/api/experiments/exp-broken/advance",
		bytes.NewReader([]byte(`{"ticks":10}`)))
	mutateRec := httptest.NewRecorder()
	restarted.routes().ServeHTTP(mutateRec, mutate)
	if mutateRec.Code != http.StatusConflict {
		t.Fatalf("mutating a broken experiment = %d, want 409", mutateRec.Code)
	}
}

func TestControlEndpointAttributesChosenIntervention(t *testing.T) {
	lab := newTestLab(t)
	idA, idB := forkPair(t, lab)
	mux := lab.routes()
	post := func(path string, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	if rec := post("/api/experiments/"+idB+"/partition", map[string]any{
		"groups": [][]int{{1, 2}, {3}}}); rec.Code != http.StatusOK {
		t.Fatalf("partition on branch B failed: %s", rec.Body.String())
	}
	if rec := post("/api/experiments/"+idB+"/advance", map[string]any{"ticks": 400}); rec.Code != http.StatusOK {
		t.Fatalf("advance on branch B failed: %s", rec.Body.String())
	}
	if rec := post("/api/experiments/"+idA+"/advance", map[string]any{"ticks": 400}); rec.Code != http.StatusOK {
		t.Fatalf("advance on branch A failed: %s", rec.Body.String())
	}
	// The divergence endpoint must answer before any control exists.
	req := httptest.NewRequest(http.MethodGet, "/api/divergence?a="+idA+"&b="+idB, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("divergence analysis failed: %s", rec.Body.String())
	}
	var report struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Status == "" {
		t.Fatal("divergence report has no status")
	}
	// Refuse to attribute the cause to a decision that is not a network
	// intervention (e.g. the last advance).
	expB := lab.experiments[idB]
	lastDecision := expB.doc.Decisions[len(expB.doc.Decisions)-1]
	if rec := post("/api/experiments/"+idB+"/controls", map[string]any{
		"targetId": idA, "decisionId": lastDecision.ID}); rec.Code != http.StatusBadRequest {
		t.Fatalf("non-intervention control = %d, want 400", rec.Code)
	}
	// Removing the chosen partition explains the divergence.
	var partitionID string
	for _, d := range expB.doc.Decisions {
		if d.Kind == "partition" {
			partitionID = d.ID
		}
	}
	if partitionID == "" {
		t.Fatal("partition decision not recorded")
	}
	rec = post("/api/experiments/"+idB+"/controls", map[string]any{
		"targetId": idA, "decisionId": partitionID})
	if rec.Code != http.StatusOK {
		t.Fatalf("control creation failed: %s", rec.Body.String())
	}
	var view struct {
		Controls []struct {
			DecisionID string `json:"decisionId"`
			Explains   bool   `json:"explains"`
		} `json:"controls"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Controls) != 1 || view.Controls[0].DecisionID != partitionID {
		t.Fatalf("saved control missing or misattributed: %+v", view.Controls)
	}
	// The saved control survives a restart and later branch progress.
	if rec := post("/api/experiments/"+idB+"/advance", map[string]any{"ticks": 100}); rec.Code != http.StatusOK {
		t.Fatalf("post-control advance failed: %s", rec.Body.String())
	}
	restarted := &Lab{}
	restarted.loadExperiments()
	got := restarted.experiments[idB]
	if got == nil || len(got.doc.Controls) != 1 || got.doc.Controls[0].DecisionID != partitionID {
		t.Fatal("saved control lost across restart or rewritten by branch progress")
	}
}
