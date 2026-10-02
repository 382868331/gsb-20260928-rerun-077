package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"raftlab/raft"
)

func TestExperimentsRemainVisibleWhenOneRecordIsBroken(t *testing.T) {
	lab := newTestLab(t)
	if err := os.MkdirAll(experimentDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(experimentDir, "broken.json"), []byte("{broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	good := raft.ExperimentDoc{ID: "good", Name: "a", Base: raft.Scenario{Nodes: []int{1, 2, 3}, Seed: 1}}
	if err := os.WriteFile(filepath.Join(experimentDir, "good.json"), raft.MustJSON(good), 0o644); err != nil {
		t.Fatal(err)
	}

	lab.loadExperiments()
	mux := lab.routes()
	req := httptest.NewRequest(http.MethodGet, "/api/experiments", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET experiments failed: %s", rec.Body.String())
	}
	body := rec.Body.String()
	if !bytes.Contains(rec.Body.Bytes(), []byte("broken.json")) || !bytes.Contains(rec.Body.Bytes(), []byte("good")) {
		t.Fatalf("broken record hid the surviving experiment: %s", body)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("loadError")) {
		t.Fatal("broken record should include explicit loadError")
	}
}

func TestAnalysisCreatesImmutableControlForFirstDivergence(t *testing.T) {
	lab := newTestLab(t)
	lab.analyses = map[string]*raft.DivergenceAnalysis{}
	lab.sim.Advance(30)
	pending := lab.sim.PendingMessages()
	if len(pending) == 0 {
		t.Fatal("no queued messages to pause on")
	}
	target := pending[0]
	lab.sim.ArmPause(raft.Selector{Type: target.Type, From: target.From, To: target.To})
	lab.sim.Advance(1 << 40)
	if !lab.sim.Paused() {
		t.Fatal("live simulator did not pause")
	}
	base := lab.currentScenario()
	base.Actions = append(base.Actions, raft.Action{Op: "advance", At: lab.sim.Now(), Delay: 1 << 40})
	makeBranch := func(name string, mutate func(*raft.Simulator)) *experiment {
		doc := raft.ExperimentDoc{ID: "exp-analysis-" + name, Name: name, Base: base}
		sim := lab.sim.Clone()
		prefixActions := len(sim.Actions())
		mutate(sim)
		exp := &experiment{doc: doc, sim: sim}
		doc.Decisions = []raft.Decision{{ID: "d1", Kind: name, Label: name, At: lab.sim.Now(), Actions: sim.Actions()[prefixActions:]}}
		exp.doc = doc
		lab.experiments[doc.ID] = exp
		if err := lab.saveExperimentLocked(exp); err != nil {
			t.Fatal(err)
		}
		return exp
	}
	a := makeBranch("a", func(s *raft.Simulator) {
		_ = s.DeliverFirst(raft.Selector{Type: target.Type, From: target.From, To: target.To})
	})
	makeBranch("b", func(s *raft.Simulator) {
		_ = s.Postpone(raft.Selector{Type: target.Type, From: target.From, To: target.To}, 500)
	})
	payload, _ := json.Marshal(map[string]any{
		"experimentAId": "exp-analysis-a",
		"experimentBId": "exp-analysis-b",
		"candidate": raft.InterventionRef{
			ExperimentID: "exp-analysis-a", DecisionID: "d1", ActionIndex: 0, At: a.doc.Decisions[0].At, Label: "a",
		},
	})
	mux := lab.routes()
	req := httptest.NewRequest(http.MethodPost, "/api/analyses", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("analysis failed: %s", rec.Body.String())
	}
	var analysis raft.DivergenceAnalysis
	if err := json.Unmarshal(rec.Body.Bytes(), &analysis); err != nil {
		t.Fatal(err)
	}
	if analysis.ControlID == "" || !analysis.ControlDoc.ReadOnly {
		t.Fatalf("analysis did not save a read-only control: %+v", analysis)
	}
	if analysis.Classification != "arrival-order-only" {
		t.Fatalf("classification = %s, want arrival-order-only: %s; treated=%v opposite=%v", analysis.Classification, analysis.Explanation, analysis.TreatedState, analysis.ControlState)
	}

	restarted := &Lab{experiments: map[string]*experiment{}, analyses: map[string]*raft.DivergenceAnalysis{}}
	restarted.loadExperiments()
	restarted.loadAnalyses()
	if _, ok := restarted.experiments[analysis.ControlID]; !ok {
		t.Fatal("saved control did not reload")
	}
	if len(restarted.analyses) != 1 {
		t.Fatalf("saved analyses = %d, want 1", len(restarted.analyses))
	}
}
