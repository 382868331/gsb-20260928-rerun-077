package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"raftlab/raft"
)

func TestFullForkCompareFlow(t *testing.T) {
	lab := newTestLab(t)
	lab.analyses = map[string]*raft.DivergenceAnalysis{}
	mux := lab.routes()
	post := func(path string, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)))
		return rec
	}
	fork := armPauseAndFork(t, lab, mux)
	if strings.Contains(fork.Body.String(), "\"decisions\":null") {
		t.Fatal("null decisions")
	}
	var pair map[string]experimentView
	json.Unmarshal(fork.Body.Bytes(), &pair)
	a, b := pair["a"], pair["b"]
	if a.State == nil || !a.State.Paused {
		t.Fatal("branch A not at frozen pause state")
	}
	sel := a.State.InFlight[0]
	selector := map[string]any{"type": sel.Type, "from": sel.From, "to": sel.To, "term": sel.Term}
	// Branch A: deliver-first; branch B: postpone. Then advance both.
	if r := post("/api/experiments/"+a.ID+"/deliver-first", map[string]any{"selector": selector}); r.Code != 200 {
		t.Fatalf("deliver-first: %s", r.Body.String())
	}
	if r := post("/api/experiments/"+b.ID+"/postpone", map[string]any{"selector": selector, "delay": 300}); r.Code != 200 {
		t.Fatalf("postpone: %s", r.Body.String())
	}
	// A failing op on A must not affect B.
	if r := post("/api/experiments/"+a.ID+"/deliver-first", map[string]any{"selector": map[string]any{"type": "Nope", "from": 9, "to": 9}}); r.Code == 200 {
		t.Fatal("bogus deliver-first should fail")
	}
	post("/api/experiments/"+a.ID+"/advance", map[string]any{"ticks": 200})
	post("/api/experiments/"+b.ID+"/advance", map[string]any{"ticks": 200})
	// Analyze first divergence from A's deliver-first intervention.
	rec := post("/api/analyses", map[string]any{
		"experimentAId": a.ID, "experimentBId": b.ID,
		"candidate": map[string]any{"experimentId": a.ID, "decisionId": "d1", "actionIndex": 0, "at": 0, "label": "deliver-first"},
	})
	if rec.Code != 200 {
		t.Fatalf("analyze: %s", rec.Body.String())
	}
	var analysis raft.DivergenceAnalysis
	json.Unmarshal(rec.Body.Bytes(), &analysis)
	if analysis.ControlID == "" || analysis.TreatedState == nil || analysis.OppositeState == nil {
		t.Fatalf("analysis missing frozen comparison basis: %+v", analysis)
	}
	frozenT := analysis.TreatedState.Now
	// Advancing branch A afterwards must not change the saved analysis.
	post("/api/experiments/"+a.ID+"/advance", map[string]any{"ticks": 500})
	restarted := &Lab{sim: lab.sim, scenario: lab.scenario, experiments: map[string]*experiment{}, analyses: map[string]*raft.DivergenceAnalysis{}}
	restarted.loadExperiments()
	restarted.loadAnalyses()
	saved := restarted.analyses[analysis.ID]
	if saved == nil || saved.TreatedState.Now != frozenT {
		t.Fatal("saved analysis basis changed after branch advanced")
	}
	if restarted.experiments[a.ID] == nil || restarted.experiments[b.ID] == nil || restarted.experiments[analysis.ControlID] == nil {
		t.Fatal("branches or control lost across restart")
	}
	if len(restarted.experiments[a.ID].doc.Decisions) == 0 {
		t.Fatal("branch A decisions lost across restart")
	}
	// Main simulator still usable and still paused at original point.
	if !lab.sim.Paused() {
		t.Fatal("main simulator lost its pause point")
	}
	if r := post("/api/clear-pause", nil); r.Code != 200 {
		t.Fatal("single-simulation flow broken")
	}
}
