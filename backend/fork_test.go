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

func postJSON(t *testing.T, mux http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader([]byte(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// pauseLive advances the live simulator and freezes it before a delivery,
// returning the pause selector.
func pauseLive(t *testing.T, lab *Lab) map[string]any {
	t.Helper()
	lab.sim.Advance(30)
	pending := lab.sim.PendingMessages()
	if len(pending) == 0 {
		t.Fatal("no queued messages to pause on")
	}
	target := pending[0]
	lab.sim.ArmPause(raft.Selector{Type: target.Type, From: target.From, To: target.To, Term: target.Term})
	lab.sim.Advance(1 << 40)
	if !lab.sim.Paused() {
		t.Fatal("live simulator did not pause")
	}
	return map[string]any{"type": string(target.Type), "from": target.From, "to": target.To, "term": target.Term}
}

func TestForkEmitsEmptyDecisionsAndIsIdempotent(t *testing.T) {
	lab := newTestLab(t)
	lab.analyses = map[string]*raft.DivergenceAnalysis{}
	mux := lab.routes()
	sel := pauseLive(t, lab)
	selJSON, _ := json.Marshal(sel)

	first := postJSON(t, mux, "/api/fork", "{\"selector\": "+string(selJSON)+"}")
	if first.Code != http.StatusOK {
		t.Fatalf("fork failed: %s", first.Body.String())
	}
	if strings.Contains(first.Body.String(), "\"decisions\":null") {
		t.Fatal("fresh fork serializes decisions as null and crashes the compare view")
	}
	if !strings.Contains(first.Body.String(), "\"decisions\":[]") {
		t.Fatal("fresh fork should serialize decisions as an empty array")
	}
	var firstResult struct {
		A       raft.ExperimentDoc `json:"a"`
		B       raft.ExperimentDoc `json:"b"`
		Created bool               `json:"created"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstResult); err != nil {
		t.Fatal(err)
	}
	if !firstResult.Created {
		t.Fatal("first fork should report created=true")
	}
	if firstResult.A.Fork == nil || firstResult.B.Fork == nil {
		t.Fatal("fork provenance missing from branch documents")
	}
	if firstResult.A.Fork.PairID != firstResult.B.ID || firstResult.B.Fork.PairID != firstResult.A.ID {
		t.Fatal("fork provenance does not link the pair")
	}

	// Retrying the same request must not create look-alike duplicates.
	second := postJSON(t, mux, "/api/fork", "{\"selector\": "+string(selJSON)+"}")
	if second.Code != http.StatusOK {
		t.Fatalf("fork retry failed: %s", second.Body.String())
	}
	var secondResult struct {
		A       raft.ExperimentDoc `json:"a"`
		B       raft.ExperimentDoc `json:"b"`
		Created bool               `json:"created"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondResult); err != nil {
		t.Fatal(err)
	}
	if secondResult.Created {
		t.Fatal("retry of the same fork should report created=false")
	}
	if secondResult.A.ID != firstResult.A.ID || secondResult.B.ID != firstResult.B.ID {
		t.Fatal("retry returned different branch IDs; duplicate pair created")
	}
	if len(lab.experiments) != 2 {
		t.Fatalf("retry created extra branches: %d experiments", len(lab.experiments))
	}

	// Provenance must survive a restart so the original request stays
	// recognizable.
	restarted := &Lab{experiments: map[string]*experiment{}}
	restarted.loadExperiments()
	got := restarted.experiments[firstResult.A.ID]
	if got == nil || got.doc.Fork == nil || got.doc.Fork.PairID != firstResult.B.ID {
		t.Fatal("fork provenance lost after restart")
	}
}

func TestForkReportsPartialPairWithoutDuplicating(t *testing.T) {
	lab := newTestLab(t)
	lab.analyses = map[string]*raft.DivergenceAnalysis{}
	mux := lab.routes()
	sel := pauseLive(t, lab)
	selJSON, _ := json.Marshal(sel)

	first := postJSON(t, mux, "/api/fork", "{\"selector\": "+string(selJSON)+"}")
	if first.Code != http.StatusOK {
		t.Fatalf("fork failed: %s", first.Body.String())
	}
	var result struct {
		B raft.ExperimentDoc `json:"b"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	// Simulate a half-lost pair: only one branch record survives.
	delete(lab.experiments, result.B.ID)

	retry := postJSON(t, mux, "/api/fork", "{\"selector\": "+string(selJSON)+"}")
	if retry.Code != http.StatusConflict {
		t.Fatalf("partial pair retry = %d, want 409: %s", retry.Code, retry.Body.String())
	}
	if !strings.Contains(retry.Body.String(), "只完成了一部分") {
		t.Fatalf("partial pair retry should explain the partial state: %s", retry.Body.String())
	}
	if len(lab.experiments) != 1 {
		t.Fatalf("partial retry created duplicates: %d experiments", len(lab.experiments))
	}
}
