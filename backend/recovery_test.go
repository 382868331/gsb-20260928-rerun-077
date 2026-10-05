package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"raftlab/raft"
)

func postJSON(t *testing.T, mux *http.ServeMux, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)))
	return rec
}

func getJSON(t *testing.T, mux *http.ServeMux, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// forkAndAdvanceA creates a paused fork via HTTP and records one decision
// on branch A, returning the pair and the mux.
func forkAndAdvanceA(t *testing.T, lab *Lab) (map[string]experimentView, *http.ServeMux) {
	t.Helper()
	mux := lab.routes()
	fork := armPauseAndFork(t, lab, mux)
	if fork.Code != http.StatusOK {
		t.Fatalf("fork failed: %s", fork.Body.String())
	}
	var pair map[string]experimentView
	if err := json.Unmarshal(fork.Body.Bytes(), &pair); err != nil {
		t.Fatal(err)
	}
	sel := pair["a"].State.InFlight[0]
	selector := map[string]any{"type": sel.Type, "from": sel.From, "to": sel.To, "term": sel.Term}
	if r := postJSON(t, mux, "/api/experiments/"+pair["a"].ID+"/deliver-first", map[string]any{"selector": selector}); r.Code != 200 {
		t.Fatalf("deliver-first: %s", r.Body.String())
	}
	if r := postJSON(t, mux, "/api/experiments/"+pair["a"].ID+"/advance", map[string]any{"ticks": 120}); r.Code != 200 {
		t.Fatalf("advance: %s", r.Body.String())
	}
	return pair, mux
}

func restartLab(lab *Lab) *Lab {
	restarted := &Lab{sim: lab.sim, scenario: lab.scenario}
	restarted.loadExperiments()
	restarted.loadManifests()
	return restarted
}

func onlyRecovery(t *testing.T, mux *http.ServeMux) forkView {
	t.Helper()
	rec := getJSON(t, mux, "/api/forks")
	if rec.Code != 200 {
		t.Fatalf("GET /api/forks: %s", rec.Body.String())
	}
	var views []forkView
	if err := json.Unmarshal(rec.Body.Bytes(), &views); err != nil {
		t.Fatal(err)
	}
	if len(views) != 1 {
		t.Fatalf("got %d fork views, want 1", len(views))
	}
	return views[0]
}

func TestRecoverCorruptSideKeepsHealthyHistory(t *testing.T) {
	lab := newTestLab(t)
	pair, _ := forkAndAdvanceA(t, lab)
	idA, idB := pair["a"].ID, pair["b"].ID

	healthyBefore, err := os.ReadFile(filepath.Join(experimentDir, idA+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(experimentDir, idB+".json"), []byte("{broken"), 0o644); err != nil {
		t.Fatal(err)
	}

	restarted := restartLab(lab)
	mux := restarted.routes()

	// Healthy side still listed; damaged side isolated with loadError.
	list := getJSON(t, mux, "/api/experiments")
	if !strings.Contains(list.Body.String(), idA) || !strings.Contains(list.Body.String(), "loadError") {
		t.Fatalf("experiment list lost healthy side or damage marker: %s", list.Body.String())
	}

	view := onlyRecovery(t, mux)
	if view.Phase != raft.ForkPhasePartial || view.Recovery == nil {
		t.Fatalf("expected partial fork with recovery preview, got %+v", view)
	}
	rec := view.Recovery
	if !rec.Recoverable || rec.HealthySide != "a" || rec.HealthyID != idA {
		t.Fatalf("recovery preview misidentifies healthy side: %+v", rec)
	}
	if len(rec.HealthyDecisions) != 2 {
		t.Fatalf("healthy side executed decisions not shown: %+v", rec.HealthyDecisions)
	}
	if rec.DamagedSide != "b" || rec.DamagedID != idB || rec.DamagedError == "" {
		t.Fatalf("recovery preview misidentifies damaged side: %+v", rec)
	}
	if rec.Planned == nil || rec.Planned.ID != idB || rec.Planned.Name != "b" || len(rec.Planned.Decisions) != 0 {
		t.Fatalf("planned record must rebuild damaged side with empty decisions: %+v", rec.Planned)
	}
	if rec.Planned.ForkKey != view.ForkKey || rec.Planned.SourceAt != view.SourceAt {
		t.Fatalf("planned record not bound to common starting point: %+v", rec.Planned)
	}

	// Confirm recovery.
	confirm := postJSON(t, mux, "/api/forks/"+view.ForkKey+"/recover", nil)
	if confirm.Code != 200 {
		t.Fatalf("recover: %s", confirm.Body.String())
	}
	var recovered map[string]experimentView
	if err := json.Unmarshal(confirm.Body.Bytes(), &recovered); err != nil {
		t.Fatal(err)
	}
	if recovered["b"].ID != idB || len(recovered["b"].Decisions) != 0 {
		t.Fatalf("rebuilt side must not invent decisions: %+v", recovered["b"])
	}
	if len(recovered["a"].Decisions) != 2 {
		t.Fatalf("healthy side history changed by recovery: %+v", recovered["a"])
	}
	healthyAfter, _ := os.ReadFile(filepath.Join(experimentDir, idA+".json"))
	if !bytes.Equal(healthyBefore, healthyAfter) {
		t.Fatal("recovery overwrote the healthy side's record")
	}
	// Damaged JSON preserved as evidence.
	matches, _ := filepath.Glob(filepath.Join(experimentDir, idB+".corrupt-*.json"))
	if len(matches) != 1 {
		t.Fatalf("corrupt record not quarantined as evidence: %v", matches)
	}

	// Repeated confirm is a safe no-op.
	again := postJSON(t, mux, "/api/forks/"+view.ForkKey+"/recover", nil)
	if again.Code != 200 {
		t.Fatalf("repeated recover: %s", again.Body.String())
	}
	var againPair map[string]experimentView
	json.Unmarshal(again.Body.Bytes(), &againPair)
	if len(againPair["b"].Decisions) != 0 || len(againPair["a"].Decisions) != 2 {
		t.Fatal("repeated recover mutated the pair")
	}
	matches, _ = filepath.Glob(filepath.Join(experimentDir, idB+".corrupt-*.json"))
	if len(matches) != 1 {
		t.Fatal("repeated recover duplicated evidence files")
	}

	// After another restart the pair is complete and recovery is gone.
	final := restartLab(lab)
	finalView := onlyRecovery(t, final.routes())
	if finalView.Phase != raft.ForkPhaseComplete || finalView.Recovery != nil {
		t.Fatalf("recovered pair not complete after restart: %+v", finalView)
	}
	if got := final.experiments[idB]; got == nil || got.sim == nil || len(got.doc.Decisions) != 0 {
		t.Fatal("rebuilt side did not survive restart")
	}
}

func TestRecoverMissingSideFile(t *testing.T) {
	lab := newTestLab(t)
	pair, _ := forkAndAdvanceA(t, lab)
	idB := pair["b"].ID
	if err := os.Remove(filepath.Join(experimentDir, idB+".json")); err != nil {
		t.Fatal(err)
	}
	restarted := restartLab(lab)
	mux := restarted.routes()
	view := onlyRecovery(t, mux)
	if view.Recovery == nil || !view.Recovery.Recoverable {
		t.Fatalf("missing side file not offered for recovery: %+v", view)
	}
	if !strings.Contains(view.Recovery.DamagedError, "缺失") {
		t.Fatalf("missing file not reported as such: %s", view.Recovery.DamagedError)
	}
	confirm := postJSON(t, mux, "/api/forks/"+view.ForkKey+"/recover", nil)
	if confirm.Code != 200 {
		t.Fatalf("recover: %s", confirm.Body.String())
	}
	if _, err := os.Stat(filepath.Join(experimentDir, idB+".json")); err != nil {
		t.Fatal("missing side record was not rebuilt")
	}
}

func TestCancelRecoveryKeepsEvidence(t *testing.T) {
	lab := newTestLab(t)
	pair, _ := forkAndAdvanceA(t, lab)
	idA := pair["a"].ID
	idB := pair["b"].ID
	damagedPath := filepath.Join(experimentDir, idB+".json")
	healthyBefore, err := os.ReadFile(filepath.Join(experimentDir, idA+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(damagedPath, []byte("{broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	restarted := restartLab(lab)
	mux := restarted.routes()
	view := onlyRecovery(t, mux)

	cancel := postJSON(t, mux, "/api/forks/"+view.ForkKey+"/cancel", nil)
	if cancel.Code != 200 {
		t.Fatalf("cancel: %s", cancel.Body.String())
	}
	// Evidence retained: manifest and the damaged record itself.
	if _, err := os.Stat(damagedPath); err != nil {
		t.Fatal("cancel removed the damaged record")
	}
	if _, err := os.Stat(filepath.Join(forkDir, view.ForkKey+".json")); err != nil {
		t.Fatal("cancel removed the manifest")
	}
	after := onlyRecovery(t, mux)
	if after.Phase != raft.ForkPhaseCanceled || after.Recovery == nil || !after.Recovery.Recoverable {
		t.Fatalf("canceled fork must retain a re-openable recovery preview: %+v", after)
	}
	if r := postJSON(t, mux, "/api/forks/"+view.ForkKey+"/recover", nil); r.Code != http.StatusConflict {
		t.Fatalf("confirm after cancel got %d, want 409", r.Code)
	}

	sel := pair["a"].SourcePause
	refork := postJSON(t, mux, "/api/fork", map[string]any{"selector": sel})
	if refork.Code != http.StatusConflict {
		t.Fatalf("retry fork after cancel must reuse existing operation, got %d: %s", refork.Code, refork.Body.String())
	}
	if len(restarted.experiments) != 2 {
		t.Fatalf("fork retry after cancel created records: %d experiments", len(restarted.experiments))
	}
	if restarted.experiments[pair["a"].ID] == nil {
		t.Fatal("re-fork disturbed the previous healthy side")
	}

	// The canceled interaction survives refresh and can be previewed again.
	refreshedLab := restartLab(lab)
	refreshedMux := refreshedLab.routes()
	refreshed := onlyRecovery(t, refreshedMux)
	if refreshed.Phase != raft.ForkPhaseCanceled || refreshed.Recovery == nil || refreshed.Recovery.Planned.ID != idB {
		t.Fatalf("restart did not preserve cancellable recovery state: %+v", refreshed)
	}
	preview := postJSON(t, refreshedMux, "/api/forks/"+view.ForkKey+"/preview", nil)
	if preview.Code != http.StatusOK {
		t.Fatalf("preview after cancel: %s", preview.Body.String())
	}
	var previewView forkView
	if err := json.Unmarshal(preview.Body.Bytes(), &previewView); err != nil {
		t.Fatal(err)
	}
	if previewView.Phase != raft.ForkPhasePartial || previewView.Recovery == nil || previewView.Recovery.Planned.ID != idB {
		t.Fatalf("reopened preview did not use the original damaged identity: %+v", previewView)
	}

	confirm := postJSON(t, refreshedMux, "/api/forks/"+view.ForkKey+"/recover", nil)
	if confirm.Code != http.StatusOK {
		t.Fatalf("recover after reopened preview: %s", confirm.Body.String())
	}
	var recovered map[string]experimentView
	if err := json.Unmarshal(confirm.Body.Bytes(), &recovered); err != nil {
		t.Fatal(err)
	}
	if recovered["a"].ID != idA || len(recovered["a"].Decisions) != 2 {
		t.Fatalf("recovery changed healthy side: %+v", recovered["a"])
	}
	if recovered["b"].ID != idB || len(recovered["b"].Decisions) != 0 {
		t.Fatalf("recovery did not rebuild only the missing damaged side: %+v", recovered["b"])
	}
	healthyAfter, _ := os.ReadFile(filepath.Join(experimentDir, idA+".json"))
	if !bytes.Equal(healthyBefore, healthyAfter) {
		t.Fatal("recovery overwrote the healthy side's record")
	}
	again := postJSON(t, refreshedMux, "/api/forks/"+view.ForkKey+"/recover", nil)
	if again.Code != http.StatusOK {
		t.Fatalf("repeat recovery: %s", again.Body.String())
	}
	matches, _ := filepath.Glob(filepath.Join(experimentDir, idB+".corrupt-*.json"))
	if len(matches) != 1 {
		t.Fatalf("repeat confirmation produced extra evidence/records: %v", matches)
	}
}

func TestCancelAndRePreviewRecreatesMissingFile(t *testing.T) {
	lab := newTestLab(t)
	pair, _ := forkAndAdvanceA(t, lab)
	idB := pair["b"].ID
	missingPath := filepath.Join(experimentDir, idB+".json")
	if err := os.Remove(missingPath); err != nil {
		t.Fatal(err)
	}
	restarted := restartLab(lab)
	mux := restarted.routes()
	view := onlyRecovery(t, mux)
	if r := postJSON(t, mux, "/api/forks/"+view.ForkKey+"/cancel", nil); r.Code != http.StatusOK {
		t.Fatalf("cancel missing-side recovery: %s", r.Body.String())
	}
	if r := postJSON(t, mux, "/api/forks/"+view.ForkKey+"/recover", nil); r.Code != http.StatusConflict {
		t.Fatalf("confirm without reopened preview got %d, want 409", r.Code)
	}

	refreshed := restartLab(lab)
	refreshedMux := refreshed.routes()
	if r := postJSON(t, refreshedMux, "/api/forks/"+view.ForkKey+"/preview", nil); r.Code != http.StatusOK {
		t.Fatalf("reopen missing-side preview: %s", r.Body.String())
	}
	if r := postJSON(t, refreshedMux, "/api/forks/"+view.ForkKey+"/recover", nil); r.Code != http.StatusOK {
		t.Fatalf("recover missing side after preview: %s", r.Body.String())
	}
	if _, err := os.Stat(missingPath); err != nil {
		t.Fatal("missing side file was not recreated after canceled recovery")
	}
	if len(refreshed.experiments) != 2 {
		t.Fatalf("recovery created extra valid records: %d experiments", len(refreshed.experiments))
	}
}

func TestForkIncompletePairBlocksLookAlikes(t *testing.T) {
	lab := newTestLab(t)
	pair, _ := forkAndAdvanceA(t, lab)
	if err := os.Remove(filepath.Join(experimentDir, pair["b"].ID+".json")); err != nil {
		t.Fatal(err)
	}
	restarted := restartLab(lab)
	mux := restarted.routes()
	sel := pair["a"].SourcePause
	rec := postJSON(t, mux, "/api/fork", map[string]any{"selector": sel})
	if rec.Code != http.StatusConflict {
		t.Fatalf("incomplete pair fork retry got %d, want 409: %s", rec.Code, rec.Body.String())
	}
	if len(restarted.experiments) != 1 {
		t.Fatalf("look-alike branches created: %d experiments", len(restarted.experiments))
	}
}
