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

func TestCancelEndsOnlyThisAttemptAndKeepsPreview(t *testing.T) {
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
	// Cancel ends only the current attempt: every piece of evidence stays.
	if _, err := os.Stat(damagedPath); err != nil {
		t.Fatal("cancel removed the damaged record")
	}
	healthyAfter, _ := os.ReadFile(filepath.Join(experimentDir, idA+".json"))
	if !bytes.Equal(healthyBefore, healthyAfter) {
		t.Fatal("cancel disturbed the healthy side's record")
	}
	// The manifest is not turned into a tombstone: the preview remains
	// available so the user can start another recovery attempt.
	after := onlyRecovery(t, mux)
	if after.Phase != raft.ForkPhasePartial || after.Recovery == nil || !after.Recovery.Recoverable {
		t.Fatalf("cancel must keep the unfinished fork recoverable: %+v", after)
	}
	if after.Recovery.HealthyID != idA || len(after.Recovery.HealthyDecisions) != 2 {
		t.Fatalf("cancel lost the healthy side or its history: %+v", after.Recovery)
	}
	if after.Recovery.DamagedID != idB || after.Recovery.Planned == nil || after.Recovery.Planned.ID != idB {
		t.Fatalf("cancel lost the damaged-side rebuild plan: %+v", after.Recovery)
	}

	// Retrying the same fork request still maps to this operation: it must
	// not mint a second, look-alike pair while the damaged record exists.
	sel := pair["a"].SourcePause
	if refork := postJSON(t, mux, "/api/fork", map[string]any{"selector": sel}); refork.Code != http.StatusConflict {
		t.Fatalf("re-fork after cancel got %d, want 409: %s", refork.Code, refork.Body.String())
	}
	if len(restarted.experiments) != 2 {
		t.Fatalf("re-fork after cancel created extra records: %d experiments", len(restarted.experiments))
	}

	// Confirming again after cancellation rebuilds only the missing side
	// from the shared starting point.
	confirm := postJSON(t, mux, "/api/forks/"+view.ForkKey+"/recover", nil)
	if confirm.Code != 200 {
		t.Fatalf("recover after reopen: %s", confirm.Body.String())
	}
	var recovered map[string]experimentView
	if err := json.Unmarshal(confirm.Body.Bytes(), &recovered); err != nil {
		t.Fatal(err)
	}
	if recovered["b"].ID != idB || len(recovered["b"].Decisions) != 0 {
		t.Fatalf("rebuilt side must reuse the original id with empty decisions: %+v", recovered["b"])
	}
	if len(recovered["a"].Decisions) != 2 {
		t.Fatalf("healthy side history changed: %+v", recovered["a"])
	}
	healthyFinal, _ := os.ReadFile(filepath.Join(experimentDir, idA+".json"))
	if !bytes.Equal(healthyBefore, healthyFinal) {
		t.Fatal("reconfirm overwrote the healthy side's executed decisions")
	}
	matches, _ := filepath.Glob(filepath.Join(experimentDir, idB+".corrupt-*.json"))
	if len(matches) != 1 {
		t.Fatalf("damaged record not preserved as the single evidence copy: %v", matches)
	}

	// A further restart shows a complete pair with no recovery entry.
	finalView := onlyRecovery(t, restartLab(lab).routes())
	if finalView.Phase != raft.ForkPhaseComplete || finalView.Recovery != nil {
		t.Fatalf("reconfirmed pair not complete after refresh: %+v", finalView)
	}
}

func TestLegacyCanceledTombstoneHealsBackToPreview(t *testing.T) {
	lab := newTestLab(t)
	pair, _ := forkAndAdvanceA(t, lab)
	idB := pair["b"].ID
	if err := os.WriteFile(filepath.Join(experimentDir, idB+".json"), []byte("{broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Simulate an older build that persisted cancellation as a tombstone.
	manifestPath := filepath.Join(forkDir, pair["a"].ForkKey+".json")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest raft.ForkManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Phase = raft.ForkPhaseCanceled
	if err := atomicWriteJSON(forkDir, manifest.ForkKey, manifest); err != nil {
		t.Fatal(err)
	}

	restarted := restartLab(lab)
	mux := restarted.routes()
	view := onlyRecovery(t, mux)
	if view.Phase != raft.ForkPhasePartial || view.Recovery == nil || !view.Recovery.Recoverable {
		t.Fatalf("legacy canceled tombstone must reopen as recovery preview: %+v", view)
	}
	confirm := postJSON(t, mux, "/api/forks/"+view.ForkKey+"/recover", nil)
	if confirm.Code != 200 {
		t.Fatalf("recover from legacy tombstone: %s", confirm.Body.String())
	}
	finalView := onlyRecovery(t, restartLab(lab).routes())
	if finalView.Phase != raft.ForkPhaseComplete {
		t.Fatalf("legacy tombstone recovery did not complete: %+v", finalView)
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
