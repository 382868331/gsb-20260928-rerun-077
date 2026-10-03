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

func restartLab(t *testing.T, lab *Lab) (*Lab, *http.ServeMux) {
	t.Helper()
	restarted := &Lab{sim: lab.sim, scenario: lab.scenario}
	restarted.loadExperiments()
	return restarted, restarted.routes()
}

func postJSON(t *testing.T, mux *http.ServeMux, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)))
	return rec
}

func forkPair(t *testing.T, lab *Lab, mux *http.ServeMux) (string, string, raft.Selector) {
	t.Helper()
	rec := armPauseAndFork(t, lab, mux)
	if rec.Code != http.StatusOK {
		t.Fatalf("fork failed: %s", rec.Body.String())
	}
	var pair map[string]experimentView
	if err := json.Unmarshal(rec.Body.Bytes(), &pair); err != nil {
		t.Fatal(err)
	}
	return pair["a"].ID, pair["b"].ID, *pair["a"].SourcePause
}

func pairStatus(t *testing.T, mux *http.ServeMux, forkKey string) forkPairView {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/fork-pairs", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("fork-pairs: %s", rec.Body.String())
	}
	var pairs []forkPairView
	if err := json.Unmarshal(rec.Body.Bytes(), &pairs); err != nil {
		t.Fatal(err)
	}
	for _, pair := range pairs {
		if pair.ForkKey == forkKey {
			return pair
		}
	}
	t.Fatalf("pair %s not listed", forkKey)
	return forkPairView{}
}

func previewRecovery(t *testing.T, mux *http.ServeMux, forkKey string) recoveryPreview {
	t.Helper()
	rec := postJSON(t, mux, "/api/fork-recovery/preview", map[string]any{"forkKey": forkKey})
	if rec.Code != http.StatusOK {
		t.Fatalf("preview: %s", rec.Body.String())
	}
	var preview recoveryPreview
	if err := json.Unmarshal(rec.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	return preview
}

func confirmRecovery(t *testing.T, mux *http.ServeMux, forkKey, token, action string) *httptest.ResponseRecorder {
	t.Helper()
	return postJSON(t, mux, "/api/fork-recovery/confirm", map[string]any{"forkKey": forkKey, "token": token, "action": action})
}

func TestCorruptSideBlocksReforkAndRepairRestoresPair(t *testing.T) {
	lab := newTestLab(t)
	mux := lab.routes()
	idA, idB, sel := forkPair(t, lab, mux)
	forkKey := lab.experiments[idA].doc.ForkKey

	// Give branch A an executed decision, then corrupt B's record on disk
	// (the file no longer parses, but its pair identity is still visible).
	if rec := postJSON(t, mux, "/api/experiments/"+idA+"/advance", map[string]any{"ticks": 50}); rec.Code != http.StatusOK {
		t.Fatalf("advance A: %s", rec.Body.String())
	}
	rawDoc, err := os.ReadFile(filepath.Join(experimentDir, idB+".json"))
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append(append([]byte(nil), rawDoc[:len(rawDoc)/4]...), rawDoc[len(rawDoc)/2:]...)
	if err := os.WriteFile(filepath.Join(experimentDir, idB+".json"), corrupt, 0o644); err != nil {
		t.Fatal(err)
	}

	restarted, restartedMux := restartLab(t, lab)
	pair := pairStatus(t, restartedMux, forkKey)
	if pair.Status != pairUnreadable {
		t.Fatalf("corrupt side not detected, status %s", pair.Status)
	}

	// Re-forking the same pause point must not mint a second identity.
	raw, _ := json.Marshal(map[string]any{"selector": sel})
	refork := httptest.NewRecorder()
	restartedMux.ServeHTTP(refork, httptest.NewRequest(http.MethodPost, "/api/fork", bytes.NewReader(raw)))
	if refork.Code != http.StatusConflict {
		t.Fatalf("refork over corrupt pair: got %d, want 409", refork.Code)
	}
	if len(restarted.experiments) != 2 {
		t.Fatalf("refork changed experiment count to %d", len(restarted.experiments))
	}

	// Preview marks the pair as recovering and changes nothing.
	preview := previewRecovery(t, restartedMux, forkKey)
	if preview.KeepSide != idA || preview.KeepDecisions != 1 || preview.RepairSide != "b" {
		t.Fatalf("preview keeps wrong side: %+v", preview)
	}
	if pairStatus(t, restartedMux, forkKey).Status != pairRecovering {
		t.Fatal("preview did not mark pair as recovering")
	}
	// A repeated preview returns the same pending session.
	again := previewRecovery(t, restartedMux, forkKey)
	if again.Token != preview.Token {
		t.Fatal("duplicate preview stacked a second recovery session")
	}

	// Confirm repair: B is rebuilt, A's history is untouched.
	decisionsBefore := append([]raft.Decision(nil), restarted.experiments[idA].doc.Decisions...)
	rec := confirmRecovery(t, restartedMux, forkKey, preview.Token, "repair")
	if rec.Code != http.StatusOK {
		t.Fatalf("confirm repair: %s", rec.Body.String())
	}
	var result recoveryResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Pair.Status != pairComplete || result.Repaired == nil {
		t.Fatalf("repair did not complete the pair: %+v", result.Pair)
	}
	gotA := restarted.experiments[idA]
	if len(gotA.doc.Decisions) != 1 || gotA.doc.Decisions[0].ID != decisionsBefore[0].ID {
		t.Fatalf("repair rewrote surviving branch history: %+v", gotA.doc.Decisions)
	}
	repaired := restarted.experiments[result.Repaired.ID]
	if repaired == nil || repaired.doc.Name != "b" || len(repaired.doc.Decisions) != 0 {
		t.Fatalf("repaired side wrong: %+v", repaired)
	}
	if repaired.doc.SourceAt != gotA.doc.SourceAt || repaired.doc.ForkKey != forkKey {
		t.Fatal("repaired side lost the source pause snapshot")
	}

	// Duplicate confirm with the same token replays the recorded outcome.
	dup := confirmRecovery(t, restartedMux, forkKey, preview.Token, "repair")
	if dup.Code != http.StatusOK {
		t.Fatalf("duplicate confirm: %s", dup.Body.String())
	}
	if len(restarted.experiments) != 2 {
		t.Fatalf("duplicate confirm changed experiment count to %d", len(restarted.experiments))
	}

	// After another restart the pair is complete and a refork retry returns
	// the original identity instead of a look-alike pair.
	restarted2, restarted2Mux := restartLab(t, restarted)
	if pairStatus(t, restarted2Mux, forkKey).Status != pairComplete {
		t.Fatal("repaired pair not complete across restart")
	}
	refork2 := httptest.NewRecorder()
	restarted2Mux.ServeHTTP(refork2, httptest.NewRequest(http.MethodPost, "/api/fork", bytes.NewReader(raw)))
	if refork2.Code != http.StatusOK {
		t.Fatalf("refork after repair: %s", refork2.Body.String())
	}
	var pair2 map[string]experimentView
	if err := json.Unmarshal(refork2.Body.Bytes(), &pair2); err != nil {
		t.Fatal(err)
	}
	if pair2["a"].ID != idA || pair2["b"].ID != result.Repaired.ID {
		t.Fatalf("refork minted new identity %s/%s", pair2["a"].ID, pair2["b"].ID)
	}
	if len(restarted2.experiments) != 2 {
		t.Fatalf("extra experiments after refork: %d", len(restarted2.experiments))
	}
}

func TestPartialWriteRecoveryAndCancelKeepsHistory(t *testing.T) {
	lab := newTestLab(t)
	mux := lab.routes()
	idA, idB, sel := forkPair(t, lab, mux)
	forkKey := lab.experiments[idA].doc.ForkKey

	// Simulate a crash between the two writes: only A's record exists.
	if err := os.Remove(filepath.Join(experimentDir, idB+".json")); err != nil {
		t.Fatal(err)
	}
	if rec := postJSON(t, mux, "/api/experiments/"+idA+"/advance", map[string]any{"ticks": 50}); rec.Code != http.StatusOK {
		t.Fatalf("advance A: %s", rec.Body.String())
	}

	restarted, restartedMux := restartLab(t, lab)
	pair := pairStatus(t, restartedMux, forkKey)
	if pair.Status != pairPartial || pair.Missing != "b" {
		t.Fatalf("partial write not detected: %+v", pair)
	}

	// Refork is blocked while the half-written pair claims the pause point.
	raw, _ := json.Marshal(map[string]any{"selector": sel})
	refork := httptest.NewRecorder()
	restartedMux.ServeHTTP(refork, httptest.NewRequest(http.MethodPost, "/api/fork", bytes.NewReader(raw)))
	if refork.Code != http.StatusConflict {
		t.Fatalf("refork over partial pair: got %d, want 409", refork.Code)
	}

	// Cancel the incomplete fork: A keeps its decisions, the pause point is
	// released, and a fresh fork gets a new explicit identity.
	preview := previewRecovery(t, restartedMux, forkKey)
	rec := confirmRecovery(t, restartedMux, forkKey, preview.Token, "cancel")
	if rec.Code != http.StatusOK {
		t.Fatalf("confirm cancel: %s", rec.Body.String())
	}
	gotA := restarted.experiments[idA]
	if gotA == nil || !gotA.doc.PairCancelled || len(gotA.doc.Decisions) != 1 {
		t.Fatalf("cancel changed surviving branch: %+v", gotA)
	}
	if pairStatus(t, restartedMux, forkKey).Status != pairCancelled {
		t.Fatal("cancelled pair not reported as cancelled")
	}
	// Duplicate cancel confirm is idempotent.
	if dup := confirmRecovery(t, restartedMux, forkKey, preview.Token, "cancel"); dup.Code != http.StatusOK {
		t.Fatalf("duplicate cancel: %s", dup.Body.String())
	}

	// A new fork at the same pause point now succeeds with fresh ids, and
	// the cancelled branch's recorded history survives another restart.
	refork2 := httptest.NewRecorder()
	restartedMux.ServeHTTP(refork2, httptest.NewRequest(http.MethodPost, "/api/fork", bytes.NewReader(raw)))
	if refork2.Code != http.StatusOK {
		t.Fatalf("refork after cancel: %s", refork2.Body.String())
	}
	var pair2 map[string]experimentView
	if err := json.Unmarshal(refork2.Body.Bytes(), &pair2); err != nil {
		t.Fatal(err)
	}
	if pair2["a"].ID == idA || pair2["a"].ForkKey == forkKey {
		t.Fatal("post-cancel fork reused the cancelled identity")
	}
	restarted2, _ := restartLab(t, restarted)
	kept := restarted2.experiments[idA]
	if kept == nil || len(kept.doc.Decisions) != 1 || !kept.doc.PairCancelled {
		t.Fatal("cancelled branch history lost across restart")
	}
	if len(restarted2.experiments) != 3 {
		t.Fatalf("want cancelled branch + new pair = 3 experiments, got %d", len(restarted2.experiments))
	}
}

func TestForkWriteFailureLeavesNoRemnants(t *testing.T) {
	lab := newTestLab(t)
	mux := lab.routes()
	// Make the data directory unusable: experiments is a file, not a dir.
	if err := os.WriteFile(experimentDir, []byte("blocked"), 0o644); err != nil {
		t.Fatal(err)
	}
	rec := armPauseAndFork(t, lab, mux)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("fork with failing writes: got %d, want 500", rec.Code)
	}
	if len(lab.experiments) != 0 {
		t.Fatalf("failed fork left %d in-memory remnants", len(lab.experiments))
	}
	// Once the directory works again the same pause point forks cleanly.
	if err := os.Remove(experimentDir); err != nil {
		t.Fatal(err)
	}
	idA, idB, _ := forkPair(t, lab, mux)
	if idA == "" || idB == "" || len(lab.experiments) != 2 {
		t.Fatal("retry after write failure did not produce exactly one pair")
	}
}
