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
)

func forkPair(t *testing.T, lab *Lab, mux *http.ServeMux) (string, string) {
	t.Helper()
	rec := armPauseAndFork(t, lab, mux)
	if rec.Code != http.StatusOK {
		t.Fatalf("fork failed: %s", rec.Body.String())
	}
	var pair map[string]experimentView
	if err := json.Unmarshal(rec.Body.Bytes(), &pair); err != nil {
		t.Fatal(err)
	}
	return pair["a"].ID, pair["b"].ID
}

func advanceBranch(t *testing.T, mux *http.ServeMux, id string) {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/experiments/"+id+"/advance", bytes.NewReader([]byte(`{"ticks":120}`))))
	if rec.Code != http.StatusOK {
		t.Fatalf("advance on %s failed: %s", id, rec.Body.String())
	}
}

func restartLab(t *testing.T) (*Lab, *http.ServeMux) {
	t.Helper()
	restarted := &Lab{}
	restarted.loadExperiments()
	return restarted, restarted.routes()
}

func getPreview(t *testing.T, mux *http.ServeMux, healthyID string) recoveryPreview {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/experiments/"+healthyID+"/recovery-preview", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("recovery preview failed: %s", rec.Body.String())
	}
	var preview recoveryPreview
	if err := json.Unmarshal(rec.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	return preview
}

func postRecover(t *testing.T, mux *http.ServeMux, healthyID string) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/experiments/"+healthyID+"/recover", bytes.NewReader([]byte("{}"))))
	if rec.Code != http.StatusOK {
		t.Fatalf("recover failed: %s", rec.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRecoveryPreviewAndConfirmAfterCorruptJSON(t *testing.T) {
	lab := newTestLab(t)
	mux := lab.routes()
	aID, bID := forkPair(t, lab, mux)
	advanceBranch(t, mux, aID)

	healthyRaw, err := os.ReadFile(filepath.Join(experimentDir, aID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(experimentDir, bID+".json"), []byte("{corrupt"), 0o644); err != nil {
		t.Fatal(err)
	}

	restarted, mux2 := restartLab(t)
	if restarted.experiments[aID] == nil || restarted.experiments[aID].loadErr != "" {
		t.Fatal("healthy side lost after restart")
	}
	foundDamaged := false
	for _, exp := range restarted.experiments {
		if exp.loadErr != "" {
			foundDamaged = true
		}
	}
	if !foundDamaged {
		t.Fatal("corrupt record not kept as evidence after restart")
	}

	preview := getPreview(t, mux2, aID)
	if preview.DamagedName != "b" || preview.DamagedState != "damaged" {
		t.Fatalf("unexpected damaged side: %+v", preview)
	}
	if len(preview.Healthy.Decisions) != 1 {
		t.Fatalf("healthy side decisions missing from preview: %+v", preview.Healthy.Decisions)
	}
	if len(preview.NewRecord.Decisions) != 0 {
		t.Fatal("new record must start with an empty decision list")
	}
	if preview.NewRecord.Name != "b" || preview.NewRecord.ForkKey != preview.ForkKey {
		t.Fatalf("new record not bound to the same fork: %+v", preview.NewRecord)
	}
	if len(preview.DamagedRecords) == 0 {
		t.Fatal("preview should list the damaged record as evidence")
	}

	result := postRecover(t, mux2, aID)
	if result["recovered"] != true {
		t.Fatalf("first recover should create the record: %v", result)
	}
	newID := result["experiment"].(map[string]any)["id"].(string)

	// Healthy side history untouched.
	afterRaw, err := os.ReadFile(filepath.Join(experimentDir, aID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(healthyRaw, afterRaw) {
		t.Fatal("recovery overwrote the healthy side's record")
	}
	// Corrupt file kept as evidence.
	corruptRaw, err := os.ReadFile(filepath.Join(experimentDir, bID+".json"))
	if err != nil || string(corruptRaw) != "{corrupt" {
		t.Fatal("corrupt record was deleted or overwritten by recovery")
	}

	// Repeated click: no duplicate.
	again := postRecover(t, mux2, aID)
	if again["existing"] != true || again["recovered"] != false {
		t.Fatalf("repeated recover should return the existing record: %v", again)
	}
	entries, err := os.ReadDir(experimentDir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), newID) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("duplicate recovery records on disk: %d", count)
	}

	// Restart again: recovered record loads healthy with empty decisions,
	// corrupt evidence still listed.
	restarted2, _ := restartLab(t)
	recovered := restarted2.experiments[newID]
	if recovered == nil || recovered.loadErr != "" {
		t.Fatal("recovered record did not survive restart")
	}
	if len(recovered.doc.Decisions) != 0 {
		t.Fatal("recovered record must not fabricate the damaged side's decisions")
	}
	if recovered.doc.RecoveredFrom == "" {
		t.Fatal("recovered record should reference the damaged record it replaces")
	}
	stillDamaged := false
	for _, exp := range restarted2.experiments {
		if exp.loadErr != "" {
			stillDamaged = true
		}
	}
	if !stillDamaged {
		t.Fatal("corrupt evidence vanished after recovery and restart")
	}
	// Preview now reports the pair is healthy again.
	rec := httptest.NewRecorder()
	_, mux3 := restartLab(t)
	mux3.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/experiments/"+aID+"/recovery-preview", nil))
	if rec.Code != http.StatusConflict {
		t.Fatalf("preview on healthy pair should conflict, got %d", rec.Code)
	}
}

func TestRecoveryAfterMissingFile(t *testing.T) {
	lab := newTestLab(t)
	mux := lab.routes()
	aID, bID := forkPair(t, lab, mux)
	if err := os.Remove(filepath.Join(experimentDir, bID+".json")); err != nil {
		t.Fatal(err)
	}
	_, mux2 := restartLab(t)
	preview := getPreview(t, mux2, aID)
	if preview.DamagedState != "missing" {
		t.Fatalf("missing file should be reported as missing: %+v", preview)
	}
	result := postRecover(t, mux2, aID)
	if result["recovered"] != true {
		t.Fatalf("recover after missing file failed: %v", result)
	}
}

func TestRecoveryRejectsHealthyOrUnpaired(t *testing.T) {
	lab := newTestLab(t)
	mux := lab.routes()
	aID, _ := forkPair(t, lab, mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/experiments/"+aID+"/recovery-preview", nil))
	if rec.Code != http.StatusConflict {
		t.Fatalf("healthy pair should not offer recovery, got %d", rec.Code)
	}
	rec2 := httptest.NewRecorder()
	mux2 := lab.routes()
	mux2.ServeHTTP(rec2, httptest.NewRequest(http.MethodPost, "/api/experiments/nonexistent/recover", bytes.NewReader([]byte("{}"))))
	if rec2.Code == http.StatusOK {
		t.Fatal("recover accepted an unknown experiment")
	}
}

