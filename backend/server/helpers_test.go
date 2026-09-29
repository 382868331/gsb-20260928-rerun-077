package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func mustSave(t *testing.T, h *Hub, name string) []byte {
	t.Helper()
	body := `{"name":"` + name + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/scenarios", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.handleSave(w, req)
	if w.Code != 200 {
		t.Fatalf("save status %d", w.Code)
	}
	return w.Body.Bytes()
}

func readFileOK(path string) ([]byte, error) { return os.ReadFile(path) }
