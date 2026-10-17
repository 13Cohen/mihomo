package route

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/metacubex/mihomo/transport/anytls/session"
)

func TestAnyTLSStageRouteUnixOnly(t *testing.T) {
	for _, debug := range []bool{false, true} {
		// Both TCP and TLS controllers use router(), including their secret.
		public := router(debug, "", "", Cors{})
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			w := httptest.NewRecorder()
			public.ServeHTTP(w, httptest.NewRequest(method, "/diagnostics/anytls-stage", nil))
			if w.Code != http.StatusNotFound {
				t.Fatalf("public controller must return 404, got %d", w.Code)
			}
		}
		private := unixRouter(debug, "", Cors{})
		w := httptest.NewRecorder()
		private.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/diagnostics/anytls-stage", nil))
		var out session.StageEvidenceSnapshot
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &out) != nil || out.Availability != "inactive" || string(out.Snapshot) != "null" || out.Sequence != nil || out.EvidenceEpoch != nil {
			t.Fatalf("Unix inactive observation must be explicit: status=%d availability=%q snapshot_bytes=%d", w.Code, out.Availability, len(out.Snapshot))
		}
		if w.Header().Get("Cache-Control") != "no-store" || w.Body.Len() > session.StageEvidenceMaxResponseBytes {
			t.Fatal("response cache or size budget")
		}
		w = httptest.NewRecorder()
		private.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/diagnostics/anytls-stage", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatal("diagnostic endpoint must be read-only")
		}
	}
}

func TestAnyTLSStageResponseRejectsOversizedPublication(t *testing.T) {
	handler := stageEvidenceHandler(func() session.StageEvidenceSnapshot {
		return session.StageEvidenceSnapshot{Snapshot: json.RawMessage(`"` + strings.Repeat("x", session.StageEvidenceMaxResponseBytes) + `"`)}
	})
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest(http.MethodGet, "/diagnostics/anytls-stage", nil))
	if w.Code != http.StatusServiceUnavailable || w.Body.Len() > 256 {
		t.Fatal("invalid or oversized response must fail closed with bounded message")
	}
}
