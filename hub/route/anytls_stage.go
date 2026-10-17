package route

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/metacubex/mihomo/transport/anytls/session"
)

// This extra route is mounted only by startUnix, never the TCP, TLS or named
// pipe controller. It inherits the Unix management socket's file permissions.
func unixRouter(isDebug bool, dohServer string, cors Cors) *chi.Mux {
	r := router(isDebug, "", dohServer, cors)
	r.Get("/diagnostics/anytls-stage", stageEvidenceHandler(session.ReadServerStageEvidence))
	return r
}

func stageEvidenceHandler(read func() session.StageEvidenceSnapshot) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// read returns a copy of one immutable, fixed-size publication. A slow
		// controller client can block its HTTP handler, never the observer.
		body, err := json.Marshal(read())
		if err != nil || len(body) > session.StageEvidenceMaxResponseBytes {
			http.Error(w, "diagnostic snapshot unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Write(body)
	}
}
