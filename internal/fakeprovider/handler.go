package fakeprovider

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// Handler serves provider requests (POST to any path), the control API under
// /_control/ (never counted as deliveries) and GET /healthz.
//
//	PUT    /_control/faults    {"rules":[Fault...]}  replace fault rules
//	DELETE /_control/faults                          clear fault rules
//	PUT    /_control/dedupe    {"enabled":bool}
//	GET    /_control/requests  ?channel=&key=&limit=
//	GET    /_control/stats
//	POST   /_control/reset
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /", s.deliver)
	// More specific than "POST /", so a mistyped control call is a 404, not a delivery.
	mux.HandleFunc("POST /_control/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "unknown control endpoint")
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("PUT /_control/faults", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Rules []Fault `json:"rules"`
		}
		if !decodeControl(w, r, &body) {
			return
		}
		if err := s.SetFaults(body.Rules); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_fault", err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /_control/faults", func(w http.ResponseWriter, _ *http.Request) {
		_ = s.SetFaults(nil) // clearing cannot fail validation
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("PUT /_control/dedupe", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Enabled *bool `json:"enabled"` // pointer: tells "false" apart from "missing"
		}
		if !decodeControl(w, r, &body) {
			return
		}
		if body.Enabled == nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "enabled is required")
			return
		}
		s.SetDedupe(*body.Enabled)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /_control/requests", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		f := Filter{Channel: q.Get("channel"), Key: q.Get("key")}
		if l := q.Get("limit"); l != "" {
			n, err := strconv.Atoi(l)
			if err != nil || n < 0 {
				writeError(w, http.StatusBadRequest, "invalid_request", "limit must be a non-negative integer")
				return
			}
			f.Limit = n
		}
		reqs := s.Requests(f)
		if reqs == nil {
			reqs = []Request{} // encode as [], not null
		}
		writeValue(w, http.StatusOK, map[string]any{"requests": reqs})
	})
	mux.HandleFunc("GET /_control/stats", func(w http.ResponseWriter, _ *http.Request) {
		writeValue(w, http.StatusOK, s.Stats())
	})
	mux.HandleFunc("POST /_control/reset", func(w http.ResponseWriter, _ *http.Request) {
		s.Reset()
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func decodeControl(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields() // a typo in a test script should fail loudly
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "body: "+err.Error())
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeValue(w, status, map[string]string{"code": code, "message": msg})
}

func writeValue(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = writeJSON(w, status, b) // caller left; nothing to do
}

// writeJSON writes the response; an error means the caller left.
func writeJSON(w http.ResponseWriter, status int, body []byte) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, err := w.Write(body)
	return err
}
