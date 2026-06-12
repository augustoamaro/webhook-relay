// Package api is the HTTP surface: thin handlers over the store and queue.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/augustoamaro/webhook-relay/internal/queue"
	"github.com/augustoamaro/webhook-relay/internal/store"
)

// Server is the HTTP API surface: thin handlers over the store and queue.
type Server struct {
	store      *store.Store
	queue      *queue.Queue
	adminKey   string
	maxPayload int64
}

// NewServer returns a Server backed by the given store and queue.
func NewServer(s *store.Store, q *queue.Queue, adminKey string, maxPayload int64) *Server {
	return &Server{store: s, queue: q, adminKey: adminKey, maxPayload: maxPayload}
}

// Handler builds and returns the API router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("POST /api/v1/applications", s.admin(s.handleCreateApp))
	mux.HandleFunc("POST /api/v1/applications/{app}/endpoints", s.admin(s.handleCreateEndpoint))
	mux.HandleFunc("GET /api/v1/applications/{app}/endpoints", s.admin(s.handleListEndpoints))
	mux.HandleFunc("PATCH /api/v1/applications/{app}/endpoints/{ep}", s.admin(s.handlePatchEndpoint))
	mux.HandleFunc("POST /api/v1/applications/{app}/messages", s.appAuth(s.handleIngest))
	mux.HandleFunc("GET /api/v1/applications/{app}/messages/{msg}", s.appAuth(s.handleMessageStatus))
	mux.HandleFunc("GET /api/v1/applications/{app}/deliveries", s.appAuth(s.handleListDeliveries))
	mux.HandleFunc("POST /api/v1/applications/{app}/deliveries/redrive", s.appAuth(s.handleRedrive))
	return mux
}

func bearer(r *http.Request) string {
	h := r.Header.Get("authorization")
	tok, _ := strings.CutPrefix(h, "Bearer ")
	return tok
}

// admin guards a route with the constant-time-compared admin key.
func (s *Server) admin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(bearer(r)), []byte(s.adminKey)) != 1 {
			writeErr(w, http.StatusUnauthorized, "invalid admin key")
			return
		}
		next(w, r)
	}
}

// appAuth resolves the per-application key and checks it matches the path app id.
func (s *Server) appAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		app, err := s.store.ApplicationByKey(r.Context(), bearer(r))
		if err != nil || app.ID != r.PathValue("app") {
			writeErr(w, http.StatusUnauthorized, "invalid application key")
			return
		}
		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode response", "err", err)
	}
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if _, err := s.queue.Depth(r.Context()); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "redis unavailable")
		return
	}
	if _, _, err := s.store.DeliveryState(r.Context(), "dlv_readycheck"); err != nil && !strings.Contains(err.Error(), "no rows") {
		writeErr(w, http.StatusServiceUnavailable, "postgres unavailable")
		return
	}
	w.WriteHeader(200)
}
