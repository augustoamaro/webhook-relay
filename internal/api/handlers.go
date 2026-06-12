package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/augustoamaro/webhook-relay/internal/queue"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func (s *Server) handleCreateApp(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Name == "" {
		writeErr(w, http.StatusBadRequest, "name is required")
		return
	}
	app, key, err := s.store.CreateApplication(r.Context(), in.Name)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "create application failed")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id": app.ID, "name": app.Name, "created_at": app.CreatedAt,
		"api_key": key, // shown exactly once
	})
}

func (s *Server) handleCreateEndpoint(w http.ResponseWriter, r *http.Request) {
	var in struct {
		URL        string   `json:"url"`
		EventTypes []string `json:"event_types"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.URL == "" {
		writeErr(w, http.StatusBadRequest, "url is required")
		return
	}
	ep, err := s.store.CreateEndpoint(r.Context(), r.PathValue("app"), in.URL, in.EventTypes)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "create endpoint failed")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id": ep.ID, "url": ep.URL, "event_types": ep.EventTypes, "status": ep.Status,
		"secret": ep.Secret, // shown exactly once
	})
}

func (s *Server) handleListEndpoints(w http.ResponseWriter, r *http.Request) {
	eps, err := s.store.ListEndpoints(r.Context(), r.PathValue("app"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list endpoints failed")
		return
	}
	out := make([]map[string]any, 0, len(eps))
	for _, ep := range eps {
		out = append(out, map[string]any{
			"id": ep.ID, "url": ep.URL, "event_types": ep.EventTypes,
			"status": ep.Status, "consecutive_failures": ep.ConsecutiveFailures,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"endpoints": out})
}

func (s *Server) handlePatchEndpoint(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || (in.Status != "enabled" && in.Status != "disabled") {
		writeErr(w, http.StatusBadRequest, `status must be "enabled" or "disabled"`)
		return
	}
	if err := s.store.SetEndpointStatus(r.Context(), r.PathValue("ep"), in.Status); err != nil {
		writeErr(w, http.StatusNotFound, "endpoint not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": in.Status})
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, s.maxPayload+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read body failed")
		return
	}
	if int64(len(body)) > s.maxPayload {
		writeErr(w, http.StatusRequestEntityTooLarge, "payload exceeds limit")
		return
	}
	var in struct {
		EventType string          `json:"event_type"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(body, &in); err != nil || in.EventType == "" || len(in.Payload) == 0 {
		writeErr(w, http.StatusBadRequest, "event_type and payload are required")
		return
	}

	// Store the ingest trace context on the deliveries (spec: span links).
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(r.Context(), carrier)

	res, err := s.store.IngestMessage(r.Context(), r.PathValue("app"),
		in.EventType, in.Payload, r.Header.Get("Idempotency-Key"), carrier["traceparent"])
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "ingest failed")
		return
	}
	if !res.Duplicate && len(res.DeliveryIDs) > 0 {
		// Hot path: best-effort enqueue. The sweeper is the guarantee.
		items := make([]queue.Item, len(res.DeliveryIDs))
		for i, id := range res.DeliveryIDs {
			items[i] = queue.Item{DeliveryID: id, Traceparent: carrier["traceparent"]}
		}
		if err := s.queue.Enqueue(r.Context(), items); err != nil {
			// Log-and-continue: the message is durably committed.
			_ = err
		}
	}
	code := http.StatusAccepted
	if res.Duplicate {
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]any{
		"id": res.Message.ID, "event_type": res.Message.EventType,
		"created_at": res.Message.CreatedAt, "duplicate": res.Duplicate,
		"deliveries": len(res.DeliveryIDs),
	})
}

func (s *Server) handleMessageStatus(w http.ResponseWriter, r *http.Request) {
	out, err := s.store.MessageStatus(r.Context(), r.PathValue("app"), r.PathValue("msg"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "message not found")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleListDeliveries(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	if status == "" {
		status = "dead"
	}
	list, total, err := s.store.ListDeliveries(r.Context(), r.PathValue("app"), status, 100)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list deliveries failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": list, "total": total})
}

func (s *Server) handleRedrive(w http.ResponseWriter, r *http.Request) {
	var in struct {
		DeliveryIDs []string   `json:"delivery_ids"`
		EndpointID  string     `json:"endpoint_id"`
		Since       *time.Time `json:"since"`
		Until       *time.Time `json:"until"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	var since, until time.Time
	if in.Since != nil {
		since = *in.Since
	}
	if in.Until != nil {
		until = *in.Until
	}
	n, err := s.store.Redrive(r.Context(), r.PathValue("app"), in.DeliveryIDs, in.EndpointID, since, until)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "redrive failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"redriven": n})
}
