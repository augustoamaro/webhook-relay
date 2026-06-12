// Package dispatch is the delivery worker: claim → sign → POST → record → decide.
package dispatch

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/augustoamaro/webhook-relay/internal/domain"
	"github.com/augustoamaro/webhook-relay/internal/signer"
	"github.com/augustoamaro/webhook-relay/internal/store"
)

const snippetCap = 4096

// Metrics is the observability hook; nil disables instrumentation.
type Metrics interface {
	DeliveryAttempt(outcome string, duration time.Duration)
	EndToEnd(d time.Duration)
}

type Worker struct {
	store   *store.Store
	client  *http.Client
	metrics Metrics
}

func New(s *store.Store, client *http.Client, m Metrics) *Worker {
	return &Worker{store: s, client: client, metrics: m}
}

// Handle processes one enqueued delivery id. Losing the claim race is a no-op
// (that is what makes duplicate enqueues and FLUSHALL-rebuilds safe).
func (w *Worker) Handle(ctx context.Context, deliveryID string) {
	cd, err := w.store.ClaimDelivery(ctx, deliveryID)
	if err != nil {
		slog.Error("claim failed", "delivery", deliveryID, "err", err)
		return
	}
	if cd == nil {
		return // lost the race, not due, or terminal
	}

	started := time.Now()
	statusCode, errMsg, snippet := w.attempt(ctx, cd)
	duration := time.Since(started)

	if err := w.store.RecordAttempt(ctx, cd.ID, duration, statusCode, errMsg, snippet); err != nil {
		slog.Error("record attempt failed", "delivery", cd.ID, "err", err)
	}

	success := statusCode >= 200 && statusCode < 300
	if success {
		if err := w.store.FinishSuccess(ctx, cd.ID, statusCode); err != nil {
			slog.Error("finish success failed", "delivery", cd.ID, "err", err)
			return
		}
		if err := w.store.ResetEndpointFailures(ctx, cd.EndpointID); err != nil {
			slog.Error("reset failures failed", "endpoint", cd.EndpointID, "err", err)
		}
		if w.metrics != nil {
			w.metrics.DeliveryAttempt("success", duration)
			w.metrics.EndToEnd(time.Since(cd.MsgCreatedAt))
		}
		return
	}

	completed := cd.AttemptCount + 1
	delay, retry := domain.NextDelay(completed, rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())))
	deadReason := ""
	if !retry {
		deadReason = "max_attempts"
	}
	if err := w.store.FinishFailure(ctx, cd.ID, delay, !retry, statusCode, errMsg, deadReason); err != nil {
		slog.Error("finish failure failed", "delivery", cd.ID, "err", err)
		return
	}
	if w.metrics != nil {
		outcome := "retry"
		if !retry {
			outcome = "dead"
		}
		w.metrics.DeliveryAttempt(outcome, duration)
	}

	n, err := w.store.IncEndpointFailures(ctx, cd.EndpointID)
	if err != nil {
		slog.Error("inc failures failed", "endpoint", cd.EndpointID, "err", err)
		return
	}
	if n >= domain.BreakerThreshold {
		slog.Warn("circuit breaker tripped", "endpoint", cd.EndpointID, "consecutive_failures", n)
		if err := w.store.TripBreaker(ctx, cd.EndpointID); err != nil {
			slog.Error("trip breaker failed", "endpoint", cd.EndpointID, "err", err)
		}
	}
}

// attempt signs and POSTs. statusCode 0 means a transport-level failure.
func (w *Worker) attempt(ctx context.Context, cd *store.ClaimedDelivery) (int, string, string) {
	ts := time.Now()
	sig, err := signer.Sign(cd.Secret, cd.MessageID, ts, cd.Payload)
	if err != nil {
		return 0, "sign: " + err.Error(), ""
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cd.URL, bytes.NewReader(cd.Payload))
	if err != nil {
		return 0, "build request: " + err.Error(), ""
	}
	req.Header.Set("content-type", "application/json")
	for k, v := range signer.Headers(cd.MessageID, ts, sig) {
		req.Header.Set(k, v)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return 0, err.Error(), ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, snippetCap))
	errMsg := ""
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errMsg = "non-2xx response"
	}
	return resp.StatusCode, errMsg, string(body)
}
