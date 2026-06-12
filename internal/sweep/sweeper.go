// Package sweep is the delivery guarantee: it rebuilds the dispatch queue
// from the Postgres ledger — after crashes, restarts, or a Redis FLUSHALL.
package sweep

import (
	"context"
	"log/slog"
	"time"

	"github.com/augustoamaro/webhook-relay/internal/queue"
	"github.com/augustoamaro/webhook-relay/internal/store"
)

// Sweep enqueues one batch of due deliveries; returns how many.
func Sweep(ctx context.Context, s *store.Store, q *queue.Queue, batch int) (int, error) {
	due, err := s.DueDeliveries(ctx, batch)
	if err != nil {
		return 0, err
	}
	if len(due) == 0 {
		return 0, nil
	}
	items := make([]queue.Item, len(due))
	for i, d := range due {
		items[i] = queue.Item{DeliveryID: d.ID, Traceparent: d.Traceparent}
	}
	if err := q.Enqueue(ctx, items); err != nil {
		return 0, err
	}
	return len(items), nil
}

// Run sweeps every interval until ctx is cancelled.
func Run(ctx context.Context, s *store.Store, q *queue.Queue, interval time.Duration, batch int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := Sweep(ctx, s, q, batch); err != nil {
				slog.Error("sweep failed", "err", err)
			} else if n > 0 {
				slog.Info("swept", "enqueued", n)
			}
		}
	}
}
