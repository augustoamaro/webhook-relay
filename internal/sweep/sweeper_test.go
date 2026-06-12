package sweep

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/augustoamaro/webhook-relay/internal/queue"
	"github.com/augustoamaro/webhook-relay/internal/store"
	"github.com/redis/go-redis/v9"
)

func TestSweepEnqueuesDueDeliveries(t *testing.T) {
	dbURL, rURL := os.Getenv("RELAY_TEST_DATABASE_URL"), os.Getenv("RELAY_TEST_REDIS_URL")
	if dbURL == "" || rURL == "" {
		t.Skip("RELAY_TEST_DATABASE_URL / RELAY_TEST_REDIS_URL not set")
	}
	ctx := context.Background()
	s, err := store.Open(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	_ = s.Migrate(ctx)
	_ = s.Truncate(ctx)
	opt, _ := redis.ParseURL(rURL)
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	_ = rdb.FlushAll(ctx).Err()
	q, err := queue.New(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}

	app, _, _ := s.CreateApplication(ctx, "acme")
	_, _ = s.CreateEndpoint(ctx, app.ID, "https://a.example/h", nil)
	_, err = s.IngestMessage(ctx, app.ID, "order.created", []byte(`{}`), "", "")
	if err != nil {
		t.Fatal(err)
	}

	n, err := Sweep(ctx, s, q, 100)
	if err != nil || n != 1 {
		t.Fatalf("sweep: %v n=%d", err, n)
	}
	depth, _ := q.Depth(ctx)
	if depth != 1 {
		t.Fatalf("queue depth: %d", depth)
	}
}

func TestRunStopsOnContextCancel(_ *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Run with nil deps must return immediately on cancelled context
	// (the loop checks ctx before touching deps).
	Run(ctx, nil, nil, time.Hour, 1)
}
