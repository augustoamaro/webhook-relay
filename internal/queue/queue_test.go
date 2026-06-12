package queue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testQueue(t *testing.T) *Queue {
	t.Helper()
	url := os.Getenv("RELAY_TEST_REDIS_URL")
	if url == "" {
		t.Skip("RELAY_TEST_REDIS_URL not set; skipping integration test")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	q, err := New(context.Background(), rdb)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestEnqueueConsumeAck(t *testing.T) {
	q, ctx := testQueue(t), context.Background()
	if err := q.Enqueue(ctx, []Item{{DeliveryID: "dlv_1", Traceparent: "tp"}}); err != nil {
		t.Fatal(err)
	}
	got := make(chan Item, 1)
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go q.Consume(cctx, "c1", func(_ context.Context, it Item) {
		got <- it
	})
	select {
	case it := <-got:
		if it.DeliveryID != "dlv_1" || it.Traceparent != "tp" {
			t.Fatalf("item: %+v", it)
		}
	case <-cctx.Done():
		t.Fatal("timed out waiting for consume")
	}
}

func TestReclaimPicksUpUnacked(t *testing.T) {
	q, ctx := testQueue(t), context.Background()
	_ = q.Enqueue(ctx, []Item{{DeliveryID: "dlv_stuck"}})

	// A consumer reads but never acks (simulated crash).
	res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "dead-consumer", Streams: []string{stream, ">"}, Count: 1, Block: time.Second,
	}).Result()
	if err != nil || len(res) == 0 || len(res[0].Messages) != 1 {
		t.Fatalf("setup read: %v", err)
	}

	got := make(chan Item, 1)
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go q.Reclaim(cctx, "rescuer", 0, 100*time.Millisecond, func(_ context.Context, it Item) {
		got <- it
	})
	select {
	case it := <-got:
		if it.DeliveryID != "dlv_stuck" {
			t.Fatalf("item: %+v", it)
		}
	case <-cctx.Done():
		t.Fatal("timed out waiting for reclaim")
	}
}
