// Package queue is the only Redis touchpoint. The stream carries delivery ids
// (pointers into Postgres), never payloads — which is what makes Redis
// disposable: FLUSHALL loses nothing the sweeper can't rebuild.
package queue

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	stream = "deliveries"
	group  = "workers"
)

// Item is the unit of work carried by the stream. It holds only a delivery id
// and a traceparent — never the webhook payload itself.
type Item struct {
	DeliveryID  string
	Traceparent string
}

// Queue wraps a Redis client and exposes stream-based dispatch primitives.
type Queue struct {
	rdb *redis.Client
}

// New creates (or re-attaches to) the consumer group on the stream and returns
// a ready Queue. It tolerates BUSYGROUP if the group already exists.
func New(ctx context.Context, rdb *redis.Client) (*Queue, error) {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, err
	}
	return &Queue{rdb: rdb}, nil
}

// Enqueue adds items to the stream in a single pipeline round-trip.
func (q *Queue) Enqueue(ctx context.Context, items []Item) error {
	if len(items) == 0 {
		return nil
	}
	pipe := q.rdb.Pipeline()
	for _, it := range items {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]any{"d": it.DeliveryID, "tp": it.Traceparent},
		})
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Depth returns the stream length (queue-depth metric).
func (q *Queue) Depth(ctx context.Context) (int64, error) {
	return q.rdb.XLen(ctx, stream).Result()
}

func itemFrom(msg redis.XMessage) Item {
	it := Item{}
	if v, ok := msg.Values["d"].(string); ok {
		it.DeliveryID = v
	}
	if v, ok := msg.Values["tp"].(string); ok {
		it.Traceparent = v
	}
	return it
}

// Consume reads new entries for this consumer until ctx is done, calling
// handle for each and acking afterwards. handle must be idempotent
// (at-least-once: a crash between handle and ack means redelivery).
func (q *Queue) Consume(ctx context.Context, consumer string, handle func(context.Context, Item)) {
	for ctx.Err() == nil {
		res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: consumer,
			Streams: []string{stream, ">"},
			Count:   16, Block: 5 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || ctx.Err() != nil {
				continue
			}
			// Transient Redis error (or FLUSHALL nuked the group): recreate and retry.
			_ = q.rdb.XGroupCreateMkStream(ctx, stream, group, "$").Err()
			time.Sleep(time.Second)
			continue
		}
		for _, str := range res {
			for _, msg := range str.Messages {
				handle(ctx, itemFrom(msg))
				q.rdb.XAck(ctx, stream, group, msg.ID)
			}
		}
	}
}

// Reclaim loops XAUTOCLAIM, stealing entries pending longer than minIdle
// (crashed consumers) and processing them.
func (q *Queue) Reclaim(ctx context.Context, consumer string, minIdle, interval time.Duration, handle func(context.Context, Item)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: stream, Group: group, Consumer: consumer,
			MinIdle: minIdle, Start: "0-0", Count: 64,
		}).Result()
		if err != nil {
			continue
		}
		for _, msg := range msgs {
			handle(ctx, itemFrom(msg))
			q.rdb.XAck(ctx, stream, group, msg.ID)
		}
	}
}
