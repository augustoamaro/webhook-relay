package store

import (
	"context"
	"testing"
	"time"

	"github.com/augustoamaro/webhook-relay/internal/domain"
)

func TestBreakerDisablesEndpointAndDeadsPending(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	app, _, _ := s.CreateApplication(ctx, "acme")
	ep, _ := s.CreateEndpoint(ctx, app.ID, "https://a.example/h", nil)
	res, _ := s.IngestMessage(ctx, app.ID, "order.created", []byte(`{}`), "", "")

	var n int
	var err error
	for range domain.BreakerThreshold {
		n, err = s.IncEndpointFailures(ctx, ep.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if n != domain.BreakerThreshold {
		t.Fatalf("count: %d", n)
	}
	if err := s.TripBreaker(ctx, ep.ID); err != nil {
		t.Fatal(err)
	}
	eps, _ := s.ListEndpoints(ctx, app.ID)
	if eps[0].Status != "disabled" {
		t.Fatal("endpoint should be disabled")
	}
	dead, _, err := s.ListDeliveries(ctx, app.ID, "dead", 10)
	if err != nil || len(dead) != 1 || dead[0].DeadReason != "endpoint_disabled" {
		t.Fatalf("pending deliveries should be dead(endpoint_disabled): %v %+v", err, dead)
	}
	_ = res
}

func TestRedriveResetsDeadDeliveries(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	app, _, _ := s.CreateApplication(ctx, "acme")
	_, _ = s.CreateEndpoint(ctx, app.ID, "https://a.example/h", nil)
	res, _ := s.IngestMessage(ctx, app.ID, "order.created", []byte(`{}`), "", "")
	dlvID := res.DeliveryIDs[0]
	c, _ := s.ClaimDelivery(ctx, dlvID)
	if c == nil {
		t.Fatal("claim")
	}
	_ = s.FinishFailure(ctx, dlvID, 0, true, 500, "boom", "max_attempts")

	count, err := s.Redrive(ctx, app.ID, nil, "", time.Time{}, time.Time{})
	if err != nil || count != 1 {
		t.Fatalf("redrive: %v %d", err, count)
	}
	if c2, _ := s.ClaimDelivery(ctx, dlvID); c2 == nil {
		t.Fatal("redriven delivery must be claimable again")
	}
}
