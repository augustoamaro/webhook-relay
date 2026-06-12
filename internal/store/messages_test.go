package store

import (
	"context"
	"testing"
)

func TestFanOutCreatesDeliveriesForMatchingEnabledEndpoints(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	app, _, _ := s.CreateApplication(ctx, "acme")
	epAll, _ := s.CreateEndpoint(ctx, app.ID, "https://a.example/h", nil)
	_, _ = s.CreateEndpoint(ctx, app.ID, "https://b.example/h", []string{"user.updated"}) // filtered out
	epOff, _ := s.CreateEndpoint(ctx, app.ID, "https://c.example/h", nil)
	_ = s.SetEndpointStatus(ctx, epOff.ID, "disabled") // disabled: skipped

	res, err := s.IngestMessage(ctx, app.ID, "order.created", []byte(`{"n":1}`), "", "")
	if err != nil {
		t.Fatal(err)
	}
	if res.Duplicate || len(res.DeliveryIDs) != 1 {
		t.Fatalf("want exactly 1 delivery (epAll=%s), got %+v", epAll.ID, res)
	}
}

func TestIngestIdempotency(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	app, _, _ := s.CreateApplication(ctx, "acme")
	_, _ = s.CreateEndpoint(ctx, app.ID, "https://a.example/h", nil)

	first, err := s.IngestMessage(ctx, app.ID, "order.created", []byte(`{}`), "idem-1", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.IngestMessage(ctx, app.ID, "order.created", []byte(`{}`), "idem-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate || second.Message.ID != first.Message.ID {
		t.Fatalf("duplicate ingest must return the original message: %+v vs %+v", first, second)
	}
	if len(second.DeliveryIDs) != 0 {
		t.Fatal("duplicate ingest must not create new deliveries")
	}
}
