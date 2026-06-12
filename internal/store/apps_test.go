package store

import (
	"context"
	"strings"
	"testing"
)

func TestCreateApplicationAndAuth(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	app, key, err := s.CreateApplication(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key, "whr_") {
		t.Fatalf("key format: %s", key)
	}
	got, err := s.ApplicationByKey(ctx, key)
	if err != nil || got.ID != app.ID {
		t.Fatalf("lookup by key: %v / %+v", err, got)
	}
	if _, err := s.ApplicationByKey(ctx, "whr_wrong"); err == nil {
		t.Fatal("expected error for unknown key")
	}
}

func TestEndpointLifecycle(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	app, _, _ := s.CreateApplication(ctx, "acme")
	ep, err := s.CreateEndpoint(ctx, app.ID, "https://example.com/hook", []string{"order.created"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ep.Secret, "whsec_") {
		t.Fatalf("secret format: %s", ep.Secret)
	}
	if err := s.SetEndpointStatus(ctx, ep.ID, "disabled"); err != nil {
		t.Fatal(err)
	}
	eps, err := s.ListEndpoints(ctx, app.ID)
	if err != nil || len(eps) != 1 || eps[0].Status != "disabled" {
		t.Fatalf("list: %v %+v", err, eps)
	}
}
