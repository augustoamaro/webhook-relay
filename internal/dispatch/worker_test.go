package dispatch

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/augustoamaro/webhook-relay/internal/domain"
	"github.com/augustoamaro/webhook-relay/internal/netguard"
	"github.com/augustoamaro/webhook-relay/internal/store"
)

func testSetup(t *testing.T) (*store.Store, context.Context) {
	t.Helper()
	url := os.Getenv("RELAY_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("RELAY_TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Truncate(ctx); err != nil {
		t.Fatal(err)
	}
	return s, ctx
}

func seed(t *testing.T, s *store.Store, ctx context.Context, url string) (string, string) {
	t.Helper()
	app, _, _ := s.CreateApplication(ctx, "acme")
	ep, _ := s.CreateEndpoint(ctx, app.ID, url, nil)
	res, err := s.IngestMessage(ctx, app.ID, "order.created", []byte(`{"n":1}`), "", "")
	if err != nil || len(res.DeliveryIDs) != 1 {
		t.Fatalf("seed: %v", err)
	}
	_ = ep
	return res.DeliveryIDs[0], res.Message.ID
}

func TestHandleDeliversSignsAndSucceeds(t *testing.T) {
	s, ctx := testSetup(t)
	var gotSig, gotID atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSig.Store(r.Header.Get("webhook-signature"))
		gotID.Store(r.Header.Get("webhook-id"))
		body, _ := io.ReadAll(r.Body)
		// jsonb normalises {"n":1} → {"n": 1} on round-trip through Postgres.
		if string(body) != `{"n": 1}` {
			t.Errorf("body: %s", body)
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dlvID, msgID := seed(t, s, ctx, srv.URL)
	w := New(s, netguard.NewClient(10*time.Second, true), nil)
	w.Handle(ctx, dlvID)

	status, attempts := deliveryState(t, s, ctx, dlvID)
	if status != string(domain.StatusSucceeded) || attempts != 1 {
		t.Fatalf("state: %s/%d", status, attempts)
	}
	if gotID.Load() != msgID || gotSig.Load() == "" {
		t.Fatalf("headers: id=%v sig=%v", gotID.Load(), gotSig.Load())
	}
}

func TestHandleFailureSchedulesRetry(t *testing.T) {
	s, ctx := testSetup(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	dlvID, _ := seed(t, s, ctx, srv.URL)
	w := New(s, netguard.NewClient(10*time.Second, true), nil)
	w.Handle(ctx, dlvID)

	status, attempts := deliveryState(t, s, ctx, dlvID)
	if status != string(domain.StatusFailed) || attempts != 1 {
		t.Fatalf("state: %s/%d", status, attempts)
	}
}

func TestHandleIsNoOpWhenNotClaimable(t *testing.T) {
	s, ctx := testSetup(t)
	calls := atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()
	dlvID, _ := seed(t, s, ctx, srv.URL)
	w := New(s, netguard.NewClient(10*time.Second, true), nil)
	w.Handle(ctx, dlvID)
	w.Handle(ctx, dlvID) // duplicate enqueue: must lose the claim and not POST again
	if calls.Load() != 1 {
		t.Fatalf("POST count: %d", calls.Load())
	}
}

// deliveryState reads status + attempt_count via the exported test hook.
func deliveryState(t *testing.T, s *store.Store, ctx context.Context, id string) (string, int) {
	t.Helper()
	status, attempts, err := s.DeliveryState(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return status, attempts
}
