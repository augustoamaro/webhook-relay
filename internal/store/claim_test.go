package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/augustoamaro/webhook-relay/internal/domain"
)

// seedDelivery creates app + endpoint + 1 message and returns the delivery id.
func seedDelivery(t *testing.T, s *Store) (string, string) {
	t.Helper()
	ctx := context.Background()
	app, _, _ := s.CreateApplication(ctx, "acme")
	_, _ = s.CreateEndpoint(ctx, app.ID, "https://a.example/h", nil)
	res, err := s.IngestMessage(ctx, app.ID, "order.created", []byte(`{"n":1}`), "", "")
	if err != nil || len(res.DeliveryIDs) != 1 {
		t.Fatalf("seed: %v %+v", err, res)
	}
	return res.DeliveryIDs[0], res.Message.ID
}

func TestClaimRaceExactlyOneWinner(t *testing.T) {
	s := testStore(t)
	dlvID, _ := seedDelivery(t, s)
	var wins int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := s.ClaimDelivery(context.Background(), dlvID)
			if err != nil {
				t.Error(err)
				return
			}
			if c != nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("want exactly 1 winner, got %d", wins)
	}
}

func TestFinishFailureSchedulesRetryThenSweeperFeedsIt(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	dlvID, _ := seedDelivery(t, s)
	c, _ := s.ClaimDelivery(ctx, dlvID)
	if c == nil {
		t.Fatal("claim failed")
	}
	// Schedule the retry in the past so it is due immediately.
	if err := s.FinishFailure(ctx, dlvID, -time.Second, false, 500, "boom", ""); err != nil {
		t.Fatal(err)
	}
	due, err := s.DueDeliveries(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].ID != dlvID {
		t.Fatalf("sweeper feed: %+v", due)
	}
	// Throttle: an immediate second sweep must not re-feed the same delivery.
	due2, _ := s.DueDeliveries(ctx, 10)
	if len(due2) != 0 {
		t.Fatalf("expected throttled empty sweep, got %+v", due2)
	}
}

func TestFinishSuccessIsTerminal(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	dlvID, _ := seedDelivery(t, s)
	c, _ := s.ClaimDelivery(ctx, dlvID)
	if c == nil {
		t.Fatal("claim failed")
	}
	if err := s.FinishSuccess(ctx, dlvID, 200); err != nil {
		t.Fatal(err)
	}
	if c2, _ := s.ClaimDelivery(ctx, dlvID); c2 != nil {
		t.Fatal("succeeded delivery must not be claimable")
	}
	_ = domain.StatusSucceeded
}
