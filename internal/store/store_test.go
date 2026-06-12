package store

import (
	"context"
	"os"
	"sync"
	"testing"
)

// testStore opens the disposable test database, migrates, and truncates.
// Every integration test in this package starts from it.
func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("RELAY_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("RELAY_TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
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
	return s
}

func TestMigrateIsIdempotent(t *testing.T) {
	s := testStore(t)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("second migrate failed: %v", err)
	}
}

func TestMigrateConcurrent(t *testing.T) {
	s := testStore(t) // opens + migrates + truncates
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Migrate(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent migrate failed: %v", err)
		}
	}
}
