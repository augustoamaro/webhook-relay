package domain

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestNextDelaySchedule(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	bases := []time.Duration{
		5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute,
		30 * time.Minute, 2 * time.Hour, 5 * time.Hour,
	}
	for i, base := range bases {
		completed := i + 1 // attempts completed so far
		d, ok := NextDelay(completed, rng)
		if !ok {
			t.Fatalf("attempt %d: expected a retry", completed)
		}
		lo, hi := time.Duration(float64(base)*0.8), time.Duration(float64(base)*1.2)
		if d < lo || d > hi {
			t.Errorf("attempt %d: delay %v outside [%v, %v]", completed, d, lo, hi)
		}
	}
}

func TestNextDelayExhausted(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	if _, ok := NextDelay(MaxAttempts, rng); ok {
		t.Fatal("attempt budget exhausted: expected ok=false")
	}
}
