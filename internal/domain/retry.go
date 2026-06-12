package domain

import (
	"math/rand/v2"
	"time"
)

// schedule[i] is the base delay after attempt i+1 fails.
var schedule = []time.Duration{
	5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute,
	30 * time.Minute, 2 * time.Hour, 5 * time.Hour,
}

// NextDelay returns the jittered (±20%) delay before the next attempt, given
// how many attempts have completed. ok=false means the budget is exhausted
// and the delivery goes to the DLQ.
func NextDelay(completedAttempts int, rng *rand.Rand) (time.Duration, bool) {
	if completedAttempts >= MaxAttempts || completedAttempts < 1 {
		return 0, false
	}
	base := schedule[completedAttempts-1]
	jitter := 0.8 + 0.4*rng.Float64() // [0.8, 1.2)
	return time.Duration(float64(base) * jitter), true
}
