package domain

import "time"

type DeliveryStatus string

const (
	StatusPending    DeliveryStatus = "pending"
	StatusDelivering DeliveryStatus = "delivering"
	StatusSucceeded  DeliveryStatus = "succeeded"
	StatusFailed     DeliveryStatus = "failed"
	StatusDead       DeliveryStatus = "dead"
)

const (
	// MaxAttempts is the total attempt budget per delivery (1 first + 7 retries).
	MaxAttempts = 8
	// BreakerThreshold disables an endpoint after this many consecutive failures.
	BreakerThreshold = 20
	// LeaseSeconds is how long a 'delivering' claim is honored before the
	// delivery becomes eligible again (crashed-worker recovery).
	LeaseSeconds = 60
)

type Application struct {
	ID        string
	Name      string
	CreatedAt time.Time
}

type Endpoint struct {
	ID                  string
	ApplicationID       string
	URL                 string
	Secret              string
	EventTypes          []string
	Status              string // "enabled" | "disabled"
	ConsecutiveFailures int
	CreatedAt           time.Time
}

type Message struct {
	ID             string
	ApplicationID  string
	EventType      string
	Payload        []byte
	IdempotencyKey string
	CreatedAt      time.Time
}
