package domain

import "time"

// DeliveryStatus is the lifecycle state of a single webhook delivery.
type DeliveryStatus string

// Delivery lifecycle states.
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

// Application is the top-level tenant; each application owns its own endpoints.
type Application struct {
	ID        string
	Name      string
	CreatedAt time.Time
}

// Endpoint is a webhook target URL registered under an application.
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

// Message is an inbound event to be fanned out to the application's endpoints.
type Message struct {
	ID             string
	ApplicationID  string
	EventType      string
	Payload        []byte
	IdempotencyKey string
	CreatedAt      time.Time
}
