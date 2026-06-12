// Package domain holds the core types and business rules; no I/O.
package domain

import "slices"

// Matches reports whether an event type passes an endpoint's filter.
// An empty filter matches every event type; otherwise the match is exact.
func Matches(eventType string, filter []string) bool {
	return len(filter) == 0 || slices.Contains(filter, eventType)
}
