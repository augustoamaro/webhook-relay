package domain

import "testing"

func TestMatches(t *testing.T) {
	cases := []struct {
		name      string
		eventType string
		filter    []string
		want      bool
	}{
		{"empty filter matches all", "order.created", nil, true},
		{"exact match", "order.created", []string{"order.created"}, true},
		{"no match", "order.deleted", []string{"order.created"}, false},
		{"multi filter", "user.updated", []string{"order.created", "user.updated"}, true},
	}
	for _, c := range cases {
		if got := Matches(c.eventType, c.filter); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
