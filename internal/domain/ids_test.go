package domain

import (
	"strings"
	"testing"
)

func TestNewIDPrefixAndUniqueness(t *testing.T) {
	a, b := NewID("msg"), NewID("msg")
	if !strings.HasPrefix(a, "msg_") {
		t.Fatalf("want msg_ prefix, got %s", a)
	}
	if a == b {
		t.Fatal("ids must be unique")
	}
	if len(a) != len("msg_")+26 { // ULID is 26 chars
		t.Fatalf("unexpected length: %s", a)
	}
}
