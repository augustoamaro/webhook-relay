package domain

import "github.com/oklog/ulid/v2"

// NewID returns a prefixed, lexicographically sortable id, e.g. "msg_01J...".
func NewID(prefix string) string {
	return prefix + "_" + ulid.Make().String()
}
