// Package laneevent holds the lane.event identity rules the root package and
// the fleet spawn engine both apply.
package laneevent

import (
	"strings"
	"unicode/utf8"
)

// idMax bounds a producer event id.
const idMax = 512

// ValidID reports whether value may be a lane.event's producer event id: 1 to
// 512 bytes of valid UTF-8 with no control characters. A job.completed names
// the READY it concludes by that id (result_id), so the same rule holds there.
func ValidID(value string) bool {
	if value == "" || len(value) > idMax || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			return false
		}
	}
	return true
}

// A READY inbox item lists the completions merged into it
// (merged_completions): the hub's dedupe key of each, its NUL separators
// written as newlines. No field of a completion may hold either, so two keys
// stay two entries, and the item's JSON never holds a \u0000, which
// PostgreSQL's JSON operators refuse.

// MergedCompletionKey is one entry of that list for a completion's dedupe key.
func MergedCompletionKey(dedupeKey string) string {
	return strings.ReplaceAll(dedupeKey, "\x00", "\n")
}

// CompletionKeyPrefix starts every entry for one job's job.completed events;
// `spawn --wait` matches it.
func CompletionKeyPrefix(job string) string {
	return MergedCompletionKey("job.completed\x00" + job + "\x00")
}
