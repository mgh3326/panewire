// Package laneevent holds the lane.event identity rules the root package and
// the fleet spawn engine both apply.
package laneevent

import (
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/durable"
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

// ReadyEventFile is the job-dir file that records the producer event id of
// the READY a worker last emitted inside the job (hk 7014). `panewire emit
// --type ready` writes it when the emit resolves a job dir; `panewire done`
// reads it as the completion's result_id when no --ready-event was given. The
// content is the event id and a newline, so a later READY replaces the earlier
// one and the newest READY is the one the completion pairs with.
const ReadyEventFile = ".ready-event-id"

// WriteReadyEvent records eventID as the job's READY producer event id,
// replacing any earlier record. The write is atomic and mode 0600, like the
// job dir's other state files.
func WriteReadyEvent(dir, eventID string) error {
	return durable.ReplaceFileAtomically(filepath.Join(dir, ReadyEventFile), ".ready-event-id-*", []byte(eventID+"\n"), 0600)
}

// ReadReadyEvent returns the recorded READY event id, or "" when the file is
// absent, unreadable, or does not hold a lane.event id; the completion then
// goes out without a result_id, exactly as before the record existed.
func ReadReadyEvent(dir string) string {
	body, err := os.ReadFile(filepath.Join(dir, ReadyEventFile))
	if err != nil {
		return ""
	}
	id := strings.TrimSpace(string(body))
	if !ValidID(id) {
		return ""
	}
	return id
}
