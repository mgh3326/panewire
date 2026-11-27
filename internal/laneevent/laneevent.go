// Package laneevent holds the lane.event identity rules the root package and
// the fleet spawn engine both apply.
package laneevent

import (
	"encoding/json"
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
// reads it as the completion's result_id when no --ready-event was given. A
// later READY replaces the earlier one, so the newest READY is the one the
// completion pairs with.
const ReadyEventFile = ".ready-event-id"

// ReadyEvent is the recorded pairing: the READY's producer event id and the
// grant epoch of the attempt that emitted it. The epoch lets `done` ignore a
// record a superseded or revoked attempt left behind.
type ReadyEvent struct {
	ID    string
	Epoch uint64
}

// readyEventRecord is the file's JSON form.
type readyEventRecord struct {
	EventID string `json:"event_id"`
	Epoch   uint64 `json:"epoch"`
}

// WriteReadyEvent records eventID as the job's READY producer event id under
// grant epoch, replacing any earlier record. The write is atomic and mode
// 0600, like the job dir's other state files.
func WriteReadyEvent(dir string, epoch uint64, eventID string) error {
	body, err := json.Marshal(readyEventRecord{EventID: eventID, Epoch: epoch})
	if err != nil {
		return err
	}
	return durable.ReplaceFileAtomically(filepath.Join(dir, ReadyEventFile), ".ready-event-id-*", body, 0600)
}

// ReadReadyEvent returns the recorded READY event, or ok=false when the file
// is absent, unreadable, or does not hold a lane.event id; the completion
// then goes out without a result_id, exactly as before the record existed.
func ReadReadyEvent(dir string) (ReadyEvent, bool) {
	body, err := os.ReadFile(filepath.Join(dir, ReadyEventFile))
	if err != nil || len(body) > 4096 {
		return ReadyEvent{}, false
	}
	var record readyEventRecord
	if json.Unmarshal(body, &record) != nil || !ValidID(record.EventID) {
		return ReadyEvent{}, false
	}
	return ReadyEvent{ID: record.EventID, Epoch: record.Epoch}, true
}
