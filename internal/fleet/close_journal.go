package fleet

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
)

// A pane-close operation journal records, in the job's own directory, every
// close the hub asks this node to make under a request id (hk 4594;
// docs/node-commands.md, "Close operations"). It survives a node restart, so
// the hub can ask what became of close X and is never left to guess between a
// refused close, a lost acknowledgement and a pane that vanished on its own.
// The job's watcher reads it too: an open operation owns the job's ending, so
// the watcher's missing-pane and quota-grace fallbacks wait for its release.

// CloseOpDir is the directory, inside a job directory, holding one file per
// close operation.
const CloseOpDir = "close-ops"

// Close operation states. Registered and Closing are in progress; the rest are
// final for the request id.
const (
	// CloseOpRegistered: recorded before any herdr read; no close has run.
	CloseOpRegistered = "registered"
	// CloseOpClosing: recorded immediately before `herdr pane close`.
	CloseOpClosing = "closing"
	// CloseOpClosed: herdr reported the close done.
	CloseOpClosed = "closed"
	// CloseOpRefused: the pane no longer matched, so it was left running.
	CloseOpRefused = "refused"
	// CloseOpGone: the pane was absent before this operation tried to close it.
	CloseOpGone = "gone"
	// CloseOpUnknown: the pane is absent after a close was started whose
	// result was never recorded (a node crash or a failed herdr close).
	CloseOpUnknown = "unknown"
	// CloseOpCancelled: released while still registered, before any close
	// was started; the pane was never touched.
	CloseOpCancelled = "cancelled"
)

// CloseOpOwnershipTTL bounds how long an unreleased operation holds back the
// watcher's own ending. The hub releases every operation it finishes; this is
// only the backstop for a hub that never returns, so a job is not held forever.
const CloseOpOwnershipTTL = 24 * time.Hour

// closeOpOwnershipTTL is CloseOpOwnershipTTL; tests shorten it.
var closeOpOwnershipTTL = CloseOpOwnershipTTL

var closeOpIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// ValidCloseOpID reports whether id can name a close operation file.
func ValidCloseOpID(id string) bool { return closeOpIDPattern.MatchString(id) }

// CloseOp is one journal entry.
type CloseOp struct {
	RequestID string    `json:"request_id"`
	PaneID    string    `json:"pane_id"`
	State     string    `json:"state"`
	Created   time.Time `json:"created"`
	Updated   time.Time `json:"updated"`
	// Released is set when the hub has finished with the operation; from then
	// on the watcher's fallbacks act again.
	Released *time.Time `json:"released,omitempty"`
}

// Final reports whether the state is the operation's last.
func (op CloseOp) Final() bool {
	switch op.State {
	case CloseOpClosed, CloseOpRefused, CloseOpGone, CloseOpUnknown, CloseOpCancelled:
		return true
	}
	return false
}

// ownsEnding reports whether op holds back the watcher's ending at now. A
// refused close left the pane running, so it owns nothing the watcher acts on.
func (op CloseOp) ownsEnding(now time.Time) bool {
	return op.Released == nil && op.State != CloseOpRefused && now.Before(op.Updated.Add(closeOpOwnershipTTL))
}

// ReadCloseOp reads one operation from the job directory root; found is false
// when there is none.
func ReadCloseOp(job *os.Root, id string) (CloseOp, bool, error) {
	if !ValidCloseOpID(id) {
		return CloseOp{}, false, errors.New("invalid close operation id")
	}
	raw, err := job.ReadFile(filepath.Join(CloseOpDir, id+".json"))
	if errors.Is(err, fs.ErrNotExist) {
		return CloseOp{}, false, nil
	} else if err != nil {
		return CloseOp{}, false, err
	}
	var op CloseOp
	if err := json.Unmarshal(raw, &op); err != nil || op.RequestID != id {
		return CloseOp{}, false, errors.New("unreadable close operation " + id)
	}
	return op, true, nil
}

// WriteCloseOp durably replaces the operation's file: written mode 0600,
// fsynced, then renamed into place, so a crash leaves the old or the new entry.
func WriteCloseOp(job *os.Root, op CloseOp) error {
	if !ValidCloseOpID(op.RequestID) {
		return errors.New("invalid close operation id")
	}
	if err := job.Mkdir(CloseOpDir, 0o700); err == nil {
		// The new directory's entry must survive a crash with the file in it.
		if parent, err := job.Open("."); err == nil {
			_ = parent.Sync()
			_ = parent.Close()
		}
	} else if !errors.Is(err, fs.ErrExist) {
		return err
	}
	raw, err := json.Marshal(op)
	if err != nil {
		return err
	}
	ops, err := job.OpenRoot(CloseOpDir)
	if err != nil {
		return err
	}
	defer ops.Close()
	return durable.ReplaceFileAtomicallyInRoot(ops, op.RequestID+".json", raw, 0o600)
}

// CloseOpOwnsEnding reports whether an operation on pane in the job directory
// dir still owns the job's ending at now. An operation on another pane belongs
// to an earlier attempt and owns nothing of this one. An unreadable journal
// owns it: the watcher must not consume the terminal record on a guess.
func CloseOpOwnsEnding(dir, pane string, now time.Time) bool {
	owns, _ := closeOpsFor(dir, pane, now)
	return owns
}

// CloseOpReleasedFor reports whether the hub released a finished operation on
// pane: it has ended the job itself, so the watcher only frees what is local.
func CloseOpReleasedFor(dir, pane string) bool {
	_, released := closeOpsFor(dir, pane, time.Time{})
	return released
}

func closeOpsFor(dir, pane string, now time.Time) (owns, released bool) {
	entries, err := os.ReadDir(filepath.Join(dir, CloseOpDir))
	if errors.Is(err, fs.ErrNotExist) {
		return false, false
	} else if err != nil {
		return true, false
	}
	job, err := os.OpenRoot(dir)
	if err != nil {
		return true, false
	}
	defer job.Close()
	for _, entry := range entries {
		id, isOp := strings.CutSuffix(entry.Name(), ".json")
		if !isOp || strings.HasPrefix(id, ".") {
			continue // a temporary file of a write in progress
		}
		op, found, err := ReadCloseOp(job, id)
		if err != nil {
			owns = true
			continue
		}
		if !found || op.PaneID != pane {
			continue
		}
		owns = owns || op.ownsEnding(now)
		released = released || (op.Released != nil && op.State != CloseOpCancelled && op.State != CloseOpRefused)
	}
	return owns, released
}
