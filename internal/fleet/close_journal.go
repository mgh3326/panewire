package fleet

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
)

// A pane-close operation journal records, in the job's own directory, every
// close the hub asks this node to make under a request id (hk 4594;
// docs/node-commands.md, "Close operations"). It survives a node restart, so
// the hub can ask what became of close X and is never left to guess between a
// refused close, a lost acknowledgement and a pane that vanished on its own.
// The job's watcher does not read it yet; that, and the hub's use of the
// operations, are hk 4652 and hk 4632.

// CloseOpDir is the directory, inside a job directory, holding one file per
// close operation. Names beginning with a dot are reserved for the journal's
// own files (temporary writes); an operation id never begins with one.
const CloseOpDir = "close-ops"

// Close operation states. Registered and Closing are in progress; the rest are
// final for the request id.
const (
	// CloseOpRegistered: recorded before any herdr read; no close has run.
	CloseOpRegistered = "registered"
	// CloseOpClosing: recorded after the first guarded look at the pane and
	// before the final one that precedes `herdr pane close`.
	CloseOpClosing = "closing"
	// CloseOpClosed: herdr reported the close done.
	CloseOpClosed = "closed"
	// CloseOpRefused: the pane no longer matched, so it was left running.
	CloseOpRefused = "refused"
	// CloseOpGone: the pane was absent before this operation could have
	// closed it: it vanished on its own.
	CloseOpGone = "gone"
	// CloseOpUnknown: a close may have run but its result was never recorded
	// (a node restart or a failed herdr close while closing).
	CloseOpUnknown = "unknown"
	// CloseOpCancelled: released while still registered, before any close
	// could start; the pane was never touched.
	CloseOpCancelled = "cancelled"
	// CloseOpPreempted: a resumed operation whose attempt had moved on or
	// ended; nothing was closed.
	CloseOpPreempted = "preempted"
)

var closeOpIDPattern = regexp.MustCompile(`^[A-Za-z0-9_:-][A-Za-z0-9._:-]{0,127}$`)

// ValidCloseOpID reports whether id can name a close operation file.
func ValidCloseOpID(id string) bool { return closeOpIDPattern.MatchString(id) }

// CloseOp is one journal entry.
type CloseOp struct {
	RequestID string `json:"request_id"`
	PaneID    string `json:"pane_id"`
	// Attempt is the job attempt the operation was registered against
	// (AttemptKey); an operation owns nothing of a later attempt.
	Attempt string    `json:"attempt"`
	State   string    `json:"state"`
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	// Released is set when the hub has finished with the operation.
	Released *time.Time `json:"released,omitempty"`
}

// Final reports whether the state is the operation's last.
func (op CloseOp) Final() bool {
	switch op.State {
	case CloseOpClosed, CloseOpRefused, CloseOpGone, CloseOpUnknown, CloseOpCancelled, CloseOpPreempted:
		return true
	}
	return false
}

// AttemptKey names the job's current attempt: the event file of the newest
// claim, spawn or re-prompt, in the order the watcher reads them.
func AttemptKey(events []Event) string {
	key := ""
	for _, ev := range events {
		switch ev.Kind {
		case "job.claim", "job.claimed", "job.reclaim", "job.spawned", "job.reprompted":
			key = ev.name
		}
	}
	return key
}

// JobDirAttemptKey is AttemptKey for the job directory root job, read in the
// order the watcher reads it. A symlinked event is refused, as there.
func JobDirAttemptKey(job *os.Root) (string, error) {
	binding, err := ReadJobBinding(job, "")
	return binding.Attempt, err
}

// JobBinding is what a job's ledger says its current attempt is: the pane and
// herdr label it runs in, its role, and whether it has already ended.
type JobBinding struct {
	Attempt, Pane, Label, Role string
	Ended                      bool
}

// ReadJobBinding reads the job directory root job the way its watcher reads it.
func ReadJobBinding(job *os.Root, id string) (JobBinding, error) {
	events, err := job.OpenRoot("events")
	if err != nil {
		return JobBinding{}, err
	}
	defer events.Close()
	list, err := loadEventsFS(events.FS())
	if err != nil {
		return JobBinding{}, err
	}
	binding := JobBinding{Attempt: AttemptKey(list)}
	j, err := jobFromEvents(id, list)
	if err != nil {
		return binding, nil // no claim or receipt: nothing binds a pane
	}
	// A quota stop announced for the hub's hand-over is not an end: the
	// hub's close is what follows it.
	_, announced := announcedQuotaStop(j)
	binding.Pane, binding.Label, binding.Ended = j.Pane, j.Label, (j.Terminal && !announced) || j.Reaped
	binding.Role = firstNonempty(j.SpawnRole, j.Role)
	return binding, nil
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

// ensureCloseOpDir creates the journal directory, durably, if it is missing.
func ensureCloseOpDir(job *os.Root) error {
	if err := job.Mkdir(CloseOpDir, 0o700); err == nil {
		// The new directory's entry must survive a crash with the file in it.
		if parent, err := job.Open("."); err == nil {
			_ = parent.Sync()
			_ = parent.Close()
		}
	} else if !errors.Is(err, fs.ErrExist) {
		return err
	}
	return nil
}

func replaceInCloseOpDir(job *os.Root, name string, raw []byte) error {
	if err := ensureCloseOpDir(job); err != nil {
		return err
	}
	ops, err := job.OpenRoot(CloseOpDir)
	if err != nil {
		return err
	}
	defer ops.Close()
	return durable.ReplaceFileAtomicallyInRoot(ops, name, raw, 0o600)
}

// WriteCloseOp durably replaces the operation's file: written mode 0600,
// fsynced, then renamed into place, so a crash leaves the old or the new entry.
func WriteCloseOp(job *os.Root, op CloseOp) error {
	if !ValidCloseOpID(op.RequestID) {
		return errors.New("invalid close operation id")
	}
	raw, err := json.Marshal(op)
	if err != nil {
		return err
	}
	return replaceInCloseOpDir(job, op.RequestID+".json", raw)
}
