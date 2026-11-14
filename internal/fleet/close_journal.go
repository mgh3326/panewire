package fleet

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
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
//
// The node (registration) and the watcher (committing a fallback ending) are
// separate processes. Both take an exclusive flock on close-ops/.lock: the
// watcher checks ownership and leaves a tombstone naming the attempt it is
// ending; the node, registering, finds that tombstone and records the
// operation preempted instead of closing. Whichever side is second sees the
// other's record.

// CloseOpDir is the directory, inside a job directory, holding one file per
// close operation. Names beginning with a dot are the journal's own files
// (lock, markers, temporary writes); an operation id never begins with one.
const CloseOpDir = "close-ops"

const (
	closeOpLockFile    = ".lock"
	closeOpEndedFile   = ".watcher-ended.json"
	closeOpAwareMarker = ".watcher-v1"
)

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
	// CloseOpPreempted: the job's watcher had already committed its own
	// ending of this attempt; nothing was closed.
	CloseOpPreempted = "preempted"
)

// CloseOpOwnershipTTL bounds how long an unreleased operation holds back the
// watcher's own ending. The hub releases every operation it finishes; this is
// only the backstop for a hub that never returns, so a job is not held forever.
const CloseOpOwnershipTTL = 24 * time.Hour

// closeOpOwnershipTTL is CloseOpOwnershipTTL; tests shorten it.
var closeOpOwnershipTTL = CloseOpOwnershipTTL

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
	// Released is set when the hub has finished with the operation; from then
	// on the watcher's fallbacks act again.
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

// ownsEnding reports whether op holds back the watcher's ending at now. A
// refused close left the pane running, and a cancelled or preempted one never
// acted, so none of them owns anything the watcher acts on.
func (op CloseOp) ownsEnding(now time.Time) bool {
	return op.Released == nil && op.State != CloseOpRefused && op.State != CloseOpPreempted && op.State != CloseOpCancelled &&
		now.Before(op.Updated.Add(closeOpOwnershipTTL))
}

// paneMayBeClosed reports whether op found its pane gone or may have closed
// it, so a rearmed watcher that never saw the pane can still settle the
// attempt.
func (op CloseOp) paneMayBeClosed() bool {
	return op.State == CloseOpClosed || op.State == CloseOpGone || op.State == CloseOpUnknown
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

// LockCloseOps takes the journal's cross-process lock for the job directory
// root, blocking until it is free. The node holds it while registering an
// operation, the watcher while committing a fallback ending.
func LockCloseOps(job *os.Root) (unlock func(), err error) {
	if err := ensureCloseOpDir(job); err != nil {
		return nil, err
	}
	file, err := job.OpenFile(filepath.Join(CloseOpDir, closeOpLockFile), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		file.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	}, nil
}

type watcherEnding struct {
	PaneID  string `json:"pane_id"`
	Attempt string `json:"attempt"`
}

// WatcherEnded reports whether the job's watcher committed its own ending of
// the attempt on pane. The caller holds LockCloseOps.
func WatcherEnded(job *os.Root, pane, attempt string) (bool, error) {
	raw, err := job.ReadFile(filepath.Join(CloseOpDir, closeOpEndedFile))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	var ending watcherEnding
	if err := json.Unmarshal(raw, &ending); err != nil {
		return false, err
	}
	return ending.PaneID == pane && ending.Attempt == attempt, nil
}

// CommitWatcherEnding is the watcher's side of the lock: under it, an
// operation that owns the attempt's ending wins (false); otherwise the
// watcher records that it is ending the attempt, so a registration after it
// is preempted, and may proceed (true). An error proceeds nowhere.
func CommitWatcherEnding(dir, pane, attempt string, now time.Time) (bool, error) {
	job, err := os.OpenRoot(dir)
	if err != nil {
		return false, err
	}
	defer job.Close()
	unlock, err := LockCloseOps(job)
	if err != nil {
		return false, err
	}
	defer unlock()
	if owns, _, _ := closeOpsFor(dir, pane, attempt, now); owns {
		return false, nil
	}
	raw, err := json.Marshal(watcherEnding{PaneID: pane, Attempt: attempt})
	if err != nil {
		return false, err
	}
	return true, replaceInCloseOpDir(job, closeOpEndedFile, raw)
}

// RecordWatcherEnding leaves the watcher's tombstone for the attempt on pane
// under the journal lock, without deferring to an operation: for an ending
// that is authoritative anyway (completion), so a later registration is
// preempted.
func RecordWatcherEnding(dir, pane, attempt string) error {
	job, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer job.Close()
	unlock, err := LockCloseOps(job)
	if err != nil {
		return err
	}
	defer unlock()
	raw, err := json.Marshal(watcherEnding{PaneID: pane, Attempt: attempt})
	if err != nil {
		return err
	}
	return replaceInCloseOpDir(job, closeOpEndedFile, raw)
}

// MarkJournalAwareWatcher records that a watcher which honours the journal
// watches the job. A node opens no new operation for a job without it: a
// watcher started by an earlier binary would not wait for the hub. Binaries
// only move forward, so every later watcher of the job honours it too.
func MarkJournalAwareWatcher(dir string) error {
	job, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer job.Close()
	if JournalAwareWatcher(job) {
		return nil
	}
	return replaceInCloseOpDir(job, closeOpAwareMarker, []byte("1\n"))
}

// JournalAwareWatcher reports whether MarkJournalAwareWatcher ran for job.
func JournalAwareWatcher(job *os.Root) bool {
	_, err := job.Stat(filepath.Join(CloseOpDir, closeOpAwareMarker))
	return err == nil
}

// CloseOpState is what the journal says about one attempt on one pane.
type CloseOpState struct {
	// Owns: an operation still owns the ending (or the journal is unreadable).
	Owns bool
	// Released: the hub released an operation that closed or lost the pane;
	// ReleasedAt is the newest such release.
	Released   bool
	ReleasedAt time.Time
	// PaneClosed: an operation found the pane gone or may have closed it.
	PaneClosed bool
}

// ReadCloseOpState reads the journal of the job directory dir for the attempt
// on pane at now. Operations on another pane or attempt count for nothing.
func ReadCloseOpState(dir, pane, attempt string, now time.Time) CloseOpState {
	var state CloseOpState
	state.Owns, state.PaneClosed, state.ReleasedAt = closeOpsScan(dir, pane, attempt, now)
	state.Released = !state.ReleasedAt.IsZero()
	return state
}

func closeOpsFor(dir, pane, attempt string, now time.Time) (owns, released, closed bool) {
	owns, closed, releasedAt := closeOpsScan(dir, pane, attempt, now)
	return owns, !releasedAt.IsZero(), closed
}

func closeOpsScan(dir, pane, attempt string, now time.Time) (owns, closed bool, releasedAt time.Time) {
	entries, err := os.ReadDir(filepath.Join(dir, CloseOpDir))
	if errors.Is(err, fs.ErrNotExist) {
		return false, false, time.Time{}
	} else if err != nil {
		return true, false, time.Time{}
	}
	job, err := os.OpenRoot(dir)
	if err != nil {
		return true, false, time.Time{}
	}
	defer job.Close()
	for _, entry := range entries {
		id, isOp := strings.CutSuffix(entry.Name(), ".json")
		if !isOp || strings.HasPrefix(id, ".") {
			continue // the journal's own files
		}
		op, found, err := ReadCloseOp(job, id)
		if err != nil {
			owns = true
			continue
		}
		if !found || op.PaneID != pane || op.Attempt != attempt {
			continue
		}
		owns = owns || op.ownsEnding(now)
		closed = closed || op.paneMayBeClosed()
		if op.Released != nil && op.paneMayBeClosed() && op.Released.After(releasedAt) {
			releasedAt = *op.Released
		}
	}
	return owns, closed, releasedAt
}
