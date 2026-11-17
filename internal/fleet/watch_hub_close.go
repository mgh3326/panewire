package fleet

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// A hub close is the hub ending a job by closing its pane through a node close
// operation (hk 4652; docs/node-commands.md, "Hub close protocol"). The node
// records a hub-close intent in the job's ledger, with the planned end_reason,
// before it registers the operation, and the node settles it: once the
// operation is final it writes exactly the intended hk record, releases the
// lease and records the final job.lost (HubCloseSettlement). While the intent
// is open the job's watcher writes no terminal record of its own; it only
// defers.
//
// It is a sibling of the quota-stop intent (watch_quota_stop.go), not a reuse:
// that one cancels on a missing pane, and a hub close makes the pane missing on
// purpose.
const (
	hubCloseIntentKind = "job.hub_close_intent"
	hubCloseCancelKind = "job.hub_close_cancelled"
	// hubCloseLostReason is the job.lost reason the node records when it
	// settles a hub close.
	hubCloseLostReason = "hub_close"
)

// HubCloseIntent is what the node records before it registers a hub close
// operation: the request, the attempt and receipt it is bound to, and the
// end_reason of the hk lost record that ends the job.
type HubCloseIntent struct {
	RequestID string
	Attempt   string
	Pane      string
	Label     string
	EndReason string
}

// RecordHubCloseIntent appends job.hub_close_intent to job's ledger under the
// jobs root.
func RecordHubCloseIntent(ctx context.Context, jobs, job string, in HubCloseIntent) error {
	home, _ := os.UserHomeDir()
	return (&App{Jobs: jobs, Home: home, Now: time.Now}).recordHubCloseIntent(ctx, job, in)
}

// recordHubCloseIntent appends the intent and then fsyncs the events
// directory itself: the ledger's own directory sync is best effort, and the
// watcher reads intents from the event files, so the close must not proceed on
// an intent a crash could drop.
func (a *App) recordHubCloseIntent(ctx context.Context, job string, in HubCloseIntent) error {
	return a.eventSynced(ctx, job, hubCloseIntentKind, map[string]any{
		"request_id": in.RequestID, "attempt": in.Attempt, "pane_id": in.Pane, "label": in.Label, "end_reason": in.EndReason,
	})
}

// eventSynced appends a hub-close protocol event and fsyncs the events
// directory, returning its error: the ledger's own directory sync is best
// effort, and these events are what a close is admitted or released on.
func (a *App) eventSynced(ctx context.Context, job, kind string, payload map[string]any) error {
	if e := a.event(ctx, job, kind, payload); e != nil {
		return e
	}
	dir, e := os.Open(filepath.Join(a.Jobs, job, "events"))
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}

// Two per-job lock files coordinate a hub close with the job's watcher, which
// is a separate process:
//
//   - jobEndLock is held by the node while it binds and records a hub-close
//     intent, and by the watcher around every terminal or expiry decision,
//     which it re-checks against a freshly read ledger under the lock. So no
//     watcher end can interleave with an intent being recorded.
//   - hubCloseWatchLock is held for its whole life by a watcher that defers to
//     hub-close intents, drops decisions a settled close made stale and lends
//     its watch lock to the node (watch_lock_handoff.go). A v2 watcher (no
//     lending) is incompatible too. A
//     watcher of an older binary holds only the watch lock, or the watch lock
//     and the v1 lock (hubCloseWatchLockV1, which lacks the stale-decision
//     check); the node refuses to open a hub close while either runs.
//
// A kernel lock lives exactly as long as its process, so neither can be
// certified by a stale file or a reused pid.
const (
	jobEndLock          = ".panewire-end.lock"
	jobWatchLock        = ".panewire-watch.lock"
	hubCloseWatchLock   = ".panewire-watch-hubclose-v3.lock"
	hubCloseWatchLockV1 = ".panewire-watch-hubclose.lock"
	// hubCloseWatchLockV2 is held by a watcher that defers but cannot lend
	// its watch lock (BorrowWatchLock): incompatible.
	hubCloseWatchLockV2 = ".panewire-watch-hubclose-v2.lock"
)

// FenceWatchers takes the job's watch lock for the node, without blocking,
// and returns its release; ok is false when a watcher holds it. While the
// node holds it, no watcher of any binary, started by any launcher, can run
// for the job: each exits at its own tryLock. The node holds it from a hub
// close's admission until its settlement ends (hk 4632).
func FenceWatchers(jobDir string) (release func(), ok bool, err error) {
	f, e := secureOpen(filepath.Join(jobDir, jobWatchLock), true, nil)
	if e != nil {
		return nil, false, e
	}
	if !tryLock(f) {
		f.Close()
		return nil, false, nil
	}
	return func() { f.Close() }, true, nil
}

// LockJobEnd takes the job's end lock, blocking, and returns its release.
func LockJobEnd(jobDir string) (func(), error) {
	f, e := secureOpen(filepath.Join(jobDir, jobEndLock), true, nil)
	if e != nil {
		return nil, e
	}
	if e := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); e != nil {
		f.Close()
		return nil, e
	}
	return func() { f.Close() }, nil
}

// LockJobEndContext takes the job's end lock, waiting at most until ctx is
// done, so a daemon step never blocks unbounded behind a slow holder.
func LockJobEndContext(ctx context.Context, jobDir string) (func(), error) {
	f, e := secureOpen(filepath.Join(jobDir, jobEndLock), true, nil)
	if e != nil {
		return nil, e
	}
	for !tryLock(f) {
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return func() { f.Close() }, nil
}

// HubCloseWatcher is what runs as a job's watcher, for a hub close.
type HubCloseWatcher int

const (
	// HubCloseWatcherNone: no watcher runs; the node settles the close
	// without one.
	HubCloseWatcherNone HubCloseWatcher = iota
	// HubCloseWatcherAware: a watcher that defers to hub-close intents runs.
	HubCloseWatcherAware
	// HubCloseWatcherIncompatible: a watcher that does not read hub-close
	// intents (an older binary that outlived a node upgrade) runs.
	HubCloseWatcherIncompatible
)

// ProbeHubCloseWatcher reports which watcher runs for the job directory.
func ProbeHubCloseWatcher(jobDir string) (HubCloseWatcher, error) {
	held := func(name string) (bool, error) {
		f, e := secureOpen(filepath.Join(jobDir, name), false, nil)
		if errors.Is(e, fs.ErrNotExist) {
			return false, nil
		} else if e != nil {
			return false, e
		}
		defer f.Close()
		return !tryLock(f), nil
	}
	if aware, e := held(hubCloseWatchLock); e != nil || aware {
		return HubCloseWatcherAware, e
	}
	// A watcher takes the watch lock before the hub-close one, so a watcher
	// caught in between reads as incompatible: the hub retries. So does a v1
	// watcher, which holds the v1 lock.
	if running, e := held(jobWatchLock); e != nil || running {
		return HubCloseWatcherIncompatible, e
	}
	for _, older := range []string{hubCloseWatchLockV1, hubCloseWatchLockV2} {
		if held, e := held(older); e != nil || held {
			return HubCloseWatcherIncompatible, e
		}
	}
	return HubCloseWatcherNone, nil
}

// RecordHubCloseCancel closes the hub-close intent of request id in job's
// ledger: the hub released a request the node never registered, so no close
// will run for it.
func RecordHubCloseCancel(ctx context.Context, jobs, job, id, reason string) error {
	home, _ := os.UserHomeDir()
	a := &App{Jobs: jobs, Home: home, Now: time.Now}
	return a.eventSynced(ctx, job, hubCloseCancelKind, map[string]any{"request_id": id, "reason": reason})
}

// OpenCloseOps lists the job's close operations for attempt that are not yet
// final, so the node can refuse a second, overlapping one.
func OpenCloseOps(job *os.Root, attempt string) ([]CloseOp, error) {
	dir, e := job.Open(CloseOpDir)
	if errors.Is(e, fs.ErrNotExist) {
		return nil, nil
	} else if e != nil {
		return nil, e
	}
	names, e := dir.Readdirnames(-1)
	dir.Close()
	if e != nil {
		return nil, e
	}
	var open []CloseOp
	for _, name := range names {
		if strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			continue
		}
		raw, e := job.ReadFile(filepath.Join(CloseOpDir, name))
		if e != nil {
			return nil, e
		}
		var op CloseOp
		if e := json.Unmarshal(raw, &op); e != nil {
			return nil, e
		}
		if op.Attempt == attempt && !op.Final() {
			open = append(open, op)
		}
	}
	return open, nil
}

// endUnlessHubClose runs end, a terminal or expiry decision of the watcher,
// under the job's end lock, unless a freshly read ledger shows an open hub
// close: deferred is then true and end does not run.
//
// seen is the snapshot the decision was taken on. If, under the lock, the
// attempt changed or a hub close settled since seen, the decision is stale
// and is discarded the same way (deferred true): the next poll decides on the
// fresh ledger. Without this, a close the node settled between the poll and
// the lock would read as no open intent, and a stale job.reprompted would
// follow the final job.lost (#572 r4576226003).
func (a *App) endUnlessHubClose(id string, seen Job, end func(j Job) error) (deferred bool, err error) {
	unlock, e := LockJobEnd(filepath.Join(a.Jobs, id))
	if e != nil {
		return false, e
	}
	defer unlock()
	j, e := a.job(id)
	if e != nil {
		return false, e
	}
	if _, open := pendingHubCloseIntent(j); open || hubCloseSettledNow(j) {
		return true, nil
	}
	if endMark(j) != endMark(seen) {
		return true, nil
	}
	return false, end(j)
}

// pendingHubCloseIntent returns the job's current hub-close intent: the
// newest intent recorded for the current attempt and its receipt's pane and
// label that no cancellation of its own request closed, and that no end or new
// attempt followed. An intent naming another attempt, pane or label is stale
// and ignored, as the node's binding ignores it (node_pane_close_op.go
// closeTargetBound). A newer request that closed nothing hands the job back
// to an older one still unresolved (an unknown close whose pane is listed).
func pendingHubCloseIntent(j Job) (HubCloseIntent, bool) {
	in, open, _, _ := hubCloseIntentState(j)
	return in, open
}

// hubCloseIntentState is pendingHubCloseIntent plus ended: whether an
// independent local end of the attempt (job.revoked, a bare job.lost, a
// completion) followed the intent. Such an end does not close the intent
// (#572 r4576225999): the node may already have closed the pane, and only
// settlement, which reads the close journal, decides whether the intent is
// settled (a closed operation) or cancelled (nothing registered or closed).
//
// reassigned is whether a hub revocation record (a job.revoked carrying the
// hub's ownership epoch) is in the current attempt's ledger, before or after
// the intent: the hub gave the job to another
// node, so this node must not write the shared hk record (#662 review).
func hubCloseIntentState(j Job) (in HubCloseIntent, open, ended, reassigned bool) {
	all, ended, reassigned := openHubCloseIntents(j)
	if len(all) == 0 {
		return HubCloseIntent{}, false, false, false
	}
	return all[len(all)-1], true, ended, reassigned
}

// openHubCloseIntents is every open intent of the job's current attempt,
// oldest first (hubCloseIntentState folds the same ledger).
func openHubCloseIntents(j Job) (intents []HubCloseIntent, ended, reassigned bool) {
	var attempt, pane, label string
	// The protocol is folded in ledger sequence (file-name) order, as the
	// node's readNodeJobLedgerEvents folds it: j.Events is ordered by
	// timestamp, which a clock stepped backwards reorders.
	for _, ev := range ledgerOrder(j.Events) {
		switch ev.Kind {
		case "job.claim", "job.claimed", "job.reclaim":
			attempt, pane, label, intents, ended, reassigned = ev.name, "", "", nil, false, false
		case "job.spawned":
			attempt, pane, label, intents, ended, reassigned = ev.name, str(ev.Payload, "pane_id"), str(ev.Payload, "label"), nil, false, false
			if label == "" {
				label = str(ev.Payload, "agent_label")
			}
		case "job.reprompted":
			attempt, intents, ended, reassigned = ev.name, nil, false, false
		case hubCloseIntentKind:
			in := HubCloseIntent{RequestID: str(ev.Payload, "request_id"), Attempt: str(ev.Payload, "attempt"), Pane: str(ev.Payload, "pane_id"), Label: str(ev.Payload, "label"), EndReason: str(ev.Payload, "end_reason")}
			if ValidCloseOpID(in.RequestID) && in.EndReason != "" && in.Attempt == attempt && in.Pane == pane && in.Pane != "" && in.Label == label {
				// A repeat of a request records its intent again.
				intents = slices.DeleteFunc(intents, func(o HubCloseIntent) bool { return o.RequestID == in.RequestID })
				intents = append(intents, in)
			}
		case hubCloseCancelKind:
			id := str(ev.Payload, "request_id")
			intents = slices.DeleteFunc(intents, func(o HubCloseIntent) bool { return o.RequestID == id })
		case "job.revoked":
			ended = ended || len(intents) > 0
			// A hub revocation reassigns the attempt whatever its order
			// relative to the intent: the hub allocates its sequence without
			// the ledger lock, so it can sort before a concurrent intent
			// (#662 r4597976581).
			if ev.Kind == ev.Type && ev.Epoch > 0 {
				reassigned = true
			}
		case "job.completed", "job.reaped", "job.completion", "job.record_settled", "job.joined", "job.handed_off", "job.escalate":
			ended = ended || len(intents) > 0
		case "job.lost":
			switch {
			case hubCloseSettledLoss(ev):
				// The node's settlement: the one event that closes intents.
				intents = nil
			case str(ev.Payload, "reason") != "quota_stop" || boolean(ev.Payload, "final", false):
				// An announced quota stop is the hand-over the hub closes
				// for; any other loss ends the attempt locally.
				ended = ended || len(intents) > 0
			}
		}
	}
	if len(intents) == 0 {
		return nil, false, false
	}
	return intents, ended, reassigned
}

// hubCloseSettledLoss reports whether ev is the final job.lost the node's
// settlement records (finishHubClose).
func hubCloseSettledLoss(ev Event) bool {
	return ev.Kind == "job.lost" && str(ev.Payload, "reason") == hubCloseLostReason && boolean(ev.Payload, "final", false)
}

// hubCloseSettledNow reports whether the job's current attempt, folded in
// ledger sequence order, already ended by a settled hub close. It is
// terminal for the watcher whatever else is active (a quota hand-off) and
// whatever the event timestamps say (#662 review).
func hubCloseSettledNow(j Job) bool {
	settled := false
	for _, ev := range ledgerOrder(j.Events) {
		switch {
		case ev.Kind == "job.claim" || ev.Kind == "job.claimed" || ev.Kind == "job.reclaim" || ev.Kind == "job.spawned" || ev.Kind == "job.reprompted":
			settled = false
		case hubCloseSettledLoss(ev):
			settled = true
		}
	}
	return settled
}

// endMark identifies, in a folded ledger, the attempt and how many hub closes
// settled: the newest attempt event's ledger name and the newest settled
// hub-close loss's. A watcher decision taken on a snapshot is stale once the
// mark under the end lock differs (#572 r4576226003).
func endMark(j Job) [2]string {
	var mark [2]string
	for _, ev := range ledgerOrder(j.Events) {
		switch {
		case ev.Kind == "job.claim" || ev.Kind == "job.claimed" || ev.Kind == "job.reclaim" || ev.Kind == "job.spawned" || ev.Kind == "job.reprompted":
			mark[0] = ev.name
		case hubCloseSettledLoss(ev):
			mark[1] = ev.name
		}
	}
	return mark
}

// ledgerOrder returns events in ledger sequence order (CompareLedgerNames).
func ledgerOrder(events []Event) []Event {
	out := slices.Clone(events)
	slices.SortStableFunc(out, func(x, y Event) int { return CompareLedgerNames(x.name, y.name) })
	return out
}

// CompareLedgerNames orders two ledger event file names ("<seq>-<kind>.json")
// by their numeric sequence prefix, then by name: the ledger's own order,
// independent of timestamps and of the prefix's width. The node's ledger
// reader and the watcher's hub-close fold both use it.
func CompareLedgerNames(x, y string) int {
	seq := func(name string) (int, bool) {
		i := strings.IndexByte(name, '-')
		if i <= 0 {
			return 0, false
		}
		n, err := strconv.Atoi(name[:i])
		return n, err == nil
	}
	nx, okx := seq(x)
	ny, oky := seq(y)
	if okx && oky && nx != ny {
		return cmp.Compare(nx, ny)
	}
	return strings.Compare(x, y)
}

// readJobCloseOp reads the job's close operation id from its journal, made
// durable first: a state a failed fsync left visible is not acted on.
func (a *App) readJobCloseOp(job, id string) (CloseOp, bool, error) {
	root, err := os.OpenRoot(filepath.Join(a.Jobs, job))
	if err != nil {
		return CloseOp{}, false, err
	}
	defer root.Close()
	op, found, err := ReadCloseOp(root, id)
	if err == nil && found {
		err = SyncCloseOps(root)
	}
	return op, found, err
}

// HubCloseSettlement is the node's settlement of hub closes: the one writer
// of a hub close's terminal hk record, lease release and final job.lost. App
// carries the jobs root, the clock, the local quota ledger and the node's own
// hk JobStore (never the watcher's credential resolution). PaneListed, when
// set, reads the full pane inventory in a herdr session; nil reads it through
// App's runner.
type HubCloseSettlement struct {
	App        *App
	PaneListed func(ctx context.Context, session, pane string) (bool, error)
	// Store, when set, resolves App's hk store the first time a step reads or
	// writes hk; a step that only cancels locally never calls it.
	Store func() (JobStore, error)
}

// HubCloseState is what one settlement step left.
type HubCloseState int

const (
	// HubCloseNone: the job has no open hub-close intent.
	HubCloseNone HubCloseState = iota
	// HubClosePending: the close or its settlement is unfinished; a later
	// step (a repeat, close_status, the daemon retry or a node start) settles
	// it.
	HubClosePending
	// HubCloseSettled: the job was finished, or another terminal hk record
	// was kept and the closed pane's lease released.
	HubCloseSettled
	// HubCloseCancelled: the request closed nothing (or was orphaned); its
	// intent was cancelled.
	HubCloseCancelled
)

// NewHubCloseSettlement builds the node's settlement from exactly what it
// needs: the jobs root, the home holding the local quota ledger, the node's hk
// store, its clock, its pane inventory reader and a warning sink. It loads no
// hosts.toml or policy, so an unrelated configuration error never strands an
// accepted close.
func NewHubCloseSettlement(jobs, home string, store func() (JobStore, error), now func() time.Time, listed func(ctx context.Context, session, pane string) (bool, error), warn io.Writer) HubCloseSettlement {
	return HubCloseSettlement{App: &App{Jobs: jobs, Home: home, Now: now, Runner: ExecRunner{}, Out: warn, Err: warn}, PaneListed: listed, Store: store}
}

// Settle drives the job's current hub close one step, idempotently.
func (s HubCloseSettlement) Settle(ctx context.Context, job string) (HubCloseState, error) {
	needHK := func() error {
		if s.App.JobStore != nil || s.Store == nil {
			return nil
		}
		store, err := s.Store()
		if err != nil {
			return err
		}
		s.App.JobStore = store
		return nil
	}
	return s.App.settleHubClose(ctx, job, s.PaneListed, needHK)
}

// HubCloseFenceState reads, from the job's ledger in sequence order, whether
// any hub-close intent is still open (open), and whether the job's attempt is
// still live (live: not ended, not reaped), so a watcher should run once no
// close owns it. The caller holds the job's end lock (hk 4632).
func HubCloseFenceState(jobs, job string) (open, live bool, err error) {
	a := &App{Jobs: jobs}
	evs, err := a.events(job)
	if err != nil {
		return false, false, err
	}
	j, err := foldJob(job, ledgerOrder(evs))
	if err != nil {
		return false, false, err
	}
	_, open = pendingHubCloseIntent(j)
	// Watchable as the re-arm pass counts it: an announced quota stop is
	// terminal locally but its hand-off still needs a watcher.
	_, handOff := announcedQuotaStop(j)
	return open, (!j.Terminal || handOff) && !j.Reaped && !hubCloseSettledNow(j), nil
}

// lockUnlessHubCloseOwnsEnd takes the job's end lock for a generic end (reap,
// pane sweep, an abandoned review) and reads, under it, whether a hub close
// still owns the job's end: an intent is open in its ledger, read in sequence
// order. When owned (or the lock or the ledger cannot be read: fail closed),
// it returns owned true and holds nothing; the caller leaves the hk record,
// the lease and job.reaped to settlement, which writes the planned record
// (#662 review: a generic lost written first would be kept and lose the
// planned reason). Otherwise the caller holds the end lock through its whole
// end and runs unlock after it, so no close is admitted in between.
func (a *App) lockUnlessHubCloseOwnsEnd(ctx context.Context, id string) (owned bool, unlock func()) {
	lockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	release, err := LockJobEndContext(lockCtx, filepath.Join(a.Jobs, id))
	if err != nil {
		return true, nil
	}
	if hubCloseOpenNow(a, id) {
		release()
		return true, nil
	}
	return false, release
}

// lockUnchangedEnd protects an end decided from seen. The ledger is append
// ordered; checking the complete event sequence also covers a new attempt,
// claim, spawn, terminal event, or a changed receipt while the lock waited.
// A failed or changed read cannot authorize an end.
func (a *App) lockUnchangedEnd(ctx context.Context, seen Job) (Job, func(), string) {
	owned, unlock := a.lockUnlessHubCloseOwnsEnd(ctx, seen.ID)
	if owned {
		return Job{}, nil, "hub_close_owns_end"
	}
	current, err := a.job(seen.ID)
	if err != nil {
		unlock()
		return Job{}, nil, "job_unreadable"
	}
	if !sameJobLedger(seen, current) {
		unlock()
		return Job{}, nil, "job_changed"
	}
	return current, unlock, ""
}

func sameJobLedger(a, b Job) bool {
	return reflect.DeepEqual(ledgerOrder(a.Events), ledgerOrder(b.Events))
}

// hubCloseOpenNow reads whether any hub-close intent of the job is open. A
// job with no events directory has none; any other unreadable ledger answers
// unreadable, which a caller decides on (a generic end fails closed).
func hubCloseOpenNow(a *App, id string) bool {
	open, readable := hubCloseOpenRead(a, id)
	return open || !readable
}

func hubCloseOpenRead(a *App, id string) (open, readable bool) {
	evs, err := a.events(id)
	if errors.Is(err, fs.ErrNotExist) {
		return false, true
	}
	if err != nil {
		return false, false
	}
	j, err := foldJob(id, ledgerOrder(evs))
	if err != nil {
		return false, false
	}
	_, open = pendingHubCloseIntent(j)
	return open, true
}

// PendingHubCloseJobs lists the jobs under the jobs root whose ledger holds an
// open hub-close intent, or whose ledger cannot be read: the node's startup
// and retry pass settles them, and the rollback drain waits on them.
func PendingHubCloseJobs(jobs string) ([]string, error) {
	entries, err := os.ReadDir(jobs)
	if err != nil {
		return nil, err
	}
	a := &App{Jobs: jobs}
	var out []string
	for _, entry := range entries {
		if !entry.IsDir() || !validID(entry.Name()) {
			continue
		}
		// An unreadable ledger blocks the rollback even if no readable file
		// happens to name an intent.
		evs, err := a.events(entry.Name())
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			out = append(out, entry.Name())
			continue
		}
		// Reaped is folded in ledger sequence order, as settlement folds it.
		j, err := foldJob(entry.Name(), ledgerOrder(evs))
		if err != nil {
			out = append(out, entry.Name())
			continue
		}
		if _, open := pendingHubCloseIntent(j); open {
			out = append(out, entry.Name())
		}
	}
	return out, nil
}

// hubClosePanePresent reads the full pane inventory in the receipt's herdr
// session. The agent list alone cannot prove a pane is gone.
func (a *App) hubClosePanePresent(ctx context.Context, j Job, listed func(ctx context.Context, session, pane string) (bool, error)) (bool, error) {
	route := a.laneRoute(j)
	if listed != nil {
		return listed(ctx, route.herdrSession(), j.Pane)
	}
	panes, e := route.panes(ctx)
	if e != nil {
		return false, e
	}
	for _, p := range panes {
		if p.Pane == j.Pane {
			return true, nil
		}
	}
	return false, nil
}

// PaneListedIn reports whether a herdr pane list answer names pane; the
// answer is decoded strictly, so an unreadable one is an error.
func PaneListedIn(out []byte, pane string) (bool, error) {
	panes, err := parsePaneList(out)
	if err != nil {
		return false, err
	}
	for _, p := range panes {
		if p.Pane == pane {
			return true, nil
		}
	}
	return false, nil
}

// settleHubClose drives the job's current hub close one step, under the
// job's end lock, on the intent current under that lock. The node's journal
// decides first: an operation not yet final (or not yet registered) stays
// pending. closed proves the pane stopped; gone and unknown do only once the
// full pane inventory in the receipt's session shows the pane absent.
// refused, cancelled and preempted closed nothing and cancel the intent. Then
// hk, through the node's own JobStore: a live record takes exactly the
// intended lost record; the intended record already there is accepted;
// another terminal record is kept, and the lease is still released and the
// intent closed, since the pane is proven stopped.
func (a *App) settleHubClose(ctx context.Context, id string, listed func(ctx context.Context, session, pane string) (bool, error), needHK func() error) (HubCloseState, error) {
	unlock, e := LockJobEndContext(ctx, filepath.Join(a.Jobs, id))
	if e != nil {
		return HubClosePending, e
	}
	defer unlock()
	// The intent is chosen from the ledger read under the lock: the node may
	// have recorded a newer one since the poll's snapshot. The job (its
	// receipt pane and session, reaped) is folded in the same sequence order
	// as the intent, never by timestamp.
	evs, e := a.events(id)
	if e != nil {
		return HubClosePending, e
	}
	j, e := foldJob(id, ledgerOrder(evs))
	if e != nil {
		return HubClosePending, e
	}
	// A reaped job's open intent is still settled (#662 review): a reaper
	// may have run while hk was unavailable, and only settlement writes the
	// planned record.
	// Every open intent is driven, newest first (#662 review): a newer close
	// left registered must not mask an older one that already closed the
	// pane. The first settled one finishes the job (its final job.lost
	// closes every intent); otherwise any pending one keeps the job pending.
	intents, ended, reassigned := openHubCloseIntents(j)
	if len(intents) == 0 {
		return HubCloseNone, nil
	}
	result := HubCloseCancelled
	for i := len(intents) - 1; i >= 0; i-- {
		state, err := a.settleHubCloseIntent(ctx, j, intents[i], ended, reassigned, listed, needHK)
		switch {
		case err != nil:
			return HubClosePending, err
		case state == HubCloseSettled:
			return state, nil
		case state == HubClosePending:
			result = HubClosePending
		}
	}
	return result, nil
}

// settlementBetweenDrainAndJournalReads is a test seam that runs between
// settlement's drain-marker read and its close-journal read.
var settlementBetweenDrainAndJournalReads func()

// settleHubCloseIntent drives one open intent one step; the caller holds the
// job's end lock and folded j under it.
func (a *App) settleHubCloseIntent(ctx context.Context, j Job, in HubCloseIntent, ended, reassigned bool, listed func(ctx context.Context, session, pane string) (bool, error), needHK func() error) (HubCloseState, error) {
	cancel := func(reason string) (HubCloseState, error) {
		return HubCloseCancelled, a.eventSynced(ctx, j.ID, hubCloseCancelKind, map[string]any{"request_id": in.RequestID, "reason": reason})
	}
	// Read the drain marker first. Once it is visible every admitted close
	// has finished and none can start, so a journal read after it is current;
	// the reverse order can pair a stale registered entry with a fresh marker.
	draining, drainErr := closeOpsDraining(a.Jobs)
	if drainErr != nil {
		return HubClosePending, drainErr
	}
	if settlementBetweenDrainAndJournalReads != nil {
		settlementBetweenDrainAndJournalReads()
	}
	op, found, err := a.readJobCloseOp(j.ID, in.RequestID)
	switch {
	case err != nil:
		a.warn("hub close of job %s waits: close operation unreadable: %v", j.ID, err)
		return HubClosePending, nil
	case found && op.Attempt != in.Attempt:
		return cancel("stale")
	case draining && (!found || op.State == CloseOpRegistered):
		// Withdrawal waited for the entire admission/close transaction and
		// refuses new retries. No close can still use this registration.
		// Persist a tombstone before cancelling the intent, so a later replay
		// cannot register it after readiness is enabled again.
		root, err := os.OpenRoot(filepath.Join(a.Jobs, j.ID))
		if err != nil {
			return HubClosePending, err
		}
		now := a.Now().UTC()
		if !found {
			op = CloseOp{RequestID: in.RequestID, PaneID: in.Pane, Attempt: in.Attempt, Created: now}
		}
		op.State, op.Updated, op.Released = CloseOpCancelled, now, &now
		err = WriteCloseOp(root, op)
		root.Close()
		if err != nil {
			return HubClosePending, err
		}
		return cancel("rollback")
	case draining && op.State == CloseOpClosing:
		// The old operation is no longer running. Its outcome is unknown,
		// exactly as a replay after interruption would report.
		root, err := os.OpenRoot(filepath.Join(a.Jobs, j.ID))
		if err != nil {
			return HubClosePending, err
		}
		now := a.Now().UTC()
		op.State, op.Updated, op.Released = CloseOpUnknown, now, &now
		err = WriteCloseOp(root, op)
		root.Close()
		if err != nil {
			return HubClosePending, err
		}
	case !found && ended:
		// The job ended locally before the operation was registered: no
		// close ran, and a repeat could no longer bind it.
		return cancel("ended")
	case !found:
		// The node recorded the intent but never registered the operation (a
		// crash, journal_failed). The hub repeats the
		// request or releases it, which cancels the intent. A job hk already
		// ended can no longer be bound, so a repeat would only answer
		// mismatch: the intent is orphaned and cancelled.
		if e := needHK(); e != nil {
			return HubClosePending, e
		}
		if state, _, ok := a.hkJobState(ctx, j.ID); ok && (state == "completed" || state == "lost" || state == "escalated") {
			return cancel("orphaned")
		}
		return HubClosePending, nil
	case !op.Final():
		// Still registered or closing: the hub repeats the request until it
		// is final (docs/node-commands.md).
		return HubClosePending, nil
	}
	switch op.State {
	case CloseOpClosed:
	case CloseOpGone, CloseOpUnknown:
		// gone came from the node's agent list, and unknown may not have
		// closed anything: both wait for the pane inventory to show the
		// pane absent.
		present, err := a.hubClosePanePresent(ctx, j, listed)
		if err != nil {
			return HubClosePending, nil
		}
		if present {
			if draining {
				if op.State == CloseOpUnknown {
					// An unknown close may have stopped the original pane, and
					// its id may have been reused. Only hand it back to the
					// watcher if a fresh agent row proves this attempt owns it.
					route := a.laneRoute(j)
					agents, err := route.agents(ctx)
					if err != nil || paneRecordedSession(j) == "" || paneOwnershipProof(j, agents) != "" {
						return HubClosePending, nil
					}
				}
				// No close was proven to have stopped the original pane.
				// Hand it back to its watcher without writing an hk loss.
				return cancel("rollback_unproven")
			}
			return HubClosePending, nil
		}
	default:
		return cancel("not_closed")
	}
	if reassigned {
		// The pane is proven stopped, but the hub reassigned the job: its
		// shared record belongs to the new owner. Release this node's lease
		// and close the intent without touching hk.
		a.warn("hub close of job %s: the hub reassigned the job; its hk record is left to the new owner", j.ID)
		return a.finishHubClose(ctx, j, in, true), nil
	}
	if e := needHK(); e != nil {
		return HubClosePending, e
	}
	state, reason, ok := a.hkJobState(ctx, j.ID)
	switch {
	case !ok:
		return HubClosePending, nil
	case state == "lost" && reason == in.EndReason:
	case state == "completed" || state == "lost" || state == "escalated":
		a.warn("hub close of job %s: hk holds another terminal record (%s); kept", j.ID, state)
		return a.finishHubClose(ctx, j, in, true), nil
	default:
		// Re-read immediately before the shared write too: a revocation
		// already in the local ledger at this reread stops the write, and it
		// fails closed if another ledger writer changed ownership since the
		// first fold. This is defence in depth only; the local end lock does
		// not fence a hub reassignment. The hub refuses to reassign a job with
		// a non-final close action (docs/node-commands.md, I1/I2, hk 4745).
		events, err := a.events(j.ID)
		if err != nil {
			return HubClosePending, err
		}
		current, err := foldJob(j.ID, ledgerOrder(events))
		if err != nil {
			return HubClosePending, err
		}
		intents, _, revoked := openHubCloseIntents(current)
		if !slices.ContainsFunc(intents, func(open HubCloseIntent) bool { return open == in }) {
			return HubClosePending, nil
		}
		if revoked {
			return a.finishHubClose(ctx, current, in, true), nil
		}
		if e := a.putJobRecord(ctx, j.ID, map[string]any{"state": "lost", "end_reason": in.EndReason, "at": a.Now().UTC()}); e != nil {
			a.warn("hub close lost record not written, retrying: %v", e)
			return HubClosePending, nil
		}
	}
	return a.finishHubClose(ctx, j, in, false), nil
}

// finishHubClose finishes a job whose pane the hub close stopped and whose hk
// record is terminal: the lease is released, then the final job.lost closes
// the intent. A failure between the two leaves the intent open, so the next
// settlement step (a repeat, close_status, the daemon retry or a node start)
// repeats both: the lease release re-reads the pools still held, and the
// final job.lost closes the intent, so it is written once.
func (a *App) finishHubClose(ctx context.Context, j Job, in HubCloseIntent, superseded bool) HubCloseState {
	if e := a.releaseQuota(ctx, j); e != nil {
		a.warn("hub close lease release failed, retrying: %v", e)
		return HubClosePending
	}
	payload := map[string]any{"pane_id": j.Pane, "reason": hubCloseLostReason, "final": true, "request_id": in.RequestID, "end_reason": in.EndReason}
	if superseded {
		payload["superseded"] = true
	}
	if e := a.eventSynced(ctx, j.ID, "job.lost", payload); e != nil {
		a.warn("hub close job.lost not recorded, retrying: %v", e)
		return HubClosePending
	}
	return HubCloseSettled
}
