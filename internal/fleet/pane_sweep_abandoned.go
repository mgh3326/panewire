package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"
	"unicode/utf8"
)

// The node's own end of a reviewer or consult job that never ran `panewire
// done` (hk 4747). A reviewer usually ends itself: it posts its result and runs
// `panewire done`, or the hub ends it on a posted result
// (hub_spawn_reviewer_end.go). Two cases have no such owner:
//
//   - the pane is gone: neither herdr list carries it any more;
//   - the pane is alive but the job posted no result before the request's time
//     limit plus a grace: the ended-job close then takes the pane.
//
// Who owns a posted result differs by role, and that is the rule's centre: only
// a reviewer has a second ender (the hub's posted-result pass), so a reviewer's
// valid posted result is never abandoned here. A consult writes its answer as
// its report, no pass completes it, and its consumer reads a lost record as a
// failed consult, so a consult whose pane is lost is cleaned up as an
// abandonment — not accepted. Unjudgeable evidence blocks only the quiet
// timeout: the pass never claims "no result" it cannot prove, while a pane both
// lists lost is proof enough to clean the job up.
//
// An implementer, a job with an open question (the #563 guard) and an announced
// quota stop are never touched here. docs/pane-sweep.md states the rule.

const (
	// SweepPaneGone is the hk end_reason, and the final job.lost reason, of a
	// reviewer or consult job the node ends because its pane is gone.
	SweepPaneGone = "pane_gone"
	// ReviewerNoResultTimeout is the hk end_reason, and the final job.lost
	// reason, of a reviewer or consult job the node ends because its pane
	// posted no result before the request's time limit plus
	// reviewerNoResultGrace.
	ReviewerNoResultTimeout = "reviewer_no_result_timeout"
)

// reviewerNoResultGrace is how long past the request's time limit the node
// waits for a result before it ends the job.
const reviewerNoResultGrace = 15 * time.Minute

// paneSweepFindingsMaxBytes bounds the findings read exactly as the hub's
// result reader does (nodeJobFindingsLimit = 24 KiB, node_job_result.go): a
// findings object over that bound is reported too_large there, so it is no
// posted result here either.
const paneSweepFindingsMaxBytes = 24 << 10

// paneSweepResultEvidence is what the job's result files prove: a posted
// result, no result at all, or a file that cannot be judged.
type paneSweepResultEvidence int

const (
	resultAbsent paneSweepResultEvidence = iota
	resultPosted
	resultUnknown
)

// paneSweepAbandoned reports a live reviewer or consult job this node owns the
// end of: it recorded a pane and a label here, and nobody has ended it.
func paneSweepAbandoned(j Job) bool {
	role := paneSweepRole(j)
	return !j.Reaped && j.Pane != "" && j.Label != "" && (role == "reviewer" || role == "consult") && !paneSweepClosed(j)
}

// endAbandonedReviews ends the live reviewer and consult jobs of this pass
// before its panes are judged. It returns the panes it reaped as gone;
// none of these jobs is one of the pass's candidates.
func (s *TaskPaneSweeper) endAbandonedReviews(ctx context.Context, a *App, jobs []Job, agents []Agent, now time.Time) []SweptPane {
	var swept []SweptPane
	if len(jobs) == 0 {
		return nil
	}
	panes := &paneSweepPaneRows{a: a}
	for _, j := range jobs {
		// The judgment names a fresh end; "" means the job still works. Either
		// way endAbandonedJob runs: hk may already hold this rule's end from a
		// write whose ledger event failed, and only this pass finishes it.
		reason := s.abandonedEnd(ctx, a, j, agents, panes, now)
		recorded, err := s.endAbandonedJob(ctx, a, j, reason, now)
		if errors.Is(err, errJobAlreadyEnded) {
			continue
		}
		if err != nil {
			if reason != "" {
				a.warn("job %s not ended (%s): %v", j.ID, reason, err)
			}
			continue
		}
		reason = recorded
		s.info("node ended a reviewer or consult job", "job", j.ID, "pane", j.Pane, "role", paneSweepRole(j), "reason", reason)
		if reason != SweepPaneGone {
			continue
		}
		// The pane is gone, so there is nothing to keep: the job is reaped
		// where the close path reaps a pane it proved absent.
		latest, err := a.job(j.ID)
		if err != nil {
			continue
		}
		// endAbandonedJob appended exactly one loss to j. If an attempt or
		// any other ledger event intervened before this snapshot, the reap
		// must not carry the old decision into that new state.
		events := ledgerOrder(latest.Events)
		if len(events) != len(j.Events)+1 {
			continue
		}
		last := events[len(events)-1]
		before := latest
		before.Events = events[:len(events)-1]
		if !sameJobLedger(j, before) || last.Kind != "job.lost" || str(last.Payload, "reason") != SweepPaneGone || str(last.Payload, "pane_id") != j.Pane {
			continue
		}
		current, unlock, refusal := a.lockUnchangedEnd(ctx, latest)
		if refusal != "" {
			continue
		}
		agents, agentErr := a.agents(ctx)
		panes, paneErr := a.panes(ctx)
		gone := agentErr == nil && paneErr == nil && paneOwnershipProof(current, agents) == "pane_unlisted"
		for _, pane := range panes {
			if pane.Pane == current.Pane {
				gone = false
			}
		}
		if gone {
			s.reapSweptPaneHeld(ctx, a, current, current, "", paneSweepEnd{state: "lost", reason: SweepPaneGone, byJob: true}, jobSession{}, false, now)
			swept = append(swept, SweptPane{Job: current.ID, Pane: current.Pane, Role: paneSweepRole(current), Reason: SweepPaneGone})
		}
		unlock()
	}
	return swept
}

// abandonedEnd is the end_reason the node writes for a live reviewer or consult
// job, or "" while the job still works or belongs to another owner. A
// reviewer's valid posted result reserves the end for the hub's posted-result
// pass and is never abandoned here, whatever its pane shows. A consult's posted
// result stops the quiet timeout but not a proven pane loss: no other pass ends
// a consult, and a lost record is how its consumer reads a failed consult.
// Unjudgeable evidence blocks only the timeout, never a proven pane loss. A pane
// is gone only when both herdr lists lost it; a live pane is ended only after
// the request's time limit plus reviewerNoResultGrace. The open-question guard
// runs under the operation lock, just before the writes (endAbandonedJob).
func (s *TaskPaneSweeper) abandonedEnd(ctx context.Context, a *App, j Job, agents []Agent, panes *paneSweepPaneRows, now time.Time) string {
	if _, stopped := announcedQuotaStop(j); stopped {
		return ""
	}
	evidence := paneSweepJobResult(a, j.ID)
	if evidence == resultPosted && paneSweepRole(j) == "reviewer" {
		return ""
	}
	if paneOwnershipProof(j, agents) == "pane_unlisted" {
		listed, err := panes.listed(ctx, j.Pane)
		if err == nil && !listed {
			return SweepPaneGone
		}
	}
	// The quiet timeout needs proof that no result exists; a result or an
	// unjudgeable file is not that proof.
	if evidence != resultAbsent {
		return ""
	}
	limit, known := paneSweepRequestLimit(j)
	if !known || now.Sub(limit) < reviewerNoResultGrace {
		return ""
	}
	return ReviewerNoResultTimeout
}

// errJobAlreadyEnded means the job changed under the pass (a completion, a
// revoke, a newer claim or another end won), so there is nothing to write.
var errJobAlreadyEnded = errors.New("job already ended")

// paneSweepResultReadHook is a test seam: it runs under the operation lock just
// before a fresh end re-reads the result files, where a reviewer's write lands.
var paneSweepResultReadHook func(job string)

// endAbandonedJob writes the job's terminal hk record and then its final
// job.lost, in that order, so the pane sweep never sees an end hk has not
// recorded. It returns the end_reason the ledger took, which is the one hk
// already holds when an earlier attempt's hk write landed and its ledger event
// did not. That repair runs even for a call with no fresh reason, before any
// result or question guard: hk's end is already the job's fate, and a result
// that arrived later must not leave the ledger live beside it. A fresh end
// re-proves the policy under the job's operation lock, which `panewire done` and
// the quota-stop record take: none of them lands between.
func (s *TaskPaneSweeper) endAbandonedJob(ctx context.Context, a *App, j Job, reason string, now time.Time) (string, error) {
	ctx = withJobEpoch(ctx, j)
	// The end lock first, as the watcher and settlement take it before the
	// operation lock: a job whose hub close is open is left to settlement,
	// and none is admitted while this end runs (#662 review).
	current, endUnlock, refusal := a.lockUnchangedEnd(ctx, j)
	if refusal != "" {
		return "", errJobAlreadyEnded
	}
	defer endUnlock()
	unlock, err := a.lockJobOperation(ctx, j.ID, quotaRecordLockWait)
	if err != nil {
		return "", err
	}
	defer unlock()
	latest, err := a.job(j.ID)
	if err != nil {
		return "", err
	}
	if !sameJobLedger(current, latest) || !paneSweepAbandoned(latest) || latest.Pane != j.Pane || latest.Label != j.Label {
		return "", errJobAlreadyEnded
	}
	store, err := a.jobStore()
	if err != nil {
		return "", err
	}
	exists := true
	rec, err := store.Get(ctx, j.ID)
	switch {
	case errors.Is(err, errJobNotFound):
		// hk holds no record to mark; the node's ledger is what ends the job
		// for the pane sweep.
		exists = false
	case err != nil:
		return "", err
	default:
		switch state := str(rec, "state"); state {
		case "lost":
			// An earlier attempt's hk write landed; its ledger event may not
			// have. That end is already hk's, so nothing is re-proved here: a
			// result or question that arrived meanwhile cannot reopen it, and
			// hk already holding a terminal record keeps the hub and the close
			// path from ending it any other way. Without this event the ledger
			// stays live and the job is stranded.
			stored := str(rec, "end_reason")
			if stored != SweepPaneGone && stored != ReviewerNoResultTimeout {
				return "", errJobAlreadyEnded
			}
			if err := a.event(ctx, j.ID, "job.lost", map[string]any{"pane_id": j.Pane, "reason": stored, "final": true}); err != nil {
				return "", err
			}
			return stored, nil
		case "claimed", "spawned":
		default:
			return "", errJobAlreadyEnded
		}
	}
	// A fresh end: prove the exclusion conditions, then write hk and the
	// ledger. A pass that judged no end and found none in hk has nothing to do.
	if reason == "" {
		return "", errJobAlreadyEnded
	}
	// Re-check the question under the lock, as the pane close does: one
	// written before the lock is seen, one written after waits for it.
	if !paneSweepNoOpenQuestion(a, latest, now) {
		return "", errJobAlreadyEnded
	}
	if paneSweepResultReadHook != nil {
		paneSweepResultReadHook(latest.ID)
	}
	// The same policy holds under the lock: a reviewer's valid posted result
	// reserves the end for the hub, and the quiet timeout needs proved-absent
	// evidence. A consult's posted result, or an unjudgeable file, does not
	// hold a proven pane loss.
	evidence := paneSweepJobResult(a, latest.ID)
	if evidence == resultPosted && paneSweepRole(latest) == "reviewer" {
		return "", errJobAlreadyEnded
	}
	if reason == ReviewerNoResultTimeout && evidence != resultAbsent {
		return "", errJobAlreadyEnded
	}
	if exists {
		if err := a.putJobRecord(ctx, j.ID, map[string]any{"state": "lost", "at": now.UTC(), "end_reason": reason}); err != nil {
			return "", err
		}
	}
	if err := a.event(ctx, j.ID, "job.lost", map[string]any{"pane_id": j.Pane, "reason": reason, "final": true}); err != nil {
		return "", err
	}
	return reason, nil
}

// paneSweepNoOpenQuestion reports that the job's pane asks no question left
// unanswered (the #563 guard). An unreadable record keeps the job, as the close
// guard keeps the pane.
func paneSweepNoOpenQuestion(a *App, j Job, now time.Time) bool {
	_, open, err := OpenLaneQuestion(filepath.Dir(a.Jobs), j.ID, j.Pane, now, DecisionTimeout())
	return err == nil && !open
}

// paneSweepRequestLimit is when the job's request time limit runs out: the
// expires_at its spawn record carries (hk 4660), else the role's default after
// the spawn. A job with neither a parseable expires_at nor a spawn time has no
// limit this pass.
func paneSweepRequestLimit(j Job) (time.Time, bool) {
	if raw := str(j.Receipt, "expires_at"); raw != "" {
		if at, err := time.Parse(time.RFC3339, raw); err == nil {
			return at, true
		}
	}
	spawned, ok := paneSweepSpawnedAt(j)
	if !ok {
		return time.Time{}, false
	}
	return spawned.Add(SpawnExpiresDefault(paneSweepRole(j))), true
}

// paneSweepSpawnedAt is the time of the job's newest spawn record.
func paneSweepSpawnedAt(j Job) (time.Time, bool) {
	for index := len(j.Events) - 1; index >= 0; index-- {
		event := j.Events[index]
		if event.Kind == "job.spawned" && !event.when.IsZero() {
			return event.when, true
		}
	}
	return time.Time{}, false
}

// paneSweepJobResult reports what the job's result files prove. Both artifacts
// are inspected and a posted one wins over an unjudgeable one: an unreadable
// report must not hide a valid findings object, nor the reverse.
func paneSweepJobResult(a *App, id string) paneSweepResultEvidence {
	root, err := os.OpenRoot(filepath.Join(a.Jobs, id))
	if errors.Is(err, os.ErrNotExist) {
		return resultAbsent
	}
	if err != nil {
		return resultUnknown
	}
	defer root.Close()
	report, findings := paneSweepReport(root), paneSweepFindings(root)
	switch {
	case report == resultPosted || findings == resultPosted:
		return resultPosted
	case report == resultUnknown || findings == resultUnknown:
		return resultUnknown
	}
	return resultAbsent
}

// paneSweepReport classifies report.md: a regular file is a posted result, a
// missing one is absent, and anything that cannot be inspected is unknown.
func paneSweepReport(root *os.Root) paneSweepResultEvidence {
	info, err := root.Lstat("report.md")
	switch {
	case errors.Is(err, os.ErrNotExist):
		return resultAbsent
	case err != nil:
		return resultUnknown
	case !info.Mode().IsRegular():
		return resultUnknown
	}
	return resultPosted
}

// paneSweepFindings classifies findings.json with the hub reader's rules: an
// object whose findings is an array, valid UTF-8, at most the hub's bound. It
// opens with O_NONBLOCK and re-checks the handle, so a name swapped for a FIFO
// never stalls the pass.
func paneSweepFindings(root *os.Root) paneSweepResultEvidence {
	file, err := root.OpenFile("findings.json", os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return resultAbsent
	}
	if err != nil {
		return resultUnknown
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		file.Close()
		return resultUnknown
	}
	body, err := io.ReadAll(io.LimitReader(file, paneSweepFindingsMaxBytes+1))
	file.Close()
	if err != nil {
		return resultUnknown
	}
	if len(body) > paneSweepFindingsMaxBytes || !utf8.Valid(body) {
		// The hub reads these as too_large / not_json: no posted result.
		return resultAbsent
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil || object == nil {
		return resultAbsent
	}
	var findings []json.RawMessage
	raw, ok := object["findings"]
	if !ok || json.Unmarshal(raw, &findings) != nil || findings == nil {
		return resultAbsent
	}
	return resultPosted
}

// paneSweepPaneRows reads `herdr pane list` at most once per pass.
type paneSweepPaneRows struct {
	a    *App
	read bool
	rows []Pane
	err  error
}

// listed reports whether the pane inventory still carries pane.
func (p *paneSweepPaneRows) listed(ctx context.Context, pane string) (bool, error) {
	if !p.read {
		p.rows, p.err = p.a.panes(ctx)
		p.read = true
	}
	if p.err != nil {
		return false, p.err
	}
	return slices.ContainsFunc(p.rows, func(row Pane) bool { return row.Pane == pane }), nil
}
