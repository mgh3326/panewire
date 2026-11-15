package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// The node's own end of a reviewer or consult job that never ran `panewire
// done` (hk 4747). A reviewer usually ends itself: it posts its result and runs
// `panewire done`, or the hub ends it on a posted result
// (hub_spawn_reviewer_end.go). Two cases have no such owner, and the node that
// spawned the pane owns them:
//
//   - the pane is gone: neither herdr list carries it any more, so the job can
//     never finish and is reaped as pane_gone;
//   - the pane is alive but posted no result (a findings object or a report on
//     disk) before the request's time limit plus a grace: the job is ended and
//     the existing one-hour pane close applies.
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
// waits for a reviewer's result before it ends the job.
const reviewerNoResultGrace = 15 * time.Minute

// paneSweepFindingsMaxBytes bounds the findings read the way `panewire done`
// bounds a result (readResult): a larger file could never complete.
const paneSweepFindingsMaxBytes = 64 << 10

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
		reason := s.abandonedEnd(ctx, a, j, agents, panes, now)
		if reason == "" {
			continue
		}
		if err := s.endAbandonedJob(ctx, a, j, reason, now); err != nil {
			if !errors.Is(err, errJobAlreadyEnded) {
				a.warn("job %s not ended (%s): %v", j.ID, reason, err)
			}
			continue
		}
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
		s.reapSweptPane(ctx, a, latest, j, "", paneSweepEnd{state: "lost", reason: SweepPaneGone, byJob: true}, TokenUsage{}, false, now)
		swept = append(swept, SweptPane{Job: j.ID, Pane: j.Pane, Role: paneSweepRole(j), Reason: SweepPaneGone})
	}
	return swept
}

// abandonedEnd is the end_reason the node writes for a live reviewer or consult
// job, or "" while the job still works. A pane is gone only when both herdr
// lists lost it; a live pane is ended only after the request's time limit plus
// reviewerNoResultGrace with no result on disk. The open-question guard runs
// under the operation lock, just before the writes (endAbandonedJob).
func (s *TaskPaneSweeper) abandonedEnd(ctx context.Context, a *App, j Job, agents []Agent, panes *paneSweepPaneRows, now time.Time) string {
	if _, stopped := announcedQuotaStop(j); stopped {
		return ""
	}
	if paneSessionProof(j, agents) == "pane_unlisted" {
		listed, err := panes.listed(ctx, j.Pane)
		if err == nil && !listed {
			return SweepPaneGone
		}
	}
	posted, known := paneSweepJobResult(a, j.ID)
	if !known || posted {
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

// endAbandonedJob writes the job's terminal hk record and then its final
// job.lost, in that order, so the pane sweep never sees an end hk has not
// recorded. The proof and both writes run under the job's operation lock, which
// `panewire done` and the quota-stop record take: none of them lands between.
func (s *TaskPaneSweeper) endAbandonedJob(ctx context.Context, a *App, j Job, reason string, now time.Time) error {
	unlock, err := a.lockJobOperation(j.ID, quotaRecordLockWait)
	if err != nil {
		return err
	}
	defer unlock()
	latest, err := a.job(j.ID)
	if err != nil {
		return err
	}
	if !paneSweepAbandoned(latest) || latest.Pane != j.Pane || latest.Label != j.Label {
		return errJobAlreadyEnded
	}
	// Re-check the question under the lock, as the pane close does: one written
	// before the lock is seen, one written after waits for it.
	if !paneSweepNoOpenQuestion(a, latest, now) {
		return errJobAlreadyEnded
	}
	// A result written while the pass waited for the lock is the job finishing
	// its work: the hub's posted-result end owns it, not this timeout.
	if reason == ReviewerNoResultTimeout {
		if posted, known := paneSweepJobResult(a, latest.ID); !known || posted {
			return errJobAlreadyEnded
		}
	}
	store, err := a.jobStore()
	if err != nil {
		return err
	}
	rec, err := store.Get(ctx, j.ID)
	switch {
	case errors.Is(err, errJobNotFound):
		// hk holds no record to mark; the node's ledger is what ends the job
		// for the pane sweep.
	case err != nil:
		return err
	default:
		switch state, _ := rec["state"].(string); state {
		case "claimed", "spawned":
			if err := store.Put(ctx, j.ID, map[string]any{"state": "lost", "at": now.UTC(), "end_reason": reason}); err != nil {
				return err
			}
		case "lost":
			// This end's hk write landed; its ledger event may not have.
			if str(rec, "end_reason") != reason {
				return errJobAlreadyEnded
			}
		default:
			return errJobAlreadyEnded
		}
	}
	return a.event(ctx, j.ID, "job.lost", map[string]any{"pane_id": j.Pane, "reason": reason, "final": true})
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

// paneSweepJobResult reports whether the job posted a result: a findings
// object (an object whose `findings` is an array; JSON null and any other shape
// are absent, as the hub's result reader counts them), or a report on disk. A
// file that cannot be read is unknown, so the caller never ends a job it could
// not judge.
func paneSweepJobResult(a *App, id string) (posted, known bool) {
	root, err := os.OpenRoot(filepath.Join(a.Jobs, id))
	if errors.Is(err, os.ErrNotExist) {
		return false, true
	}
	if err != nil {
		return false, false
	}
	defer root.Close()
	info, err := root.Lstat("report.md")
	switch {
	case err == nil && !info.Mode().IsRegular():
		return false, false
	case err == nil:
		return true, true
	case !errors.Is(err, os.ErrNotExist):
		return false, false
	}
	info, err = root.Lstat("findings.json")
	if errors.Is(err, os.ErrNotExist) {
		return false, true
	}
	if err != nil {
		return false, false
	}
	if !info.Mode().IsRegular() {
		return false, false
	}
	file, err := root.Open("findings.json")
	if err != nil {
		return false, false
	}
	body, err := io.ReadAll(io.LimitReader(file, paneSweepFindingsMaxBytes+1))
	file.Close()
	if err != nil {
		return false, false
	}
	if len(body) > paneSweepFindingsMaxBytes {
		// A result too large to complete through `panewire done` is not one.
		return false, true
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil || object == nil {
		return false, true
	}
	var findings []json.RawMessage
	raw, ok := object["findings"]
	if !ok || json.Unmarshal(raw, &findings) != nil || findings == nil {
		return false, true
	}
	return true, true
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
