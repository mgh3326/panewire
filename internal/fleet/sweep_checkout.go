package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

var errCheckoutSweepNoWorkRoot = errors.New("checkout sweep: work root unset")

// The checkout sweep (docs/pane-sweep.md, hk 5091) is the node command
// checkout.sweep: it lists the *-checkout worktrees of the node's clones
// under its work root whose job is not live — no running job record, no pane
// — and removes each through the same guarded path the sweep tail uses
// (worktreePlan, removeWorktree). A dry run reports what it would remove and
// changes nothing. The tail removes a job-owned checkout when the job's pane
// is swept; this pass is for the ones that outlived their record — a job dir
// the TTL took, a spawn that failed before its receipt, a --keep job — and
// for the checkouts already lying on the hosts.
//
// A checkout no local record claims is an orphan. It goes only with proof that
// its job ended (orphanEnded) and that no pane is its own or works inside it
// (orphanPaneKeep), and its patch is saved outside the job directory that is
// gone (OrphanPatches, hk 6291).

// CheckoutSweepRow is one *-checkout worktree's verdict. Outcome is
// "removed", "kept" or "would_remove" (dry run only); Reason is the keep
// reason, the removal kind ("discard"), or empty.
type CheckoutSweepRow struct {
	Path    string `json:"path"`
	Job     string `json:"job,omitempty"`
	Outcome string `json:"outcome"`
	Reason  string `json:"reason,omitempty"`
}

// The keep reasons a checkout sweep row can carry beyond the tail's own.
const (
	sweepCheckoutJobLive       = "job_live"
	sweepCheckoutPaneOpen      = "pane_open"
	sweepCheckoutAgentsUnknown = "agents_unreadable"
	sweepCheckoutNoJob         = "no_job"
	sweepCheckoutJobUnreadable = "job_unreadable"
	// An orphan whose end neither hk nor the node's spawns prove.
	sweepCheckoutJobUnconfirmed = "job_unconfirmed"
	// An orphan whose clone another git operation held.
	sweepCheckoutCloneBusy = "clone_busy"
)

// CheckoutSweepLiveKeep reports whether a checkout sweep keep reason means a
// live user still needs the worktree: the job or pane it was prepared for is
// still running, so it leaves on its own when that work ends. The node's
// worktree flow (hk 5117) never counts such a row as made: live inventory is
// not a stuck remover.
func CheckoutSweepLiveKeep(reason string) bool {
	return reason == sweepCheckoutJobLive || reason == sweepCheckoutPaneOpen
}

// CheckoutOrphan is what the node's own spawns say about the job a *-checkout
// belonged to when no local job record claims it. The zero value attributes
// nothing.
type CheckoutOrphan struct {
	Job string
	// Pane and Label are what a spawn of the job reported, empty when none did.
	Pane, Label string
	// InFlight reports a spawn of the job the node has not finished.
	InFlight bool
	// NeverLaunched reports that every spawn of the job finished as a failure
	// without a pane: the one end that needs no hk record.
	NeverLaunched bool
}

// CheckoutSweep holds what one pass needs. OrphanOwner attributes a *-checkout
// directory no local job record claims — from the node's spawn journal on the
// hub path. It is asked again right before an orphan's removal. Info receives
// one line per kept or removed checkout.
type CheckoutSweep struct {
	WorkRoot    string
	DryRun      bool
	OrphanOwner func(dir string) CheckoutOrphan
	// OrphanRecords reads the hk job record that proves an orphan's job ended.
	// Nil proves nothing, so every orphan is kept.
	OrphanRecords JobStore
	// OrphanHold, when set, takes the lock a spawn's worktree preparation
	// takes on clone and returns its release; an orphan's last check and its
	// removal run under it.
	OrphanHold func(ctx context.Context, clone string) (release func(), err error)
	// OrphanPatches is the directory an orphan's patch is saved under, as
	// <OrphanPatches>/<job>/, when its job directory is gone. Empty keeps such
	// a checkout, since its work could not be saved.
	OrphanPatches string
	Info          func(message string, attrs ...any)
}

func (s *CheckoutSweep) info(message string, attrs ...any) {
	if s.Info != nil {
		s.Info(message, attrs...)
	}
}

// Pass lists every *-checkout worktree under WorkRoot that nodeWorktree
// proves is a linked worktree of a clone the node owns, attributes it to a
// job, and keeps or removes it. It returns the rows in path order and never
// fails the whole pass on one candidate: a candidate that cannot be judged
// is kept with its reason.
func (s *CheckoutSweep) Pass(ctx context.Context, a *App) ([]CheckoutSweepRow, error) {
	if s.WorkRoot == "" {
		return nil, errCheckoutSweepNoWorkRoot
	}
	dirs, err := s.checkoutDirs(ctx, a)
	if err != nil {
		return nil, err
	}
	owners, unreadable, err := s.jobOwners(a)
	if err != nil {
		return nil, err
	}
	ts := &TaskPaneSweeper{WorkRoot: s.WorkRoot}
	// The end the removal paths read: a job-owned checkout is discarded
	// whatever it holds, and the reason lands in the patch header.
	end := paneSweepEnd{state: "lost", reason: "checkout_sweep"}
	var agents []Agent
	agentsTried, agentsOK := false, false
	agentsFor := func() ([]Agent, bool) {
		if !agentsTried {
			agentsTried = true
			if list, err := a.agents(ctx); err == nil {
				agents, agentsOK = list, true
			}
		}
		return agents, agentsOK
	}
	rows := make([]CheckoutSweepRow, 0, len(dirs))
	for _, dir := range dirs {
		row := CheckoutSweepRow{Path: dir, Outcome: "kept"}
		j, known := owners[sweepResolved(dir)]
		var orphan CheckoutOrphan
		if !known {
			for _, id := range unreadable {
				if eventsMention(filepath.Join(a.Jobs, id, "events"), dir) {
					row.Reason = sweepCheckoutJobUnreadable
					break
				}
			}
			if row.Reason == "" && s.OrphanOwner != nil {
				// The id names a directory the patch is written to.
				if found := s.OrphanOwner(dir); validID(found.Job) {
					orphan, j, known = found, Job{ID: found.Job, Receipt: map[string]any{"cwd": dir}}, true
				}
			}
		}
		if row.Reason == "" && !known {
			row.Reason = sweepCheckoutNoJob
		}
		if row.Reason == "" {
			row.Job = j.ID
			var record map[string]any
			var keep string
			if orphan.Job == "" {
				keep = s.jobKeep(j, agentsFor)
			} else if record, keep = s.orphanEnded(ctx, orphan); keep == "" {
				keep = s.orphanPaneKeep(ctx, a, orphan, record, dir)
			}
			if keep != "" {
				row.Reason = keep
			} else {
				clone, branch, owned, reason := ts.worktreePlan(ctx, a, j, dir, end)
				switch {
				case reason != "":
					row.Reason = reason
				case s.DryRun:
					row.Outcome, row.Reason = "would_remove", "discard"
				default:
					if reason := s.removeCheckout(ctx, a, j, orphan, record, end, clone, branch, dir, owned); reason == "" {
						row.Outcome = "removed"
					} else {
						row.Reason = reason
					}
				}
			}
		}
		if row.Outcome == "removed" {
			s.info("checkout removed", "job", row.Job, "worktree", filepath.Base(dir))
		} else {
			s.info("checkout kept", "job", row.Job, "worktree", filepath.Base(dir), "reason", row.Reason)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// checkoutDirs lists the *-checkout directories under the work root that are
// linked worktrees of a clone the node owns, in path order.
func (s *CheckoutSweep) checkoutDirs(ctx context.Context, a *App) ([]string, error) {
	entries, err := os.ReadDir(s.WorkRoot)
	if err != nil {
		return nil, err
	}
	ts := &TaskPaneSweeper{WorkRoot: s.WorkRoot}
	var dirs []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, "-checkout") || (!entry.IsDir() && entry.Type()&os.ModeSymlink == 0) {
			continue
		}
		path := filepath.Join(s.WorkRoot, name)
		if _, _, ok := ts.nodeWorktree(ctx, a, path); ok {
			dirs = append(dirs, path)
		}
	}
	sort.Strings(dirs)
	return dirs, nil
}

// jobOwners folds every readable job's receipt cwd into the owner map, keyed
// by resolved path. A job whose events cannot be read is returned by name so
// a candidate can be checked against its event files, as worktreeInUse does.
func (s *CheckoutSweep) jobOwners(a *App) (map[string]Job, []string, error) {
	entries, err := os.ReadDir(a.Jobs)
	if err != nil {
		return nil, nil, err
	}
	owners := make(map[string]Job)
	var unreadable []string
	for _, entry := range entries {
		if !entry.IsDir() || !validID(entry.Name()) {
			continue
		}
		j, err := a.job(entry.Name())
		if err != nil {
			unreadable = append(unreadable, entry.Name())
			continue
		}
		if cwd := str(j.Receipt, "cwd"); cwd != "" {
			if key := sweepResolved(cwd); key != "" {
				if _, taken := owners[key]; !taken {
					owners[key] = j
				}
			}
		}
	}
	return owners, unreadable, nil
}

// removeCheckout removes a checkout worktreePlan cleared and returns the keep
// reason when it stays. An orphan is removed under OrphanHold and judged once
// more there: a spawn of its job may have started, or a pane moved in, since
// the pass first looked.
func (s *CheckoutSweep) removeCheckout(ctx context.Context, a *App, j Job, orphan CheckoutOrphan, record map[string]any, end paneSweepEnd, clone, branch, dir string, owned bool) string {
	ts := &TaskPaneSweeper{WorkRoot: s.WorkRoot}
	if orphan.Job != "" {
		if s.OrphanHold != nil {
			release, err := s.OrphanHold(ctx, clone)
			if err != nil {
				return sweepCheckoutCloneBusy
			}
			defer release()
		}
		now := s.OrphanOwner(dir)
		if now.Job != orphan.Job || now.InFlight {
			return sweepCheckoutJobLive
		}
		if reason := s.orphanPaneKeep(ctx, a, now, record, dir); reason != "" {
			return reason
		}
		ts.orphanPatches = s.OrphanPatches
	}
	return ts.removeWorktree(ctx, a, j, end, clone, branch, dir, filepath.Base(filepath.Clean(dir)), owned)
}

// jobKeep names why the checkout's recorded job or its pane still uses it: a
// record that has not ended or been reaped, or a terminal record whose pane
// herdr still lists. An unreadable herdr list keeps every candidate that could
// have a pane, so nothing is removed blind. "" when neither does.
func (s *CheckoutSweep) jobKeep(j Job, agentsFor func() ([]Agent, bool)) string {
	if j.Reaped {
		return ""
	}
	if !j.Terminal {
		return sweepCheckoutJobLive
	}
	if j.Pane == "" {
		return ""
	}
	agents, ok := agentsFor()
	if !ok {
		return sweepCheckoutAgentsUnknown
	}
	for _, agent := range agents {
		if agent.Pane == j.Pane {
			return sweepCheckoutPaneOpen
		}
	}
	return ""
}

// orphanEnded proves an orphan's job ended, since no local record can: no spawn
// of it is in flight, and hk holds its record in a final state — or holds none
// while every spawn of it failed before a pane. It returns the hk record, nil
// when hk holds none, and the keep reason when the end is not proven.
func (s *CheckoutSweep) orphanEnded(ctx context.Context, orphan CheckoutOrphan) (map[string]any, string) {
	if orphan.InFlight {
		return nil, sweepCheckoutJobLive
	}
	if s.OrphanRecords == nil {
		return nil, sweepCheckoutJobUnconfirmed
	}
	record, err := s.OrphanRecords.Get(ctx, orphan.Job)
	if errors.Is(err, errJobNotFound) {
		if !orphan.NeverLaunched {
			return nil, sweepCheckoutJobUnconfirmed
		}
		return nil, ""
	}
	if err != nil || record == nil {
		return nil, sweepCheckoutJobUnconfirmed
	}
	if id := str(record, "job_id"); id != "" && id != orphan.Job {
		return nil, sweepCheckoutJobUnconfirmed
	}
	switch str(record, "state") {
	case "completed", "lost", "failed", "refused":
		return record, ""
	}
	return nil, sweepCheckoutJobLive
}

// orphanPaneKeep keeps an orphan while herdr lists a pane that is its job's —
// by the pane id or label its spawns or its hk record name — or a pane working
// inside the directory. It reads both lists afresh: a plain shell is only in
// the pane list. A list that cannot be read keeps the checkout.
func (s *CheckoutSweep) orphanPaneKeep(ctx context.Context, a *App, orphan CheckoutOrphan, record map[string]any, dir string) string {
	panes, err := a.panes(ctx)
	if err != nil {
		return sweepCheckoutAgentsUnknown
	}
	agents, err := a.agents(ctx)
	if err != nil {
		return sweepCheckoutAgentsUnknown
	}
	named := func(value string, wanted ...string) bool {
		return value != "" && slices.Contains(wanted, value)
	}
	uses := func(pane string, names []string, cwds ...string) bool {
		if named(pane, orphan.Pane, str(record, "pane_id")) {
			return true
		}
		for _, name := range names {
			if named(name, orphan.Label, str(record, "label")) {
				return true
			}
		}
		return slices.ContainsFunc(cwds, func(cwd string) bool {
			return cwd != "" && (sweepSamePath(cwd, dir) || sweepWithin(dir, cwd))
		})
	}
	for _, pane := range panes {
		if uses(pane.Pane, []string{pane.Label, pane.Name}, pane.CWD, pane.ForegroundCWD) {
			return sweepCheckoutPaneOpen
		}
	}
	for _, agent := range agents {
		if uses(agent.Pane, []string{agent.Name}, agent.CWD, agent.ForegroundCWD) {
			return sweepCheckoutPaneOpen
		}
	}
	return ""
}
