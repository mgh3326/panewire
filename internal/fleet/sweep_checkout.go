package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
)

// CheckoutSweepLiveKeep reports whether a checkout sweep keep reason means a
// live user still needs the worktree: the job or pane it was prepared for is
// still running, so it leaves on its own when that work ends. The node's
// worktree flow (hk 5117) never counts such a row as made: live inventory is
// not a stuck remover.
func CheckoutSweepLiveKeep(reason string) bool {
	return reason == sweepCheckoutJobLive || reason == sweepCheckoutPaneOpen
}

// CheckoutSweep holds what one pass needs. OrphanOwner names the job a
// *-checkout directory belonged to when no local job record claims it — the
// node's spawn journal on the hub path — and returns "" when nothing
// attributes it. Info receives one line per kept or removed checkout.
type CheckoutSweep struct {
	WorkRoot    string
	DryRun      bool
	OrphanOwner func(dir string) string
	Info        func(message string, attrs ...any)
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
		orphan := false
		if !known {
			for _, id := range unreadable {
				if eventsMention(filepath.Join(a.Jobs, id, "events"), dir) {
					row.Reason = sweepCheckoutJobUnreadable
					break
				}
			}
			if row.Reason == "" && s.OrphanOwner != nil {
				if id := s.OrphanOwner(dir); id != "" {
					j, orphan, known = Job{ID: id, Receipt: map[string]any{"cwd": dir}}, true, true
				}
			}
		}
		if row.Reason == "" && !known {
			row.Reason = sweepCheckoutNoJob
		}
		if row.Reason == "" {
			row.Job = j.ID
			if live, reason := s.jobLive(j, orphan, dir, agentsFor); live {
				row.Reason = reason
			} else {
				clone, branch, owned, reason := ts.worktreePlan(ctx, a, j, dir, end)
				switch {
				case reason != "":
					row.Reason = reason
				case s.DryRun:
					row.Outcome, row.Reason = "would_remove", "discard"
				default:
					base := filepath.Base(filepath.Clean(dir))
					if reason := ts.removeWorktree(ctx, a, j, end, clone, branch, dir, base, owned); reason == "" {
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

// jobLive reports whether the checkout's job or a pane still uses it: a
// record that has not ended or been reaped, a terminal record whose pane
// herdr still lists, or — for a job whose record is gone — any pane working
// inside the directory. An unreadable herdr list keeps every candidate that
// could have a pane, so nothing is removed blind.
func (s *CheckoutSweep) jobLive(j Job, orphan bool, dir string, agentsFor func() ([]Agent, bool)) (bool, string) {
	if orphan {
		agents, ok := agentsFor()
		if !ok {
			return true, sweepCheckoutAgentsUnknown
		}
		for _, agent := range agents {
			if agent.CWD != "" && (sweepSamePath(agent.CWD, dir) || sweepWithin(dir, agent.CWD)) {
				return true, sweepCheckoutPaneOpen
			}
		}
		return false, ""
	}
	if j.Reaped {
		return false, ""
	}
	if !j.Terminal {
		return true, sweepCheckoutJobLive
	}
	if j.Pane == "" {
		return false, ""
	}
	agents, ok := agentsFor()
	if !ok {
		return true, sweepCheckoutAgentsUnknown
	}
	for _, agent := range agents {
		if agent.Pane == j.Pane {
			return true, sweepCheckoutPaneOpen
		}
	}
	return false, ""
}
