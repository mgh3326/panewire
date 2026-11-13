package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"
)

func duration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		d, e := time.ParseDuration(strings.TrimSuffix(s, "d") + "h")
		return d * 24, e
	}
	return time.ParseDuration(s)
}
func (a *App) hygiene(ctx context.Context, kind string, args []string) error {
	f := flags(kind)
	apply := f.Bool("apply", false, "")
	lane := f.String("lane", "", "")
	job := f.String("job", "", "")
	grace := f.String("grace", "10m", "")
	builders := f.Bool("include-builders", false, "")
	f.BoolVar(builders, "include-captains", false, "")
	worktrees := f.Bool("worktrees", false, "")
	stale := f.String("stale-age", "24h", "")
	_ = f.String("heavy-age", "24h", "")
	_ = f.String("policy", "", "")
	// --expire-job-dirs adds the job dir TTL pass (job_dir_ttl.go): a dry run
	// unless --apply is also given. A plain reap --apply never deletes a dir.
	expire := f.Bool("expire-job-dirs", false, "")
	if e := f.Parse(args); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() != 0 {
		return fail(exitUsage, "unexpected hygiene argument")
	}
	if *expire && kind != "reap" {
		return fail(exitUsage, "--expire-job-dirs belongs to reap")
	}
	if *expire && *lane != "" {
		// The TTL pass has no lane filter; with --lane it would expire
		// every lane's dirs while the reap kept to one.
		return fail(exitUsage, "--expire-job-dirs takes no --lane; limit it with --job")
	}
	age, e := duration(*grace)
	if e != nil || age < 0 {
		return fail(exitUsage, "invalid grace")
	}
	staleAge, e := duration(*stale)
	if e != nil || staleAge < 0 {
		return fail(exitUsage, "invalid stale-age")
	}
	agents, e := a.agents(ctx)
	if e != nil {
		return e
	}
	entries, e := os.ReadDir(a.Jobs)
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	parents, prs := map[string]paneSweepInherited{}, map[string]paneSweepPRRead{}
	for _, entry := range entries {
		if !entry.IsDir() || !validID(entry.Name()) {
			continue
		}
		if *job != "" && !slices.Contains(strings.Split(*job, ","), entry.Name()) {
			continue
		}
		j, e := a.job(entry.Name())
		if e != nil {
			a.warn("skip job %s: %v", entry.Name(), e)
			continue
		}
		if *lane != "" && j.Owner != *lane {
			continue
		}
		if j.Reaped {
			continue
		}
		reason := ""
		if j.Keep {
			reason = "protected"
		} else if (j.Role == "builder" || j.Role == "captain") && !*builders {
			reason = "builder"
		} else if !j.Terminal {
			reason = "live"
		} else if a.Now().Sub(j.At) < age {
			reason = "grace"
		} else if j.Pane == "" {
			reason = "malformed-receipt"
		}
		present := false
		working := false
		for _, ag := range agents {
			if ag.Pane == j.Pane {
				present = true
				working = ag.Status == "working" || ag.Name != j.Label || j.Tab != "" && ag.Tab != j.Tab
			}
		}
		if reason == "" && working {
			reason = "pane-working-or-mismatch"
		}
		if reason == "" && paneSweepAwaitsPR(j) {
			// A completed author waits on its open PR, as in the task pane
			// sweep (docs/pane-sweep.md); a job its owner closed does not.
			reason = a.reapPRKeep(ctx, j, parents, prs)
		}
		if kind == "prune" && reason == "live" && !present && !j.Keep {
			events := j.Events
			if len(events) > 0 && a.Now().Sub(events[len(events)-1].when) >= staleAge {
				reason = "stale-pane-absent"
			}
		}
		eligible := reason == "" || reason == "stale-pane-absent"
		fmt.Fprintf(a.Out, "job=%s pane=%s action=%s reason=%s\n", j.ID, j.Pane, map[bool]string{true: "reap", false: "keep"}[eligible], reason)
		if !eligible || !*apply {
			continue
		}
		// Re-read both durable history and live pane identity immediately before an
		// irreversible close. A revived job always wins over the older terminal.
		latest, e := a.job(j.ID)
		if e != nil {
			return e
		}
		if latest.Keep || latest.Pane != j.Pane || (!latest.Terminal && reason != "stale-pane-absent") ||
			(paneSweepAwaitsPR(latest) && !paneSweepAwaitsPR(j)) {
			continue
		}
		fresh, e := a.agents(ctx)
		if e != nil {
			return e
		}
		present = false
		for _, ag := range fresh {
			if ag.Pane == j.Pane {
				if ag.Status == "working" || ag.Name != j.Label || j.Tab != "" && ag.Tab != j.Tab {
					return fail(exitStateConflict, "pane changed during reap")
				}
				present = true
			}
		}
		if reason == "stale-pane-absent" && present {
			continue
		}
		a.recordJobUsage(ctx, latest, true)
		a.markEndedJobLost(ctx, latest)
		if present {
			if e = a.must(ctx, "herdr", "pane", "close", j.Pane); e != nil {
				return e
			}
		}
		if e = a.releaseQuota(ctx, j); e != nil {
			return e
		}
		if owner, foreign := a.laneOwnedElsewhere(j); foreign {
			a.warn("lane %s retained: it belongs to machine %s and this host is %s; a node cannot remove another machine's lane, so remove it from %s", j.HubLane, j.Machine, owner, j.Machine)
		} else if e = a.removeLane(ctx, j); e != nil {
			a.warn("lane retained: %v", e)
		}
		if e = a.event(ctx, j.ID, "job.reaped", map[string]any{"pane_id": j.Pane, "reason": reason}); e != nil {
			return e
		}
		if *worktrees {
			if e = a.reapWorktree(ctx, j); e != nil {
				a.warn("worktree retained: %v", e)
			}
		}
	}
	if !*expire {
		return nil
	}
	ttl, ok := ConfiguredJobDirTTL(a.Config)
	if !ok {
		ttl = DefaultJobDirTTL
	}
	expiry := &JobDirTTL{TTL: ttl, Apply: *apply, Out: a.Out}
	if *job != "" {
		expiry.Only = strings.Split(*job, ",")
	}
	_, e = expiry.Pass(ctx, a)
	return e
}

// reapPRKeep is the task pane sweep's open-PR keep (paneSweepPRKeep) for a
// completed author that reap would close: the reason its pane stays, or ""
// when no open PR holds it. Its task is the sweep's: the claim's, else the
// parent job's. A task that could not be established (hk unreadable, or a
// named parent that does not resolve to a task yet) keeps the pane; a job that
// names no parent anywhere has no PR to wait on.
func (a *App) reapPRKeep(ctx context.Context, j Job, parents map[string]paneSweepInherited, prs map[string]paneSweepPRRead) string {
	s := &TaskPaneSweeper{PR: a.PRs}
	task := j.Task
	var taskErr error
	if task == "" {
		task, taskErr = s.inheritedTask(ctx, a, j, parents)
	}
	reason, _ := s.paneSweepPRKeep(ctx, a, j, task, taskErr, prs)
	return reason
}

// laneRoute is the App as the job's lane credential sees it: the herdr session
// recorded in the receipt picks the node token and machine id.
func (a *App) laneRoute(j Job) App {
	route := *a
	if session := str(j.Receipt, "herdr_session"); session != "" {
		route.HerdrSession = session
	}
	return route
}

// laneOwnedElsewhere reports that the job's registered lane belongs to a
// machine other than the one the lane credential acts as. The hub answers a
// node token 403 lane_machine_mismatch for such a lane, so reap does not try;
// it returns the caller's machine id for the message.
func (a *App) laneOwnedElsewhere(j Job) (caller string, foreign bool) {
	if j.HubLane == "" || j.Machine == "" {
		return "", false
	}
	route := a.laneRoute(j)
	caller = route.machine()
	return caller, caller != j.Machine
}
func (a *App) removeLane(ctx context.Context, j Job) error {
	if j.HubLane == "" || j.Machine == "" {
		return nil
	}
	args := []string{"lanes", "rm", j.HubLane, "--expect-machine", j.Machine, "--expect-pane", j.Pane}
	route := a.laneRoute(j)
	args = append(args, route.laneHubArgs(j.Role)...)
	return a.must(ctx, "panewire", args...)
}
func (a *App) reapWorktree(ctx context.Context, j Job) error {
	cwd := str(j.Receipt, "cwd")
	if cwd == "" {
		return fail(exitUsage, "missing worktree path")
	}
	if e := noSymlinks(cwd); e != nil {
		return e
	}
	r := a.call(ctx, "git", "-C", cwd, "rev-parse", "--git-dir", "--git-common-dir")
	if e := check(r, "worktree identity"); e != nil {
		return e
	}
	lines := strings.Fields(r.Out)
	if len(lines) != 2 || lines[0] == lines[1] || lines[0] == ".git" {
		return fail(exitUsage, "canonical checkout protected")
	}
	r = a.call(ctx, "git", "-C", cwd, "status", "--porcelain")
	if r.Code != 0 || r.Out != "" {
		return fail(exitUsage, "dirty worktree protected")
	}
	if e := a.must(ctx, "git", "-C", cwd, "merge-base", "--is-ancestor", "HEAD", "refs/remotes/origin/main"); e != nil {
		return fail(exitUsage, "unmerged worktree protected")
	}
	return a.must(ctx, "git", "-C", cwd, "worktree", "remove", cwd)
}

type Tab struct {
	ID    string `json:"tab_id"`
	Label string `json:"label"`
}

func (a *App) tabs(ctx context.Context) ([]Tab, error) {
	r := a.call(ctx, "herdr", "tab", "list")
	if e := check(r, "tab list"); e != nil {
		return nil, e
	}
	var m map[string]any
	if e := json.Unmarshal([]byte(r.Out), &m); e != nil {
		return nil, e
	}
	b, _ := encode(unwrap(m)["tabs"])
	var tabs []Tab
	if string(b) == "null" {
		return nil, fail(exitConfig, "tab list missing tabs")
	}
	e := json.Unmarshal(b, &tabs)
	return tabs, e
}
func (a *App) diagnostics(ctx context.Context, kind string, args []string) error {
	if kind == "hosts" {
		if len(args) > 0 {
			return fail(exitUsage, "hosts accepts no arguments")
		}
		path := a.ConfigPath
		if path == "" {
			path = configPath(a.Err)
		}
		fmt.Fprintf(a.Out, "config=%s\nlocal spawn=%t\n", path, boolean(a.Config.Local, "spawn", true))
		for _, host := range slices.Sorted(maps.Keys(a.Config.Hosts)) {
			h := a.Config.Hosts[host]
			fmt.Fprintf(a.Out, "host=%s via=%s\n", host, str(h, "via"))
		}
		return nil
	}
	if kind == "profiles" {
		if len(args) > 0 {
			return fail(exitUsage, "profiles accepts no arguments")
		}
		for _, name := range slices.Sorted(maps.Keys(a.Policy.Profiles)) {
			fmt.Fprintln(a.Out, name)
		}
		return nil
	}
	if kind == "lanes-missing" {
		if len(args) > 0 {
			return fail(exitUsage, "lanes-missing accepts no arguments")
		}
		return a.lanesMissing(ctx)
	}
	agents, e := a.agents(ctx)
	if e != nil {
		return e
	}
	tabs, e := a.tabs(ctx)
	if e != nil {
		return e
	}
	if kind == "find" {
		f := flags(kind)
		paneOnly := false
		target := ""
		for _, v := range args {
			if v == "--pane-only" {
				paneOnly = true
			} else if strings.HasPrefix(v, "-") || target != "" {
				return fail(exitUsage, "find requires one name or label")
			} else {
				target = v
			}
		}
		_ = f
		if target == "" {
			return fail(exitUsage, "find requires a name or label")
		}
		// Canonical identities (agent names here, agent_name/job/lane on the
		// hub) are name-tier; tab and display labels are the label-tier
		// fallback, exactly as the local lookup always resolved them.
		var candidates []findMatch
		for _, ag := range agents {
			if ag.Name == target {
				candidates = append(candidates, a.localFindMatch(ag, true))
			}
		}
		for _, t := range tabs {
			if t.Label == target {
				for _, ag := range agents {
					if ag.Tab == t.ID {
						candidates = append(candidates, a.localFindMatch(ag, false))
					}
				}
			}
		}
		candidates = append(candidates, a.fleetFind(ctx, target)...)
		matches := mergeFindMatches(candidates)
		if len(matches) == 0 {
			return fail(exitFailure, "no exact name or label match")
		}
		if paneOnly {
			if len(matches) != 1 {
				return fail(exitFailure, "ambiguous pane selection")
			}
			// A local pane keeps the bare id panewire prompt resolves; a
			// remote one is printed machine-qualified so the operator knows
			// which host to run it on.
			if matches[0].Local {
				fmt.Fprintln(a.Out, matches[0].PaneID)
			} else {
				fmt.Fprintf(a.Out, "%s:%s\n", matches[0].Machine, matches[0].PaneID)
			}
			return nil
		}
		for _, m := range matches {
			name, cwd, status := m.Name, m.CWD, m.Status
			if name == "" {
				name = "-"
			}
			if cwd == "" {
				cwd = "-"
			}
			if status == "" {
				status = "-"
			}
			fmt.Fprintf(a.Out, "machine=%s pane_id=%s name=%s cwd=%s status=%s via=%s", m.Machine, m.PaneID, name, cwd, status, findViaString(m.Via))
			if m.Stale {
				fmt.Fprint(a.Out, " stale=true")
			}
			fmt.Fprintln(a.Out)
			if !m.Local {
				continue
			}
			r := a.call(ctx, "herdr", "agent", "read", m.PaneID, "--source", "recent-unwrapped", "--lines", "5")
			if e = check(r, "pane preview"); e != nil {
				return e
			}
			fmt.Fprintln(a.Out, r.Out)
		}
		if len(matches) > 1 {
			return fail(exitFailure, "multiple panes matched")
		}
		return nil
	}
	apply := false
	var labels []string
	for _, arg := range args {
		if arg == "--apply" {
			apply = true
		} else if strings.HasPrefix(arg, "-") {
			return fail(exitUsage, "invalid name-sync flag")
		} else {
			labels = append(labels, arg)
		}
	}
	taken := map[string]bool{}
	for _, ag := range agents {
		taken[ag.Name] = true
	}
	for _, tab := range tabs {
		var members []Agent
		for _, ag := range agents {
			if ag.Tab == tab.ID {
				members = append(members, ag)
			}
		}
		if len(labels) > 0 && !slices.Contains(labels, tab.Label) {
			continue
		}
		if len(members) != 1 || members[0].Name != "" || taken[tab.Label] || len(tab.Label) < 3 {
			continue
		}
		fmt.Fprintf(a.Out, "rename pane=%s name=%s apply=%t\n", members[0].Pane, tab.Label, apply)
		if apply {
			if e = a.must(ctx, "herdr", "agent", "rename", members[0].Pane, tab.Label); e != nil {
				return e
			}
			taken[tab.Label] = true
		}
	}
	return nil
}
func (a *App) lanesMissing(ctx context.Context) error {
	if str(a.Config.Hub, "hub_url") == "" {
		return fail(exitConfig, "lanes audit needs hub config")
	}
	args := append([]string{"lanes", "ls"}, a.laneHubArgs("builder")...)
	r := a.call(ctx, "panewire", args...)
	if e := check(r, "lanes audit"); e != nil {
		return e
	}
	var doc struct {
		Lanes []struct{ Lane, Parent string } `json:"lanes"`
	}
	if e := json.Unmarshal([]byte(r.Out), &doc); e != nil || doc.Lanes == nil {
		return fail(exitConfig, "invalid lanes response")
	}
	lanes := map[string]string{}
	for _, lane := range doc.Lanes {
		lanes[lane.Lane] = lane.Parent
	}
	entries, e := os.ReadDir(a.Jobs)
	if e != nil {
		return e
	}
	count, missing := 0, 0
	for _, entry := range entries {
		// A job dir TTL tombstone (.expiring-*) is not a job.
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), jobDirTombstone) {
			continue
		}
		j, e := a.job(entry.Name())
		if e != nil {
			// As in the artifact sweep: a dir with no events/ is skipped, and
			// unreadable events are named; the pass goes on (hk 3685).
			if !errors.Is(e, os.ErrNotExist) {
				a.warn("skip job %s: %v", entry.Name(), e)
			}
			continue
		}
		if j.Terminal || j.Reaped {
			continue
		}
		count++
		_, exists := lanes[j.Owner]
		_, parentExists := lanes[j.Parent]
		if !exists || j.Parent != "" && !parentExists {
			missing++
			fmt.Fprintf(a.Out, "missing job=%s lane=%s parent=%s\n", j.ID, j.Owner, j.Parent)
		}
	}
	fmt.Fprintf(a.Out, "lanes-missing: audited %d open job(s), %d missing\n", count, missing)
	return nil
}

// markEndedJobLost writes `lost` to the hk record of a job that ended without a
// completion: its newest terminal event is job.revoked or job.lost, so no
// job.completed or job.joined follows its last claim. Without it the record
// stays claimed or spawned and holds the task's role for every later spawn. A
// record that is already completed or lost, or that hk does not hold, is left
// alone. A failed read or write is a warning plus job.record_failed; reap
// goes on.
func (a *App) markEndedJobLost(ctx context.Context, j Job) {
	if j.Ended != "job.revoked" && j.Ended != "job.lost" {
		return
	}
	readFailed := func(e error) {
		a.warn("job record reap read failed: %v", e)
		_ = a.event(ctx, j.ID, "job.record_failed", map[string]any{"op": "reap"})
	}
	store, e := a.jobStore()
	if e != nil {
		readFailed(e)
		return
	}
	rec, e := store.Get(ctx, j.ID)
	if errors.Is(e, errJobNotFound) {
		return
	}
	if e != nil {
		readFailed(e)
		return
	}
	if state := str(rec, "state"); state != "claimed" && state != "spawned" {
		return
	}
	if e = a.MarkJobLost(ctx, j.ID); e != nil {
		a.jobRecordFailed(ctx, j.ID, "reap", e)
	}
}
