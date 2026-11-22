package panewire

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Live spawns by placed profile, for the `[quota].pool_max_concurrent` cap
// (docs/config.md). Two records together cover the fleet: the hub's spawn
// operations, for every spawn it placed, and the jobs each node reports active
// in its heartbeat, for the jobs a direct `panewire spawn` started on that
// node. An operation is live while it is accepted or dispatching, and once it
// has succeeded for as long as a node still reports its job active: a job a
// node no longer lists has ended (completed or lost). A hub spawn that has
// become a node job is in both records, so it is counted once, under its
// operation; its node entry is skipped by job id.

// spawnLiveSettle is how long a succeeded operation counts before a node has
// reported its job in a heartbeat. Without it a burst of spawns would each see
// the earlier ones as not yet running.
const spawnLiveSettle = 2 * time.Minute

// liveSpawnProfiles returns the number of live jobs for each placed profile,
// leaving out exceptRequest (the spawn being placed): every live hub spawn
// operation plus each job a node reports active that no live operation owns,
// which covers a direct `panewire spawn` (hk 3399). An operation that has no
// placed profile yet (an auto spawn still being placed) cannot be attributed
// and is not counted; its job, once a node lists it with a profile, is.
func (h *HubServer) liveSpawnProfiles(ctx context.Context, exceptRequest string) (map[string]int, error) {
	return h.liveSpawnProfilesFrom(ctx, h.liveSpawnSnapshot, exceptRequest)
}

func (h *HubServer) liveSpawnProfilesFrom(ctx context.Context, snapshot liveSnapshotFunc, exceptRequest string) (map[string]int, error) {
	tasks, reported, err := snapshot(ctx)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	// attributed names each job an operation already counted, so the node's
	// own entry for the same job is skipped.
	attributed := map[string]bool{}
	for _, task := range tasks {
		if task.Request.RequestID == exceptRequest {
			if task.Job != "" {
				attributed[task.Job] = true
			}
			continue
		}
		profile := livePlacedProfile(task)
		if profile == "" {
			continue
		}
		counts[profile]++
		if task.Job != "" {
			attributed[task.Job] = true
		}
	}
	for job, profile := range reported {
		if profile == "" || attributed[job] {
			continue
		}
		counts[profile]++
	}
	return counts, nil
}

// liveProfilesByAccount counts live hub spawns' placed profiles by account
// key, leaving out exceptRequest. Profiles stay as placed (a seat name where
// the launch went through a seat): selection folds them through the seat
// canon. The account comes from the placed columns while the attempt holds
// its seat, from the terminal outcome after it succeeded (hk 5124).
func (h *HubServer) liveProfilesByAccount(ctx context.Context, exceptRequest string) (map[string]map[string]int, error) {
	return h.liveProfilesByAccountFrom(ctx, h.liveSpawnSnapshot, exceptRequest)
}

func (h *HubServer) liveProfilesByAccountFrom(ctx context.Context, snapshot liveSnapshotFunc, exceptRequest string) (map[string]map[string]int, error) {
	if h.inbox == nil {
		return map[string]map[string]int{}, nil
	}
	tasks, _, err := snapshot(ctx)
	if err != nil {
		return nil, err
	}
	counts := map[string]map[string]int{}
	for _, task := range tasks {
		if task.Request.RequestID == exceptRequest {
			continue
		}
		if profile := livePlacedProfile(task); profile != "" {
			if accountKey := placedAccountKeyOf(task); accountKey != "" {
				if counts[accountKey] == nil {
					counts[accountKey] = map[string]int{}
				}
				counts[accountKey][profile]++
			}
		}
	}
	return counts, nil
}

// liveAccountJobs returns running jobs by account key, leaving out the hub
// request named by its second argument (hk 5113, hk 5124): the union of the
// operations holding a seat in the placed columns and the succeeded
// operations whose job is still live. A failed lookup refuses fail-closed:
// the callers drop the seat with account_busy_unknown.
func (h *HubServer) liveAccountJobs(ctx context.Context, exceptRequest string) (map[string]string, error) {
	return h.liveAccountJobsFrom(ctx, h.liveSpawnSnapshot, exceptRequest)
}

func (h *HubServer) liveAccountJobsFrom(ctx context.Context, snapshot liveSnapshotFunc, exceptRequest string) (map[string]string, error) {
	if h.inbox == nil {
		return map[string]string{}, nil
	}
	tasks, _, err := snapshot(ctx)
	h.noteAccountJobsLookup(err)
	if err != nil {
		return nil, err
	}
	type liveCandidate struct {
		accountKey string
		jobID      string
		jobName    string
	}
	var candidates []liveCandidate
	for _, task := range tasks {
		if task.Request.RequestID == exceptRequest {
			continue
		}
		if task.Status == "failed" || task.Status == "given_up" || task.Status == "cancelled" {
			continue
		}
		if accountKey := placedAccountKeyOf(task); accountKey != "" {
			jobID := task.Job
			if jobID == "" {
				jobID = task.Request.RequestID
			}
			candidates = append(candidates, liveCandidate{
				accountKey: accountKey,
				jobID:      jobID,
				jobName:    task.Job,
			})
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	jobs := map[string]string{}
	for _, c := range candidates {
		if c.jobName != "" {
			if rec := h.jobs[c.jobName]; rec != nil && rec.Completed {
				continue
			}
		}
		jobs[c.accountKey] = c.jobID
	}
	return jobs, nil
}

// placedAccountKeyOf is the account an operation occupies: the placed columns
// while the attempt holds its seat, the terminal outcome after the attempt
// succeeded and cleared them (hk 5124).
func placedAccountKeyOf(task hubSpawnTask) string {
	if task.Status == "succeeded" {
		return task.Outcome.AccountKey
	}
	return task.PlacedAccountKey
}

// noteAccountJobsLookup records the occupancy lookup outcome: one warn log
// per failure streak, silent while the failure persists, reset on success.
func (h *HubServer) noteAccountJobsLookup(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err == nil {
		h.accountJobsLookupFails = 0
		return
	}
	h.accountJobsLookupFails++
	if h.accountJobsLookupFails == 1 {
		h.logger.Warn("hub spawn account occupancy lookup failed; refusing seats fail-closed", "error", err)
	}
}

// liveSpawnTasks lists the operations that are live by the rule above: every
// accepted or dispatching one, and each succeeded one that a node still
// reports active or that started within spawnLiveSettle.
func (h *HubServer) liveSpawnTasks(ctx context.Context) ([]hubSpawnTask, error) {
	tasks, _, err := h.liveSpawnSnapshot(ctx)
	return tasks, err
}

// liveSnapshotFunc is one read of the fleet's live spawns (liveSpawnSnapshot,
// or a memo of it).
type liveSnapshotFunc func(context.Context) ([]hubSpawnTask, map[string]string, error)

// memoLiveSnapshot reads liveSpawnSnapshot once and serves every later call
// the same result: the three live-spawn readers of one fleet.App selection
// (pool cap, account launches, account occupancy) run once per candidate host
// and each asked the store for the same ~10k-row set (hk 6082). A failed read is
// not kept, so the next call asks again and still fails closed. The result is
// shared and read-only; the App is built per selection, so the memo ends with
// it. refuseBusyCommandcodeSeat does not use it: it must see what changed while
// it waited for the account lock.
func (h *HubServer) memoLiveSnapshot() liveSnapshotFunc {
	var mu sync.Mutex
	var tasks []hubSpawnTask
	var profiles map[string]string
	var have bool
	return func(ctx context.Context) ([]hubSpawnTask, map[string]string, error) {
		mu.Lock()
		defer mu.Unlock()
		if !have {
			t, p, err := h.liveSpawnSnapshot(ctx)
			if err != nil {
				return nil, nil, err
			}
			tasks, profiles, have = t, p, true
		}
		return tasks, profiles, nil
	}
}

// liveSpawnSnapshot is one read of the fleet's live spawns: the operations
// that count, and the profile of every job a node reports active (empty when
// the node's job.spawned receipt named none). Both come from the same presence
// snapshot, so a job cannot fall between the operation query and the map.
func (h *HubServer) liveSpawnSnapshot(ctx context.Context) ([]hubSpawnTask, map[string]string, error) {
	active, profiles := h.activeJobs()
	if h.inbox == nil {
		return nil, profiles, nil
	}
	tasks, err := h.inbox.store.liveSpawns(ctx, active, h.now().Add(-spawnLiveSettle), h.now())
	if err != nil {
		return nil, nil, err
	}
	return tasks, profiles, nil
}

// activeJobs is the jobs the hub's nodes report active, as the ids the
// operation query filters on and the placed profile each was spawned with. A
// job two nodes list counts once, under the node the hub holds as its owner,
// else under the first machine in id order.
func (h *HubServer) activeJobs() ([]string, map[string]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	machines := make([]string, 0, len(h.nodes))
	for machine := range h.nodes {
		machines = append(machines, machine)
	}
	slices.Sort(machines)
	profiles := map[string]string{}
	for _, machine := range machines {
		record := h.nodes[machine]
		if record == nil {
			continue
		}
		for id, job := range record.activeJobs {
			_, seen := profiles[id]
			if !seen || h.jobs[id] != nil && h.jobs[id].Node == machine {
				profiles[id] = job.Profile
			}
		}
	}
	ids := make([]string, 0, len(profiles))
	for id := range profiles {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids, profiles
}

// livePlacedProfile is the profile an operation was placed on: the chosen
// profile once it ran, the one bound to its node request before or during dispatch,
// or the one the submitter named.
func livePlacedProfile(task hubSpawnTask) string {
	if task.Status == "succeeded" && task.Outcome.Profile != "" {
		return task.Outcome.Profile
	}
	if task.Node != nil {
		if profile := hubSpawnArg(task.Node.Args, "-m"); profile != "" && profile != "auto" {
			return profile
		}
	}
	// An accepted retry still carries the prior attempt's outcome for retry
	// policy, but that profile is not the new attempt's placement.
	if spawn, err := fleet.ValidateSubmission(task.Request); err == nil && spawn.Profile != "auto" {
		return spawn.Profile
	}
	return ""
}

// liveSpawns lists the operations that count toward a pool cap: accepted
// (except one parked, unbound and not yet due: it holds no slot; a due
// one may be placing and counts, hk 4661),
// bound and dispatching ones, and succeeded ones whose job is in activeJobs or that
// started after settled.
//
// It runs one statement per index instead of one OR over the table (hk 6082):
// the live statuses use spawn_operations_queue, a succeeded row created after
// settled uses spawn_operations_succeeded, one that last ran after settled uses
// spawn_operations_succeeded_run, and one named by activeJobs uses
// spawn_operations_job. The OR read every succeeded row's 12 KB payload. A row
// two parts match is kept once, and rows only move toward succeeded, so a part
// that runs later can add a row an earlier one missed only by its own test.
// The result is ordered by creation, then request id.
func (s *laneInboxStore) liveSpawns(parent context.Context, activeJobs []string, settled, now time.Time) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	const from = `SELECT ` + spawnTaskColumns + ` FROM spawn_operations WHERE `
	parts := []struct {
		where string
		args  []any
	}{
		{`status IN ('accepted','bound','dispatching','bound_grant','dispatching_grant') AND NOT (status='accepted' AND node_id IS NULL AND park_reason<>'' AND next_check_ms>?)`, []any{now.UnixMilli()}},
		{`status='succeeded' AND created_ms>=?`, []any{settled.UnixMilli()}},
		{`status='succeeded' AND last_run_ms>=?`, []any{settled.UnixMilli()}},
	}
	if len(activeJobs) > 0 {
		args := make([]any, len(activeJobs))
		for i, id := range activeJobs {
			args[i] = id
		}
		parts = append(parts, struct {
			where string
			args  []any
		}{`status='succeeded' AND job IN (` + strings.TrimSuffix(strings.Repeat("?,", len(activeJobs)), ",") + `)`, args})
	}
	var tasks []hubSpawnTask
	seen := map[string]bool{}
	for _, part := range parts {
		got, err := s.queryLiveSpawns(ctx, from+part.where, part.args)
		if err != nil {
			return nil, err
		}
		for _, task := range got {
			if !seen[task.Request.RequestID] {
				seen[task.Request.RequestID] = true
				tasks = append(tasks, task)
			}
		}
	}
	slices.SortFunc(tasks, func(a, b hubSpawnTask) int {
		if c := a.Created.Compare(b.Created); c != 0 {
			return c
		}
		return strings.Compare(a.Request.RequestID, b.Request.RequestID)
	})
	return tasks, nil
}

func (s *laneInboxStore) queryLiveSpawns(ctx context.Context, query string, args []any) ([]hubSpawnTask, error) {
	rows, err := s.db.QueryContext(ctx, s.q(query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []hubSpawnTask
	for rows.Next() {
		task, err := scanSpawnTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (h *HubServer) handleSpawnLive(w http.ResponseWriter, r *http.Request) {
	if h.authenticate(r).kind != hubCallerOperator {
		hubUnauthorized(w)
		return
	}
	profiles, err := h.liveSpawnProfiles(r.Context(), r.URL.Query().Get("except"))
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "store_unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"profiles": profiles})
}
