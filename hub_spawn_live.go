package panewire

import (
	"context"
	"net/http"
	"slices"
	"strings"
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
	tasks, reported, err := h.liveSpawnSnapshot(ctx)
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
// canon. An operation that has no recorded account yet (accepted or
// dispatching, before its first success) cannot be attributed and is not
// counted: the gate covers bursts spaced by placement latency, not
// same-instant races.
func (h *HubServer) liveProfilesByAccount(ctx context.Context, exceptRequest string) (map[string]map[string]int, error) {
	if h.inbox == nil {
		return map[string]map[string]int{}, nil
	}
	tasks, err := h.liveSpawnTasks(ctx)
	if err != nil {
		return nil, err
	}
	counts := map[string]map[string]int{}
	for _, task := range tasks {
		if task.Request.RequestID == exceptRequest {
			continue
		}
		if profile := livePlacedProfile(task); profile != "" && task.Outcome.AccountKey != "" {
			if counts[task.Outcome.AccountKey] == nil {
				counts[task.Outcome.AccountKey] = map[string]int{}
			}
			counts[task.Outcome.AccountKey][profile]++
		}
	}
	return counts, nil
}

// liveAccountJobs returns running jobs by account key, leaving out the hub
// request named by its second argument (hk 5113).
func (h *HubServer) liveAccountJobs(ctx context.Context, exceptRequest string) (map[string]string, error) {
	if h.inbox == nil {
		return map[string]string{}, nil
	}
	tasks, err := h.liveSpawnTasks(ctx)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	jobs := map[string]string{}
	for _, task := range tasks {
		if task.Request.RequestID == exceptRequest {
			continue
		}
		if task.Status == "failed" || task.Status == "given_up" || task.Status == "cancelled" {
			continue
		}
		jobID := task.Job
		if jobID == "" {
			jobID = task.Request.RequestID
		}
		if rec := h.jobs[task.Job]; rec != nil && rec.Completed {
			continue
		}
		if task.Outcome.AccountKey != "" {
			jobs[task.Outcome.AccountKey] = jobID
		}
	}
	return jobs, nil
}

// liveSpawnTasks lists the operations that are live by the rule above: every
// accepted or dispatching one, and each succeeded one that a node still
// reports active or that started within spawnLiveSettle.
func (h *HubServer) liveSpawnTasks(ctx context.Context) ([]hubSpawnTask, error) {
	tasks, _, err := h.liveSpawnSnapshot(ctx)
	return tasks, err
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
func (s *laneInboxStore) liveSpawns(parent context.Context, activeJobs []string, settled, now time.Time) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	query := `SELECT ` + spawnTaskColumns + ` FROM spawn_operations WHERE (status IN ('accepted','bound','dispatching','bound_grant','dispatching_grant') AND NOT (status='accepted' AND node_id IS NULL AND park_reason<>'' AND next_check_ms>?)) OR status='succeeded' AND (created_ms>=? OR last_run_ms>=?`
	args := []any{now.UnixMilli(), settled.UnixMilli(), settled.UnixMilli()}
	if len(activeJobs) > 0 {
		query += ` OR job IN (` + strings.TrimSuffix(strings.Repeat("?,", len(activeJobs)), ",") + `)`
		for _, id := range activeJobs {
			args = append(args, id)
		}
	}
	rows, err := s.db.QueryContext(ctx, s.q(query+`)`), args...)
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
