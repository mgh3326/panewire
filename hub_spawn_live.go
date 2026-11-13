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
// (docs/config.md). The hub places every hub spawn, so its operations are the
// fleet-wide record. An operation is live while it is accepted or dispatching,
// and once it has succeeded for as long as a node still reports its job
// active: a job a node no longer lists has ended (completed or lost).

// spawnLiveSettle is how long a succeeded operation counts before a node has
// reported its job in a heartbeat. Without it a burst of spawns would each see
// the earlier ones as not yet running.
const spawnLiveSettle = 2 * time.Minute

// liveSpawnProfiles returns the number of live hub spawns for each placed
// profile, leaving out exceptRequest (the spawn being placed, which is itself
// an accepted or dispatching operation). An operation that has no placed
// profile yet (an auto spawn still being placed) cannot be attributed and is
// not counted.
func (h *HubServer) liveSpawnProfiles(ctx context.Context, exceptRequest string) (map[string]int, error) {
	if h.inbox == nil {
		return map[string]int{}, nil
	}
	tasks, err := h.liveSpawnTasks(ctx)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, task := range tasks {
		if task.Request.RequestID == exceptRequest {
			continue
		}
		if profile := livePlacedProfile(task); profile != "" {
			counts[profile]++
		}
	}
	return counts, nil
}

// liveSpawnTasks lists the operations that are live by the rule above: every
// accepted or dispatching one, and each succeeded one that a node still
// reports active or that started within spawnLiveSettle.
func (h *HubServer) liveSpawnTasks(ctx context.Context) ([]hubSpawnTask, error) {
	h.mu.Lock()
	var active []string
	for _, record := range h.nodes {
		for id := range record.activeJobs {
			if !slices.Contains(active, id) {
				active = append(active, id)
			}
		}
	}
	h.mu.Unlock()
	slices.Sort(active)
	return h.inbox.store.liveSpawns(ctx, active, h.now().Add(-spawnLiveSettle))
}

// livePlacedProfile is the profile an operation was placed on: the chosen
// profile once it ran, the one bound to its node request while dispatching,
// or the one the submitter named.
func livePlacedProfile(task hubSpawnTask) string {
	if task.Outcome.Profile != "" {
		return task.Outcome.Profile
	}
	if task.Node != nil {
		if profile := hubSpawnArg(task.Node.Args, "-m"); profile != "" && profile != "auto" {
			return profile
		}
	}
	if spawn, err := fleet.ValidateSubmission(task.Request); err == nil && spawn.Profile != "auto" {
		return spawn.Profile
	}
	return ""
}

// liveSpawns lists the operations that count toward a pool cap: accepted and
// dispatching ones, and succeeded ones whose job is in activeJobs or that
// started after settled.
func (s *laneInboxStore) liveSpawns(parent context.Context, activeJobs []string, settled time.Time) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	query := `SELECT ` + spawnTaskColumns + ` FROM spawn_operations WHERE status IN ('accepted','dispatching') OR status='succeeded' AND (created_ms>=? OR last_run_ms>=?`
	args := []any{settled.UnixMilli(), settled.UnixMilli()}
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
