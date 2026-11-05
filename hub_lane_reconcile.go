package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const laneReconcileInterval = 5 * time.Minute
const laneReconcileBackoff = 30 * time.Minute

type laneRetry struct {
	failures int
	next     time.Time
}

type laneReconciler struct {
	machine  string
	retries  map[string]laneRetry
	list     func(context.Context) (map[string]hubLaneProjection, error)
	register func(context.Context, HubActiveJob) error
	pending  func(HubActiveJob) bool
}

func (r *laneReconciler) pass(ctx context.Context, jobs []HubActiveJob, now time.Time) {
	lanes, err := r.list(ctx)
	if err != nil {
		lanes = nil
	}
	if r.retries == nil {
		r.retries = make(map[string]laneRetry)
	}
	live := make(map[string]bool, len(jobs))
	for _, job := range jobs {
		live[job.JobID] = true
		if job.Pane == "" || (lanes == nil && (r.pending == nil || !r.pending(job))) {
			continue
		}
		lane := job.AgentLabel
		if job.Role == "builder" {
			lane = job.OwnerLane
		}
		if route, ok := lanes[lane]; ok && route.Machine == r.machine && route.Pane == job.Pane {
			delete(r.retries, job.JobID)
			continue
		}
		retry := r.retries[job.JobID]
		if now.Before(retry.next) {
			continue
		}
		if err := r.register(ctx, job); err != nil {
			retry.failures++
			if retry.failures >= 3 {
				retry.next = now.Add(laneReconcileBackoff)
			}
			r.retries[job.JobID] = retry
		} else {
			delete(r.retries, job.JobID)
		}
	}
	for id := range r.retries {
		if !live[id] {
			delete(r.retries, id)
		}
	}
}

func (client *HubClient) runLaneReconcile(ctx context.Context) {
	if client.jobsInboxRoot == "" {
		return
	}
	list := client.nodeLanes
	if client.laneReconcileList != nil {
		list = client.laneReconcileList
	}
	r := laneReconciler{machine: client.machineID, list: list, pending: func(job HubActiveJob) bool {
		return pendingLaneRegistration(filepath.Join(client.jobsInboxRoot, "jobs", job.JobID, "events"))
	}, register: func(ctx context.Context, job HubActiveJob) error {
		if client.laneReconcileRegister != nil {
			return client.laneReconcileRegister(ctx, job)
		}
		config, _, err := client.resolveHubSpawnConfig()
		if err != nil {
			return err
		}
		err = fleet.ReconcileNodeLane(ctx, filepath.Join(client.jobsInboxRoot, "jobs"), config.HERDRSession, job.JobID, client.machineID)
		if err != nil {
			slog.Warn("job lane registration failed", "job", job.JobID)
		}
		return err
	}}
	ticks := client.laneReconcileTicks
	if ticks == nil {
		ticker := time.NewTicker(laneReconcileInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			jobs := scanHubActiveJobsForLimit(ctx, client.jobsInboxRoot, client.panesAliveHook(), 0)
			if client.laneReconcileJobs != nil {
				jobs = client.laneReconcileJobs(ctx)
			}
			r.pass(ctx, jobs, client.nowUTC())
		}
	}
}

func (client *HubClient) nodeLanes(ctx context.Context) (map[string]hubLaneProjection, error) {
	for _, endpoint := range client.endpoints {
		u, err := url.Parse(endpoint)
		if err != nil {
			continue
		}
		if u.Scheme == "wss" {
			u.Scheme = "https"
		} else {
			u.Scheme = "http"
		}
		u.Path = "/v1/lanes"
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			continue
		}
		request.Header = client.dialHeaders(endpoint)
		response, err := (&http.Client{Timeout: 15 * time.Second}).Do(request)
		if err != nil {
			continue
		}
		var body struct {
			Lanes []hubLaneProjection `json:"lanes"`
		}
		err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || err != nil || body.Lanes == nil {
			continue
		}
		lanes := make(map[string]hubLaneProjection, len(body.Lanes))
		for _, lane := range body.Lanes {
			lanes[lane.Lane] = lane
		}
		return lanes, nil
	}
	return nil, fmt.Errorf("hub lanes unavailable")
}

// pendingLaneRegistration finds an unresolved landing failure if the hub's
// lane listing is unavailable. A later success retires the receipt flag.
func pendingLaneRegistration(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	pending := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil || len(data) > 16<<10 {
			continue
		}
		var event struct {
			Kind    string `json:"kind"`
			Payload struct {
				Unregistered bool `json:"lane_unregistered"`
			} `json:"payload"`
		}
		if json.Unmarshal(data, &event) != nil {
			continue
		}
		switch event.Kind {
		case "job.spawned":
			pending = event.Payload.Unregistered
		case "job.lane_registered":
			pending = false
		}
	}
	return pending
}
