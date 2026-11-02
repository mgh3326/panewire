package panewire

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// Every way runtug ends a job must take it off the active list. The mergewake
// review runs that inflated mac-dev's count wrote claim then quota release,
// with no spawn and no completion; a lost or reaped job has no pane left.
func TestActiveJobScanEndsOnEveryRuntugTerminalEvent(t *testing.T) {
	root := t.TempDir()
	at := time.Now().UTC().Add(-time.Minute)
	writeR20T6Job(t, root, "job-running", "w1:p1", at)
	writeR20T6Job(t, root, "job-review-released", "", at)
	writeJobEvent(t, root, "job-review-released", 2, "quota_pool.release", map[string]any{"forced": false}, at)
	for i, kind := range []string{"job.completed", "quota_pool.release", "quota_pool.release.force", "job.lost", "job.reaped"} {
		job := "job-ended-" + string(rune('a'+i))
		writeR20T6Job(t, root, job, "w1:p9", at)
		writeJobEvent(t, root, job, 3, kind, map[string]any{"pane_id": "w1:p9"}, at)
	}
	if got := jobIDsR20T6(scanHubActiveJobs(root)); !slices.Equal(got, []string{"job-running"}) {
		t.Fatalf("active=%q, want only the running job", got)
	}
}

func idleTestClient(t *testing.T, root string, alive []string, states *[]HerdrAgentState, statesErr *error) (*HubClient, *time.Time) {
	t.Helper()
	client, err := NewHubClient(HubClientConfig{
		URL: "ws://fixture.invalid", MachineID: "node-a", Token: r6NodeAToken, JobsInboxRoot: root, AllowInsecureForTests: true,
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, nil },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, errors.New("fixture") },
	})
	if err != nil {
		t.Fatal(err)
	}
	hook, _ := aliveR20T6Hook(alive...)
	client.SetPanesAlive(hook)
	client.SetSessionSnapshot(func(context.Context) ([]HerdrAgentState, error) { return *states, *statesErr })
	clock := time.Now().UTC()
	client.now = func() time.Time { return clock }
	return client, &clock
}

func heartbeatJobIDs(t *testing.T, client *HubClient) []string {
	t.Helper()
	heartbeat, ok := decodeHubHeartbeatPayload(client.heartbeatEvent(t.Context()).Payload)
	if !ok {
		t.Fatal("heartbeat did not decode")
	}
	return jobIDsR20T6(heartbeat.ActiveJobs)
}

// A closed pane leaves the heartbeat's active list at the next beat.
func TestHeartbeatDropsJobWhosePaneClosed(t *testing.T) {
	root := t.TempDir()
	writeR20T6Job(t, root, "job-open", "w1:p1", time.Now().UTC())
	writeR20T6Job(t, root, "job-closed", "w1:p2", time.Now().UTC())
	states, statesErr := []HerdrAgentState{{PaneID: "w1:p1", Status: "working"}}, error(nil)
	client, _ := idleTestClient(t, root, []string{"w1:p1"}, &states, &statesErr)
	if got := heartbeatJobIDs(t, client); !slices.Equal(got, []string{"job-open"}) {
		t.Fatalf("active=%q", got)
	}
}

// A job whose pane sits idle or done for 30 minutes leaves the list; work on
// the pane restarts the clock, and any other status never starts it.
func TestHeartbeatDropsJobIdleThirtyMinutes(t *testing.T) {
	root := t.TempDir()
	for _, job := range []string{"job-idle", "job-done", "job-blocked", "job-working"} {
		writeR20T6Job(t, root, job, "pane-"+job, time.Now().UTC())
	}
	states := []HerdrAgentState{
		{PaneID: "pane-job-idle", Status: "idle"}, {PaneID: "pane-job-done", Status: "done"},
		{PaneID: "pane-job-blocked", Status: "blocked"}, {PaneID: "pane-job-working", Status: "working"},
	}
	var statesErr error
	client, clock := idleTestClient(t, root, []string{"pane-job-idle", "pane-job-done", "pane-job-blocked", "pane-job-working"}, &states, &statesErr)
	all := []string{"job-blocked", "job-done", "job-idle", "job-working"}
	if got := heartbeatJobIDs(t, client); !slices.Equal(got, all) {
		t.Fatalf("first beat active=%q", got)
	}
	*clock = clock.Add(hubJobIdleAfter - time.Second)
	if got := heartbeatJobIDs(t, client); !slices.Equal(got, all) {
		t.Fatalf("just under the idle limit active=%q", got)
	}
	*clock = clock.Add(time.Second)
	if got := heartbeatJobIDs(t, client); !slices.Equal(got, []string{"job-blocked", "job-working"}) {
		t.Fatalf("at the idle limit active=%q", got)
	}
	// The idle pane picks up work, then goes idle again: a fresh clock.
	states[0].Status = "working"
	*clock = clock.Add(time.Minute)
	_ = heartbeatJobIDs(t, client)
	states[0].Status = "idle"
	*clock = clock.Add(time.Minute)
	if got := heartbeatJobIDs(t, client); !slices.Contains(got, "job-idle") {
		t.Fatalf("restarted idle clock dropped the job at once: active=%q", got)
	}
	// An unreadable herdr snapshot drops nothing, and it resets every idle
	// clock: whether a pane worked meanwhile is unknown, so the done pane
	// gets a fresh 30 minutes once herdr answers again.
	statesErr = errors.New("herdr down")
	if got := heartbeatJobIDs(t, client); !slices.Equal(got, all) {
		t.Fatalf("snapshot failure filtered jobs: active=%q", got)
	}
	statesErr = nil
	*clock = clock.Add(time.Minute)
	if got := heartbeatJobIDs(t, client); !slices.Equal(got, all) {
		t.Fatalf("idle clock survived a snapshot failure: active=%q", got)
	}
}

// The idle clock belongs to a job on a pane, not to the pane: a job claimed
// on a pane that sat idle under an earlier job is reported from its first
// beat, and starts its own 30 minutes.
func TestHeartbeatIdleClockRestartsForNewJobOnSamePane(t *testing.T) {
	root := t.TempDir()
	writeR20T6Job(t, root, "job-old", "w1:p1", time.Now().UTC())
	states, statesErr := []HerdrAgentState{{PaneID: "w1:p1", Status: "idle"}}, error(nil)
	client, clock := idleTestClient(t, root, []string{"w1:p1"}, &states, &statesErr)
	_ = heartbeatJobIDs(t, client)
	*clock = clock.Add(hubJobIdleAfter + time.Minute)
	if got := heartbeatJobIDs(t, client); len(got) != 0 {
		t.Fatalf("long-idle job still active: %q", got)
	}
	writeJobEvent(t, root, "job-old", 3, "job.completed", map[string]any{}, time.Now().UTC())
	writeR20T6Job(t, root, "job-new", "w1:p1", time.Now().UTC())
	if got := heartbeatJobIDs(t, client); !slices.Equal(got, []string{"job-new"}) {
		t.Fatalf("new job on a long-idle pane was dropped at once: active=%q", got)
	}
}
