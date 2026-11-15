package fleet

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/handoffkeep/client"
)

// hk 4654: a --parent-job successor moves the lane row its predecessor held
// onto its own pane, so the lane a task's findings are addressed to follows
// the takeover instead of staying on the old pane.

// laneAddCalls records every `lanes add` the spawn makes and answers each one
// from answer, so a test can distinguish the takeover write from the fallback.
func recordLaneAdds(fx *rebindFixture, answer func(args []string) Result) *[]Command {
	var calls []Command
	original := fx.f.Do
	fx.f.Do = func(c Command) Result {
		if c.Name != "panewire" || !strings.HasPrefix(strings.Join(c.Args, " "), "lanes add") {
			return original(c)
		}
		calls = append(calls, c)
		return answer(c.Args)
	}
	return &calls
}

func jobEvent(t *testing.T, fx *rebindFixture, kind string) map[string]any {
	t.Helper()
	events, err := fx.a.events(fx.s.Job)
	if err != nil {
		t.Fatal(err)
	}
	var found map[string]any
	for _, ev := range events {
		if ev.Kind == kind {
			found = ev.Payload
		}
	}
	return found
}

func TestTakeoverMovesTheTaskLaneToTheSuccessor(t *testing.T) {
	fx := newRebindFixture(t, "spawned", rebindParent)
	fx.s.Inherited, fx.s.Task = rebindParent, "42"
	fx.a.Config.Hub = map[string]any{"hub_url": "https://hub.example.invalid"}
	fx.a.Sleep = func(context.Context, time.Duration) error { return nil }
	calls := recordLaneAdds(fx, func([]string) Result {
		return Result{Out: `{"lane":"mw-recheck-coverage","machine":"test-host","pane":"w:p1","parent":"owner"}`}
	})
	if err := fx.a.event(context.Background(), fx.s.Job, "job.spawned", map[string]any{}); err != nil {
		t.Fatal(err)
	}

	fx.a.finishTaskTakeover(context.Background(), &fx.s,
		client.Task{Lane: "mw-recheck-coverage", Refs: client.TaskRefs{JobID: rebindParent}},
		"owner", "mw-recheck-coverage", "w:p1", "test-host")

	if len(*calls) != 1 {
		t.Fatalf("lanes add calls=%d, want the takeover write only", len(*calls))
	}
	args := (*calls)[0].Args
	if len(args) < 3 || args[0] != "lanes" || args[1] != "add" || args[2] != "mw-recheck-coverage" {
		t.Fatalf("takeover wrote the wrong lane: %v", args)
	}
	if flagValue(args, "--takeover") != rebindParent {
		t.Fatalf("takeover did not name the predecessor: %v", args)
	}
	if fx.s.laneUnregistered || fx.s.registeredLane == nil || fx.s.registeredLane.HubLane != "mw-recheck-coverage" || fx.s.registeredLane.Machine != "test-host" {
		t.Fatalf("registration not kept: %+v", fx.s)
	}
	j, err := fx.a.job(fx.s.Job)
	if err != nil || j.HubLane != "mw-recheck-coverage" || j.Machine != "test-host" {
		t.Fatalf("job lane=%q machine=%q err=%v", j.HubLane, j.Machine, err)
	}
	registered := jobEvent(t, fx, "job.lane_registered")
	if registered["hub_lane"] != "mw-recheck-coverage" || registered["takeover_job"] != rebindParent {
		t.Fatalf("job.lane_registered=%v", registered)
	}
}

// A lane the hub does not route from the predecessor's pane — a design task's
// lane, for one — is refused; the successor then registers its own label and
// records that the takeover did not happen.
func TestTakeoverFallsBackToTheSuccessorLaneWhenRefused(t *testing.T) {
	fx := newRebindFixture(t, "spawned", rebindParent)
	var output bytes.Buffer
	fx.a.Out = &output
	fx.s.Inherited, fx.s.Task = rebindParent, "42"
	fx.a.Config.Hub = map[string]any{"hub_url": "https://hub.example.invalid"}
	fx.a.Sleep = func(context.Context, time.Duration) error { return nil }
	calls := recordLaneAdds(fx, func(args []string) Result {
		if flagValue(args, "--takeover") != "" {
			return Result{Code: 6, Err: "lanes rejected by hub: lane_machine_mismatch"}
		}
		return Result{Out: `{"lane":"worker-a","machine":"test-host","pane":"w:p1","parent":"owner"}`}
	})
	if err := fx.a.event(context.Background(), fx.s.Job, "job.spawned", map[string]any{}); err != nil {
		t.Fatal(err)
	}

	fx.a.finishTaskTakeover(context.Background(), &fx.s,
		client.Task{Lane: "design-lane", Refs: client.TaskRefs{JobID: rebindParent}},
		"owner", "design-lane", "w:p1", "test-host")

	if len(*calls) != 3 {
		t.Fatalf("lanes add calls=%d, want takeover, its retry and the fallback", len(*calls))
	}
	fallback := (*calls)[2].Args
	if flagValue(fallback, "--takeover") != "" || fallback[2] != "worker-a" {
		t.Fatalf("fallback wrote %v", fallback)
	}
	if fx.s.laneUnregistered || fx.s.registeredLane == nil || fx.s.registeredLane.HubLane != "worker-a" {
		t.Fatalf("fallback registration not kept: %+v", fx.s)
	}
	if failed := jobEvent(t, fx, "job.lane_takeover_failed"); failed["lane"] != "design-lane" || failed["takeover_job"] != rebindParent {
		t.Fatalf("job.lane_takeover_failed=%v", failed)
	}
	if !strings.Contains(output.String(), "lane design-lane not moved: ") || !strings.Contains(output.String(), "lane_machine_mismatch") {
		t.Fatalf("spawn output did not explain the failed lane move: %q", output.String())
	}
	if registered := jobEvent(t, fx, "job.lane_registered"); registered["hub_lane"] != "worker-a" {
		t.Fatalf("job.lane_registered=%v", registered)
	}
}
