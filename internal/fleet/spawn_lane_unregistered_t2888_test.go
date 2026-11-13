package fleet

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// laneRegisterFixture is a cmd spawn with a configured hub whose "lanes add"
// answers come from outcomes in order (the last repeats); every panewire
// emit is recorded.
func laneRegisterFixture(t *testing.T, outcomes ...Result) (*App, Spawn, *fakeRunner, *[]Command, *[]time.Duration) {
	t.Helper()
	a, s, f, _ := cmdFixture(t)
	s.Owner = "desk"
	a.Config.Hub = map[string]any{"hub_url": "https://hub.example.invalid"}
	var emitted []Command
	var sleeps []time.Duration
	baseSleep := a.Sleep
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return baseSleep(ctx, d)
	}
	original, adds := f.Do, 0
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case c.Name == "panewire" && strings.HasPrefix(args, "lanes add"):
			r := outcomes[min(adds, len(outcomes)-1)]
			adds++
			return r
		case c.Name == "panewire" && strings.HasPrefix(args, "emit"):
			emitted = append(emitted, c)
			return Result{}
		}
		return original(c)
	}
	return a, s, f, &emitted, &sleeps
}

func laneAddCalls(f *fakeRunner) int {
	n := 0
	for _, c := range f.Calls {
		if c.Name == "panewire" && len(c.Args) > 1 && c.Args[0] == "lanes" && c.Args[1] == "add" {
			n++
		}
	}
	return n
}

// spawnedReceipt is the payload of the job's job.spawned event.
func spawnedReceipt(t *testing.T, a *App, job string) map[string]any {
	t.Helper()
	evs, err := a.events(job)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range evs {
		if ev.Kind == "job.spawned" {
			return ev.Payload
		}
	}
	t.Fatalf("no job.spawned event for %s", job)
	return nil
}

func flagValue(args []string, name string) string {
	for i, arg := range args {
		if arg == name && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func TestLaneRegistrationRetriesOnceAndSucceeds(t *testing.T) {
	a, s, f, emitted, sleeps := laneRegisterFixture(t, Result{Code: 4, Err: "hub unreachable"}, Result{})
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if n := laneAddCalls(f); n != 2 {
		t.Fatalf("lanes add calls=%d, want 2", n)
	}
	if len(*sleeps) == 0 {
		t.Fatal("retry did not pause")
	}
	r := spawnedReceipt(t, a, s.Job)
	if r["hub_lane"] != "worker-a" || r["lane_unregistered"] != nil {
		t.Fatalf("receipt after a successful retry: %v", r)
	}
	if len(*emitted) != 0 || strings.Contains(a.Out.(*bytes.Buffer).String(), "UNREGISTERED") {
		t.Fatalf("recovered registration was reported: %v %s", *emitted, a.Out)
	}
}

func TestLaneRegistrationFailureIsLoudButSucceedsByDefault(t *testing.T) {
	a, s, f, emitted, _ := laneRegisterFixture(t, Result{Code: 6, Err: "lanes rejected"})
	t.Setenv("PANEWIRE_LANE_STRICT", "")
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("default mode must succeed: %v", err)
	}
	if n := laneAddCalls(f); n != 2 {
		t.Fatalf("lanes add calls=%d, want exactly one retry", n)
	}
	r := spawnedReceipt(t, a, s.Job)
	if r["lane_unregistered"] != true || r["hub_lane"] != nil || r["hub_lane_machine"] != nil {
		t.Fatalf("receipt: %v", r)
	}
	if out := a.Out.(*bytes.Buffer).String(); !strings.Contains(out, "landed=yes") || !strings.Contains(out, "lane=UNREGISTERED") {
		t.Fatalf("OK line does not name the problem: %s", out)
	}
	if len(*emitted) != 1 {
		t.Fatalf("emits=%d, want 1", len(*emitted))
	}
	args := (*emitted)[0].Args
	text := flagValue(args, "--text")
	if flagValue(args, "--kind") != "lane.event" || flagValue(args, "--lane") != "desk" || flagValue(args, "--pane") != "w:p1" ||
		flagValue(args, "--event-id") != s.Job+"-lane-unregistered" {
		t.Fatalf("event routing: %v", args)
	}
	if !strings.HasPrefix(text, "["+s.Job+"] LANE_UNREGISTERED | ") || strings.Contains(text, "READY") || strings.Contains(text, "ESC") {
		t.Fatalf("event text must name LANE_UNREGISTERED and avoid wait/completion keywords: %q", text)
	}
	if warn := a.Err.(*bytes.Buffer).String(); !strings.Contains(warn, "lanes rejected") {
		t.Fatalf("cause lost: %s", warn)
	}
}

func TestLaneRegistrationFailureStrictExitsDistinctCode(t *testing.T) {
	a, s, _, emitted, _ := laneRegisterFixture(t, Result{Code: 6, Err: "lanes rejected"})
	t.Setenv("PANEWIRE_LANE_STRICT", "1")
	err := a.Spawn(context.Background(), s)
	if code(err) != exitLaneUnregistered {
		t.Fatalf("strict exit=%d (%v), want %d", code(err), err, exitLaneUnregistered)
	}
	if r := spawnedReceipt(t, a, s.Job); r["lane_unregistered"] != true {
		t.Fatalf("strict mode must still record the committed spawn: %v", r)
	}
	if len(*emitted) != 1 {
		t.Fatalf("strict mode skipped the event: %v", *emitted)
	}
}

func TestLaneRegistrationConfigRefusalIsNotRetried(t *testing.T) {
	a, s, f, _, _ := laneRegisterFixture(t, Result{})
	a.Config.Hub = map[string]any{} // "hub lane config missing" is exitConfig
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if laneAddCalls(f) != 0 {
		t.Fatal("config refusal reached the hub")
	}
	if r := spawnedReceipt(t, a, s.Job); r["lane_unregistered"] != true {
		t.Fatalf("receipt: %v", r)
	}
}

func TestLaneUnregisteredEventFailureDoesNotFailSpawn(t *testing.T) {
	a, s, f, _, _ := laneRegisterFixture(t, Result{Code: 6, Err: "lanes rejected"})
	t.Setenv("PANEWIRE_LANE_STRICT", "")
	original := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "panewire" && len(c.Args) > 0 && c.Args[0] == "emit" {
			return Result{Code: 1, Err: "socket down"}
		}
		return original(c)
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if warn := a.Err.(*bytes.Buffer).String(); !strings.Contains(warn, "LANE_UNREGISTERED event to desk not delivered") {
		t.Fatalf("undelivered event not warned: %s", warn)
	}
}

func TestWaitOutputAndSpawnedNoticeNameUnregisteredLane(t *testing.T) {
	var out bytes.Buffer
	SpawnAttempt{Host: "node-b", Profile: "sonnet", Pane: "w:p9", LaneUnregistered: true}.print(&out, "job")
	if got := out.String(); got != "SPAWNED job=job host=node-b profile=sonnet pane=w:p9 lane=UNREGISTERED\n" {
		t.Fatalf("SPAWNED line: %q", got)
	}

	a := testApp(t)
	s := Spawn{Job: "job-w", Owner: "desk", Role: "implementer", WaitTimeout: "1s", laneUnregistered: true}
	putEvent(t, a, s.Job, "job.completed", map[string]any{"report": "r.md"})
	if err := a.waitJob(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if got := a.Out.(*bytes.Buffer).String(); !strings.Contains(got, "wait job=job-w outcome=completed") || !strings.Contains(got, "lane=UNREGISTERED") {
		t.Fatalf("--wait output: %q", got)
	}
}
