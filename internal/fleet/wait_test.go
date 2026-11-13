package fleet

import (
	"context"
	"strings"
	"testing"
)

func waitApp(t *testing.T) (*App, Spawn) {
	t.Helper()
	a := testApp(t)
	putEvent(t, a, "j", "job.claim", map[string]any{"owner_lane": "default", "task_id": "42", "t_level": "T1", "role": "implementer"})
	return a, Spawn{Job: "j", Role: "implementer", Owner: "default", WaitTimeout: "10s"}
}

func waitOut(a *App) string { return a.Out.(interface{ String() string }).String() }

func TestWaitCompletedFromEvents(t *testing.T) {
	a, s := waitApp(t)
	putEvent(t, a, "j", "job.completed", map[string]any{"report": "/jobs/j/report.md", "pane_id": "w:p1"})
	if e := a.waitJob(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	out := waitOut(a)
	if !strings.Contains(out, "outcome=completed") || !strings.Contains(out, "report=/jobs/j/report.md") {
		t.Fatalf("%s", out)
	}
}

func TestWaitEscalateDistinctCode(t *testing.T) {
	a, s := waitApp(t)
	putEvent(t, a, "j", "job.escalate", map[string]any{"question": "which branch?"})
	if code(a.waitJob(context.Background(), s)) != 83 {
		t.Fatal("escalate must exit 83")
	}
	if !strings.Contains(waitOut(a), "outcome=escalate") {
		t.Fatal("escalate outcome missing")
	}
}

func TestWaitLostAndRevokedDistinctCode(t *testing.T) {
	for _, kind := range []string{"job.lost", "job.revoked"} {
		t.Run(kind, func(t *testing.T) {
			a, s := waitApp(t)
			putEvent(t, a, "j", kind, map[string]any{"pane_id": "w:p1"})
			if code(a.waitJob(context.Background(), s)) != 84 {
				t.Fatalf("%s must exit 84", kind)
			}
		})
	}
}

func TestWaitJoinedIsSuccess(t *testing.T) {
	a, s := waitApp(t)
	putEvent(t, a, "j", "job.joined", map[string]any{"pane_id": "w:p1"})
	if e := a.waitJob(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(waitOut(a), "outcome=joined") {
		t.Fatal("joined outcome missing")
	}
}

func TestWaitReadyFromLaneInbox(t *testing.T) {
	a, s := waitApp(t)
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if c.Name == "panewire" && strings.HasPrefix(strings.Join(c.Args, " "), "inbox list") {
			return Result{Out: `[{"kind":"lane.event","event":{"job_id":"x","text":"[j] READY | https://pr.invalid/1 | /jobs/j/report.md"}}]`}
		}
		return Result{}
	}}
	if e := a.waitJob(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	out := waitOut(a)
	if !strings.Contains(out, "outcome=ready") || !strings.Contains(out, "report=/jobs/j/report.md") {
		t.Fatalf("READY must carry the report field: %s", out)
	}
}

func TestWaitCompletedFromLaneInbox(t *testing.T) {
	a, s := waitApp(t)
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if c.Name == "panewire" {
			return Result{Out: `[{"kind":"job.completed","event":{"job_id":"j","report":"/jobs/j/r.md"}}]`}
		}
		return Result{}
	}}
	if e := a.waitJob(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(waitOut(a), "outcome=completed") {
		t.Fatal("inbox completion missing")
	}
}

func TestWaitDecisionFromLaneInboxStaysLive(t *testing.T) {
	a, s := waitApp(t)
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		return Result{Out: `[{"kind":"lane.event","event":{"job_id":"x","text":"[j] ESC | should this be READY?"}}]`}
	}}
	if code(a.waitJob(context.Background(), s)) != exitWaitTimeout {
		t.Fatal("lane ESC must leave the job live until wait timeout")
	}
}

func TestWaitUnrelatedLaneNoiseIgnored(t *testing.T) {
	a, s := waitApp(t)
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		return Result{Out: `[{"kind":"lane.event","event":{"job_id":"x","text":"[other-job] READY | pr | /x"}},{"kind":"job.completed","event":{"job_id":"other"}}]`}
	}}
	if code(a.waitJob(context.Background(), s)) != 85 {
		t.Fatal("other jobs' signals must not end the wait")
	}
}

func TestWaitTimeoutDistinctCode(t *testing.T) {
	a, s := waitApp(t)
	s.WaitTimeout = "20s"
	if code(a.waitJob(context.Background(), s)) != 85 {
		t.Fatal("timeout must exit 85")
	}
	if !strings.Contains(waitOut(a), "outcome=timeout") {
		t.Fatal("timeout outcome missing")
	}
}

func TestWaitTimeoutEnvFallback(t *testing.T) {
	a, s := waitApp(t)
	s.WaitTimeout = ""
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "30")
	if code(a.waitJob(context.Background(), s)) != 85 {
		t.Fatal("env timeout must exit 85")
	}
}

func TestWaitInvalidTimeoutRefused(t *testing.T) {
	a, s := waitApp(t)
	s.WaitTimeout = "nonsense"
	if code(a.waitJob(context.Background(), s)) != 2 {
		t.Fatal("an unparseable timeout is a usage error")
	}
}

func TestWaitFlagCombinationsRefused(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	s.Wait = true
	s.DryRun = true
	if code(a.Spawn(context.Background(), s)) != 2 {
		t.Fatal("--wait with --dry-run must refuse")
	}
	s.DryRun = false
	s.Wait = false
	s.WaitTimeout = "10s"
	if code(a.Spawn(context.Background(), s)) != 2 {
		t.Fatal("--wait-timeout without --wait must refuse")
	}
}

func TestWaitRunsAfterLocalSpawn(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	s.Wait = true
	s.WaitTimeout = "10s"
	putEvent(t, a, "job-a", "job.completed", map[string]any{"report": "/jobs/job-a/report.md"})
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(waitOut(a), "outcome=completed") {
		t.Fatal("the wait must follow a successful spawn")
	}
}
