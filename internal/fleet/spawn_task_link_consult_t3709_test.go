package fleet

import (
	"context"
	"testing"
)

// A consult works beside the task's claimant: it must not take the task's
// exclusive claim, so a fable+astra pair can run under one task (hk 3709).
func TestSpawnTaskLinkConsultDoesNotClaimT3709(t *testing.T) {
	ctx := context.Background()
	consult := func(job, label string) Spawn {
		return Spawn{Task: "42", Job: job, Label: label, Role: "counsel", RoutingRole: "consult", Owner: "owner"}
	}
	taskLinkStatus := func(t *testing.T, a *App, job string) string {
		t.Helper()
		events, e := a.ledger().Events(ctx, job)
		if e != nil {
			t.Fatal(e)
		}
		links := 0
		status := ""
		for _, ev := range events {
			if ev.Kind != "job.task_link" {
				continue
			}
			links++
			status, _ = ev.Payload["status"].(string)
		}
		if links != 1 {
			t.Fatalf("job.task_link events=%d want 1", links)
		}
		return status
	}

	t.Run("consult pair shares one task", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.SetTask("42", hkTaskJSON(42, "ready", "consult (grade B, T1)", "", "panewire", `{}`)).Handle(hkClaimAnswer)
		for _, s := range []Spawn{consult("fable-job", "fable-label"), consult("astra-job", "astra-label")} {
			task, _, e := a.taskPrecheck(ctx, s)
			if e != nil {
				t.Fatalf("%s precheck: %v", s.Job, e)
			}
			if e := a.taskLink(ctx, s, task, false); e != nil {
				t.Fatalf("%s taskLink: %v", s.Job, e)
			}
			if got := taskLinkStatus(t, a, s.Job); got != "unlinked" {
				t.Fatalf("%s job.task_link status=%q want unlinked", s.Job, got)
			}
		}
		hkOnly(t, hk, "GET /v1/tasks/42", "GET /v1/tasks/42")
	})

	t.Run("consult joins a task another claimant holds", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.SetTask("42", hkTaskJSON(42, "developing", "consult (grade B, T1)", "fable-label", "panewire", `{"job_id":"fable-job"}`))
		s := consult("astra-job", "astra-label")
		task, _, e := a.taskPrecheck(ctx, s)
		if e != nil {
			t.Fatalf("consult precheck: %v", e)
		}
		if e := a.taskLink(ctx, s, task, false); e != nil {
			t.Fatalf("consult taskLink: %v", e)
		}
		hkOnly(t, hk, "GET /v1/tasks/42")
		if got := taskLinkStatus(t, a, s.Job); got != "unlinked" {
			t.Fatalf("job.task_link status=%q want unlinked", got)
		}
	})

	t.Run("consult still refuses a parked task", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.SetTask("42", hkTaskJSON(42, "hold", "held (grade B, T1)", "", "panewire", `{}`))
		if _, _, e := a.taskPrecheck(ctx, consult("astra-job", "astra-label")); exitCode(e) != exitUsage || hkErrOf(e) != "task unavailable: hold" {
			t.Fatalf("consult precheck=%d %q, want the parked-task refusal", exitCode(e), hkErrOf(e))
		}
	})

	t.Run("worker still refuses another claimant", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.SetTask("42", hkTaskJSON(42, "developing", "implement (grade B, T1)", "fable-label", "panewire", `{}`))
		s := Spawn{Task: "42", Job: "worker-job", Label: "worker-label", Role: "worker"}
		if _, _, e := a.taskPrecheck(ctx, s); exitCode(e) != exitUsage || hkErrOf(e) != "task has another claimant without a job binding" {
			t.Fatalf("worker precheck=%d %q, want the unchanged claimant refusal", exitCode(e), hkErrOf(e))
		}
	})

	t.Run("worker with a consult routing role still refuses another claimant", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.SetTask("42", hkTaskJSON(42, "developing", "implement (grade B, T1)", "fable-label", "panewire", `{}`))
		s := Spawn{Task: "42", Job: "worker-job", Label: "worker-label", Role: "worker", RoutingRole: "consult"}
		if _, _, e := a.taskPrecheck(ctx, s); exitCode(e) != exitUsage || hkErrOf(e) != "task has another claimant without a job binding" {
			t.Fatalf("worker+consult precheck=%d %q, want the claimant refusal", exitCode(e), hkErrOf(e))
		}
	})
}
