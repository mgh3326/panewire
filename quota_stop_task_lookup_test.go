package panewire

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQuotaStopTaskLookupByIDIgnoresFullTaskList(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
	// The list is deliberately beyond the old limit. The manager must use the
	// task id in the job receipt and never inspect this page.
	rows := make([]string, quotaStopTaskLimit+1)
	for i := range rows {
		rows[i] = `{"priority":3,"expected":"4h","refs":{"job_id":"job-active"}}`
	}
	list := `{"tasks":[` + strings.Join(rows, ",") + `]}`
	lookedUp := ""
	rig.q.hk.task = func(_ context.Context, id string) ([]byte, error) {
		lookedUp = id
		return []byte(`{"priority":3,"expected":"4h","refs":{"job_id":"job-active"}}`), nil
	}
	rig.q.hk.tasks = func(context.Context, int64) ([]byte, error) { return []byte(list), nil }
	rig.run()
	if lookedUp != "42" {
		t.Fatalf("task lookup id=%q, want 42", lookedUp)
	}
	if decision, reason := rig.decision(t); decision != "wait" || reason != "reset_within_expected" {
		t.Fatalf("decision=%s reason=%s, want wait reset_within_expected", decision, reason)
	}
}

func TestQuotaStopTaskLookupFindsTaskPastFirstPage(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
	for _, name := range []string{"00001-job.claimed.json", "00002-job.spawned.json"} {
		path := filepath.Join(rig.manager.inboxRoot, "jobs", "job-active", "events", name)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		updated := strings.ReplaceAll(string(b), `,"task_id":"42"`, "")
		if err := os.WriteFile(path, []byte(updated), 0600); err != nil {
			t.Fatal(err)
		}
	}

	rows := make([]string, quotaStopTaskLimit+1)
	for i := range rows {
		job := fmt.Sprintf("other-%d", i)
		if i == quotaStopTaskLimit {
			job = "job-active"
		}
		rows[i] = fmt.Sprintf(`{"id":%d,"priority":3,"expected":"4h","refs":{"job_id":%q}}`, i+1, job)
	}
	rig.q.hk.tasks = func(_ context.Context, afterID int64) ([]byte, error) {
		start := int(afterID)
		end := min(start+quotaStopTaskLimit, len(rows))
		return []byte(`{"tasks":[` + strings.Join(rows[start:end], ",") + `]}`), nil
	}

	rig.run()
	if decision, reason := rig.decision(t); decision != "wait" || reason != "reset_within_expected" {
		t.Fatalf("decision=%s reason=%s, want wait reset_within_expected for task after the first page", decision, reason)
	}
}

func TestQuotaStopTaskLookupDoesNotReusePageDecodeState(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
	rows := make([]string, quotaStopTaskLimit)
	for i := range rows {
		job := fmt.Sprintf("other-%d", i)
		if i == 0 {
			job = "job-active"
		}
		rows[i] = fmt.Sprintf(`{"id":%d,"priority":3,"expected":"4h","refs":{"job_id":%q}}`, i+1, job)
	}
	rig.q.hk.tasks = func(_ context.Context, afterID int64) ([]byte, error) {
		if afterID == 0 {
			return []byte(`{"tasks":[` + strings.Join(rows, ",") + `]}`), nil
		}
		return []byte(`{"tasks":[{"id":1001,"priority":3}]}`), nil
	}
	priority, expected, ok := rig.q.task(context.Background(), HubActiveJob{JobID: "job-active"}, "", "")
	if !ok || priority != 3 || expected != 4*time.Hour {
		t.Fatalf("task priority=%d expected=%v ok=%t, want the unique match from page one", priority, expected, ok)
	}
}

func TestQuotaStopTaskLookupCursorUsesExactTaskID(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
	rows := make([]string, quotaStopTaskLimit)
	for i := 0; i < quotaStopTaskLimit-1; i++ {
		rows[i] = fmt.Sprintf(`{"id":%d,"priority":3,"refs":{"job_id":"other"}}`, i+1)
	}
	rows[quotaStopTaskLimit-1] = `{"id":1000000,"priority":3,"refs":{"job_id":"other"}}`
	rig.q.hk.tasks = func(_ context.Context, afterID int64) ([]byte, error) {
		if afterID == 0 {
			return []byte(`{"tasks":[` + strings.Join(rows, ",") + `]}`), nil
		}
		return []byte(`{"tasks":[{"id":1000001,"priority":3,"expected":"4h","refs":{"job_id":"job-active"}}]}`), nil
	}
	priority, expected, ok := rig.q.task(context.Background(), HubActiveJob{JobID: "job-active"}, "", "")
	if !ok || priority != 3 || expected != 4*time.Hour {
		t.Fatalf("task priority=%d expected=%v ok=%t, want the unique match after task id 1,000,000", priority, expected, ok)
	}
}

func TestQuotaStopTaskLookupMissingTaskHandsOver(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
	rig.q.hk.task = func(context.Context, string) ([]byte, error) { return nil, fmt.Errorf("not_found") }
	rig.run()
	if decision, reason := rig.decision(t); decision != "handover" || reason != "task_unavailable" {
		t.Fatalf("decision=%s reason=%s, want handover task_unavailable", decision, reason)
	}
}

func TestQuotaStopTaskLookupMissingMatchReportsReason(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
	for _, name := range []string{"00001-job.claimed.json", "00002-job.spawned.json"} {
		path := filepath.Join(rig.manager.inboxRoot, "jobs", "job-active", "events", name)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		updated := strings.ReplaceAll(string(b), `,"task_id":"42"`, "")
		if err := os.WriteFile(path, []byte(updated), 0600); err != nil {
			t.Fatal(err)
		}
	}
	rig.q.hk.tasks = func(context.Context, int64) ([]byte, error) {
		return []byte(`{"tasks":[]}`), nil
	}
	rig.run()
	if decision, reason := rig.decision(t); decision != "handover" || reason != "task_unavailable" {
		t.Fatalf("decision=%s reason=%s, want handover task_unavailable", decision, reason)
	}
	if !strings.Contains(rig.logs.String(), "error=no_matching_task") {
		t.Fatalf("lookup warnings=%q, want the no_matching_task reason", rig.logs.String())
	}
}

func TestQuotaStopTaskLookupPreservesFoundTaskDecision(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
	rig.run()
	if decision, reason := rig.decision(t); decision != "wait" || reason != "reset_within_expected" {
		t.Fatalf("decision=%s reason=%s, want base decision wait reset_within_expected", decision, reason)
	}
	if rig.sends != 1 || len(rig.announce) != 0 {
		t.Fatalf("sends=%d announced=%d, want one resume and no hand-over", rig.sends, len(rig.announce))
	}
}

func TestQuotaStopTaskLookupAcceptsStringPriority(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
	rig.q.hk.task = func(context.Context, string) ([]byte, error) {
		return []byte(`{"priority":"3","expected":"4h","refs":{"job_id":"job-active"}}`), nil
	}
	rig.run()
	if decision, reason := rig.decision(t); decision != "wait" || reason != "reset_within_expected" {
		t.Fatalf("decision=%s reason=%s, want wait reset_within_expected", decision, reason)
	}
}

func TestQuotaStopTaskLookupRejectsInvalidPriority(t *testing.T) {
	for name, answer := range map[string]string{
		"unparseable string": `{"priority":"urgent","expected":"4h","refs":{"job_id":"job-active"}}`,
		"non-integer number": `{"priority":1.5,"expected":"4h","refs":{"job_id":"job-active"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
			rig.q.hk.task = func(context.Context, string) ([]byte, error) { return []byte(answer), nil }
			rig.run()
			if decision, reason := rig.decision(t); decision != "handover" || reason != "task_unavailable" {
				t.Fatalf("decision=%s reason=%s, want handover task_unavailable", decision, reason)
			}
		})
	}
}

func TestQuotaStopTaskListAcceptsStringPriorityWithoutReceiptID(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, `{"tasks":[{"priority":"3","expected":"4h","refs":{"job_id":"job-active"}}]}`)
	for _, name := range []string{"00001-job.claimed.json", "00002-job.spawned.json"} {
		path := filepath.Join(rig.manager.inboxRoot, "jobs", "job-active", "events", name)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		updated := strings.ReplaceAll(string(b), `,"task_id":"42"`, "")
		if err := os.WriteFile(path, []byte(updated), 0600); err != nil {
			t.Fatal(err)
		}
	}
	rig.run()
	if decision, reason := rig.decision(t); decision != "wait" || reason != "reset_within_expected" {
		t.Fatalf("decision=%s reason=%s, want wait reset_within_expected", decision, reason)
	}
}

func TestQuotaStopTaskListIgnoresUnrelatedInvalidPriority(t *testing.T) {
	answer := `{"tasks":[{"priority":"urgent","refs":{"job_id":"other-job"}},{"priority":3,"expected":"4h","refs":{"job_id":"job-active"}}]}`
	rig := newRoleStopRig(t, "worker", time.Hour, answer)
	for _, name := range []string{"00001-job.claimed.json", "00002-job.spawned.json"} {
		path := filepath.Join(rig.manager.inboxRoot, "jobs", "job-active", "events", name)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		updated := strings.ReplaceAll(string(b), `,"task_id":"42"`, "")
		if err := os.WriteFile(path, []byte(updated), 0600); err != nil {
			t.Fatal(err)
		}
	}
	rig.run()
	if decision, reason := rig.decision(t); decision != "wait" || reason != "reset_within_expected" {
		t.Fatalf("decision=%s reason=%s, want wait reset_within_expected", decision, reason)
	}
}
