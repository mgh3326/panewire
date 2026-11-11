package panewire

import (
	"context"
	"fmt"
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
	rig.q.hk.tasks = func(context.Context) ([]byte, error) { return []byte(list), nil }
	rig.run()
	if lookedUp != "42" {
		t.Fatalf("task lookup id=%q, want 42", lookedUp)
	}
	if decision, reason := rig.decision(t); decision != "wait" || reason != "reset_within_expected" {
		t.Fatalf("decision=%s reason=%s, want wait reset_within_expected", decision, reason)
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
