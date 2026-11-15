package panewire

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

func TestStallSafetyActiveJobExamination(t *testing.T) {
	for _, mode := range []string{"idle", "child", "quota", "retry", "changed", "ready"} {
		t.Run(mode, func(t *testing.T) {
			rig := newRoleStopRig(t, "implementer", time.Hour, `{"tasks":[{"priority":1,"expected":"4h","refs":{"job_id":"job-active"}}]}`)
			ctx := context.Background()
			rig.observe("idle", 1, rig.now)
			var notes []string
			rig.q.hk.note = func(_ context.Context, key, job, body string) error {
				notes = append(notes, key+" "+job+" "+body)
				if mode == "retry" && len(notes) == 1 {
					return errors.New("temporary hk failure")
				}
				return nil
			}
			m, err := newStallSafetyManager(rig.q, rig.manager, 30*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			signalled := false
			m.signalled = func(context.Context, HubActiveJob) (bool, error) { return signalled, nil }
			m.busy = func(context.Context, HubActiveJob, HerdrAgentState) (bool, error) { return mode == "child", nil }
			reads := 0
			screen := strings.Repeat("x", 3000) + " last screen lines"
			if mode == "quota" {
				data, err := os.ReadFile("testdata/quota-stop/codex.txt")
				if err != nil {
					t.Fatal(err)
				}
				screen = string(data)
			}
			m.read = func(context.Context, string) (string, error) {
				reads++
				if mode == "changed" {
					rig.observe("working", 2, rig.now.Add(31*time.Minute))
				}
				if mode == "ready" {
					signalled = true
				}
				return screen, nil
			}
			m.explain = func(context.Context, string) (string, error) { return `{"agent":"codex"}`, nil }
			m.detect = func(_ context.Context, _ string, _ HerdrAgentState, screen, _ string, at time.Time) (quotastop.Stop, bool) {
				stop, _, ok := quotastop.Detect(screen, "codex", "", quotastop.DevinSessionEvidence{}, at, time.Local)
				return stop, ok
			}
			agent := HerdrAgentState{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Harness: "codex"}
			for _, at := range []time.Time{rig.now, rig.now.Add(31 * time.Minute), rig.now.Add(32 * time.Minute)} {
				if err := m.scan(ctx, []HerdrAgentState{agent}, at); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "child" {
				if reads != 0 || len(notes) != 0 || len(rig.owner) != 0 {
					t.Fatalf("child examined: reads=%d notes=%v events=%v", reads, notes, rig.owner)
				}
				return
			}
			wantNotes := 1
			if mode == "retry" {
				wantNotes = 2
			}
			if reads != 1 || len(notes) != wantNotes || !strings.Contains(notes[0], "job-active") {
				t.Fatalf("reads=%d notes=%v", reads, notes)
			}
			if mode == "idle" || mode == "retry" {
				if len(rig.owner) != 1 || rig.owner[0].NotifyType != "blocked" || !strings.Contains(rig.owner[0].Text, "last screen lines") {
					t.Fatalf("owner events=%+v", rig.owner)
				}
				return
			}
			if mode == "changed" || mode == "ready" {
				if len(rig.owner) != 0 {
					t.Fatalf("stale examination notified owner: %+v", rig.owner)
				}
				return
			}
			decision, reason := rig.decision(t)
			if decision != "handover" || reason != "urgent" {
				t.Fatalf("quota decision=%q reason=%q", decision, reason)
			}
			if len(rig.owner) != 0 {
				t.Fatalf("quota emitted blocked event: %+v", rig.owner)
			}
			rig.manager.Tick(ctx, rig.now.Add(33*time.Minute))
			if len(rig.announce) != 1 || len(rig.owner) != 0 {
				t.Fatalf("quota action: announced=%v events=%v", rig.announce, rig.owner)
			}
		})
	}
}

func TestStallSafetyQuotaWaitRespectsPaneState(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed=%t", changed), func(t *testing.T) {
			installFakeHerdr(t, t.TempDir(), "#!/bin/sh\nprintf '%s\\n' '{\"agent\":\"codex\"}'\n")
			rig := newRoleStopRig(t, "implementer", time.Hour, roleStopTask)
			ctx := context.Background()
			rig.observe("idle", 1, rig.now)
			m, err := newStallSafetyManager(rig.q, rig.manager, 30*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			m.signalled = func(context.Context, HubActiveJob) (bool, error) { return false, nil }
			m.busy = func(context.Context, HubActiveJob, HerdrAgentState) (bool, error) { return false, nil }
			m.explain = func(context.Context, string) (string, error) { return `{"agent":"codex"}`, nil }
			data, err := os.ReadFile("testdata/quota-stop/codex.txt")
			if err != nil {
				t.Fatal(err)
			}
			m.read = func(context.Context, string) (string, error) { return string(data), nil }
			rig.q.read = m.read
			m.detect = func(context.Context, string, HerdrAgentState, string, string, time.Time) (quotastop.Stop, bool) {
				return quotastop.Stop{Pool: "codex", Reset: rig.now.Add(35 * time.Minute)}, true
			}
			agent := HerdrAgentState{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Harness: "codex"}
			for _, at := range []time.Time{rig.now, rig.now.Add(31 * time.Minute)} {
				if err := m.scan(ctx, []HerdrAgentState{agent}, at); err != nil {
					t.Fatal(err)
				}
			}
			if decision, reason := rig.decision(t); decision != "wait" {
				t.Fatalf("decision=%s reason=%s", decision, reason)
			}
			if changed {
				rig.observe("working", 2, rig.now.Add(34*time.Minute))
			}
			rig.manager.Tick(ctx, rig.now.Add(36*time.Minute))
			want := 1
			if changed {
				want = 0
			}
			if rig.sends != want {
				t.Fatalf("resumes=%d want %d", rig.sends, want)
			}
		})
	}
}
