package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// The quota fixture uses flat relay events; the node's watcher journal also
// carries kind and payload. Include both forms to exercise the real watcher.
func stallSafetyWatcherJournal(t *testing.T, rig *roleStopRig) {
	t.Helper()
	root, err := filepath.EvalSymlinks(rig.manager.inboxRoot)
	if err != nil {
		t.Fatal(err)
	}
	rig.manager.inboxRoot, rig.q.root = root, root
	dir := filepath.Join(root, "jobs", "job-active", "events")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var event map[string]any
		if err := json.Unmarshal(body, &event); err != nil {
			t.Fatal(err)
		}
		payload := make(map[string]any, len(event))
		for key, value := range event {
			payload[key] = value
		}
		event["kind"] = event["type"]
		event["payload"] = payload
		body, err = json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStallSafetyActiveJobExamination(t *testing.T) {
	for _, mode := range []string{"idle", "child", "quota", "retry", "action_retry", "changed", "ready", "nonworking", "episode_ended"} {
		t.Run(mode, func(t *testing.T) {
			rig := newRoleStopRig(t, "implementer", time.Hour, `{"tasks":[{"priority":1,"expected":"4h","refs":{"job_id":"job-active"}}]}`)
			stallSafetyWatcherJournal(t, rig)
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
			if job, ok := rig.q.jobForPane(ctx, "workspace:pane-1"); !ok {
				t.Fatal("fixture job not found")
			} else if _, _, err := m.signalled(ctx, job); err != nil {
				t.Fatalf("watcher journal: %v", err)
			}
			if mode == "action_retry" {
				act := m.act
				attempts := 0
				m.act = func(ctx context.Context, r stallSafetyExamination, d stallSafetyDetails, key string, at time.Time) (string, error) {
					attempts++
					if attempts == 1 {
						return "", errors.New("temporary delivery failure")
					}
					return act(ctx, r, d, key, at)
				}
			}
			signalled := false
			watcherSignal := m.signalled
			m.signalled = func(ctx context.Context, job HubActiveJob) (string, bool, error) {
				attempt, found, err := watcherSignal(ctx, job)
				return attempt, found || signalled, err
			}
			m.busy = func(context.Context, HubActiveJob, HerdrAgentState) (bool, error) { return mode == "child", nil }
			reads := 0
			screen := strings.Repeat("x", 3000) + "\nlast screen lines\x1b[0m"
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
				if mode == "nonworking" {
					rig.observe("blocked", 2, rig.now.Add(31*time.Minute))
				}
				if mode == "episode_ended" {
					if err := m.state.markWorking(ctx, "workspace:pane-1"); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "ready" {
					signalled = true
				}
				return screen, nil
			}
			m.explain = func(context.Context, string) (string, error) { return `{"agent":"codex"}`, nil }
			agent := HerdrAgentState{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Harness: "codex"}
			for index, at := range []time.Time{rig.now, rig.now.Add(31 * time.Minute), rig.now.Add(32 * time.Minute)} {
				if mode == "nonworking" && index == 2 {
					agent.Status = "blocked"
				}
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
			if mode == "retry" || mode == "action_retry" || mode == "nonworking" {
				wantNotes = 2
			}
			if reads != 1 || len(notes) != wantNotes || !strings.Contains(notes[0], "job-active") {
				t.Fatalf("reads=%d notes=%v", reads, notes)
			}
			if mode == "action_retry" && (!strings.Contains(notes[0], "pending") || !strings.Contains(notes[1], "blocked event recorded")) {
				t.Fatalf("action retry records=%v", notes)
			}
			if wantNotes == 2 && strings.Fields(notes[0])[0] != strings.Fields(notes[1])[0] {
				t.Fatalf("retry changed hk document key")
			}
			if mode == "idle" || mode == "retry" || mode == "action_retry" || mode == "nonworking" {
				if len(rig.owner) != 1 || rig.owner[0].NotifyType != "blocked" || !strings.Contains(rig.owner[0].Text, "last screen lines") || !validLaneEventText(rig.owner[0].Text) {
					t.Fatalf("owner events=%+v", rig.owner)
				}
				published := scanHubLaneEventsWithin(rig.manager.inboxRoot, 0)
				if len(published) != 1 || published[0].NotifyType != "blocked" || !strings.Contains(published[0].Text, "last screen lines") {
					t.Fatalf("durable lane events=%+v", published)
				}
				return
			}
			if mode == "changed" || mode == "ready" || mode == "episode_ended" {
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
			stallSafetyWatcherJournal(t, rig)
			ctx := context.Background()
			rig.observe("idle", 1, rig.now)
			m, err := newStallSafetyManager(rig.q, rig.manager, 30*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
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

func TestStallSafetyExcludesJobOutsideActiveWindow(t *testing.T) {
	t.Setenv("PANEWIRE_JOB_ACTIVE_MAX_AGE", "72h")
	rig := newRoleStopRig(t, "implementer", 73*time.Hour, roleStopTask)
	rig.observe("idle", 1, rig.now)
	reads, notes := 0, 0
	rig.q.read = func(context.Context, string) (string, error) { reads++; return "screen", nil }
	rig.q.hk.note = func(context.Context, string, string, string) error { notes++; return nil }
	m, err := newStallSafetyManager(rig.q, rig.manager, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	agent := HerdrAgentState{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Harness: "codex"}
	for _, at := range []time.Time{rig.now, rig.now.Add(31 * time.Minute), rig.now.Add(2 * time.Hour)} {
		if err := m.scan(context.Background(), []HerdrAgentState{agent}, at); err != nil {
			t.Fatal(err)
		}
	}
	if reads != 0 || notes != 0 || len(rig.owner) != 0 {
		t.Fatalf("job outside active window was examined: reads=%d notes=%d events=%v", reads, notes, rig.owner)
	}
}
