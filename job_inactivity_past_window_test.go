package panewire

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quotastop"
)

// hk 4836: a job whose claim is older than the active-job window is examined
// only after hk confirms it is still the job running on the pane.

// writePastWindowJob writes a claimed and spawned job whose claim is older
// than the active window. Both events carry kind as well as type, and the
// payload form the node's watcher journal writes, because the stall safety
// net reads the ledger and the inbox index from the same files.
func writePastWindowJob(t *testing.T, root, job, pane, started, harnessID string) {
	t.Helper()
	events := filepath.Join(root, "jobs", job, "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	claim := fmt.Sprintf(`{"type":"job.claimed","kind":"job.claimed","created_at":%q,"agent_label":"worker-synthetic","owner_lane":"owner","role":"implementer","epoch":1}`, started)
	spawn := fmt.Sprintf(`{"type":"job.spawned","kind":"job.spawned","created_at":%q,"payload":{"pane_id":%q,"herdr_session":"panewire","role":"implementer","harness_session":{"kind":"codex","id":%q,"source":"herdr:codex"}}}`, started, pane, harnessID)
	for name, body := range map[string]string{"00001-job.claimed.json": claim, "00002-job.spawned.json": spawn} {
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

// resolvePastWindowRoot points the rig at the real path of its inbox: the
// ledger reader refuses a symlinked path, and a macOS temp dir is one.
func resolvePastWindowRoot(t *testing.T, rig *roleStopRig) {
	t.Helper()
	root, err := filepath.EvalSymlinks(rig.q.root)
	if err != nil {
		t.Fatal(err)
	}
	rig.manager.inboxRoot, rig.q.root = root, root
}

func hkVouchedRecord() map[string]any {
	return map[string]any{"state": "spawned", "task_id": float64(42), "pane_id": "workspace:pane-1", "harness": "codex"}
}

func countStallUnexaminedLogs(logs *bytes.Buffer) int {
	return strings.Count(logs.String(), "stall safety left the job unexamined")
}

// TestStallSafetyPastActiveWindowNeedsHK drives a 96h job on its own live pane
// through three scans, past the 30m limit, and varies what hk says about it.
// Only the confirmed case is examined, and only once.
func TestStallSafetyPastActiveWindowNeedsHK(t *testing.T) {
	t.Setenv("PANEWIRE_JOB_ACTIVE_MAX_AGE", "72h")
	const taskActive = `{"id":42,"state":"developing","refs":{"job_id":"job-active"}}`
	for _, tc := range []struct {
		name     string
		hk       func() (map[string]any, error)
		task     string
		live     string
		examined bool
		logs     int
	}{
		{
			name:     "hk active",
			hk:       func() (map[string]any, error) { return hkVouchedRecord(), nil },
			task:     taskActive,
			live:     "sess-old",
			examined: true,
		},
		{
			name: "hk terminal",
			hk: func() (map[string]any, error) {
				record := hkVouchedRecord()
				record["state"] = "completed"
				return record, nil
			},
			task: taskActive,
			live: "sess-old",
			logs: 1,
		},
		{
			name: "task not in work",
			hk:   func() (map[string]any, error) { return hkVouchedRecord(), nil },
			task: `{"id":42,"state":"merged","refs":{"job_id":"job-active"}}`,
			live: "sess-old",
			logs: 1,
		},
		{
			name: "task bound to another job",
			hk:   func() (map[string]any, error) { return hkVouchedRecord(), nil },
			task: `{"id":42,"state":"developing","refs":{"job_id":"job-other"}}`,
			live: "sess-old",
			logs: 1,
		},
		{
			name: "pane reused by another session",
			hk:   func() (map[string]any, error) { return hkVouchedRecord(), nil },
			task: taskActive,
			live: "sess-new",
			logs: 1,
		},
		{
			name: "hk names another pane",
			hk: func() (map[string]any, error) {
				record := hkVouchedRecord()
				record["pane_id"] = "workspace:pane-9"
				return record, nil
			},
			task: taskActive,
			live: "sess-old",
			logs: 1,
		},
		{
			name: "hk names another harness",
			hk: func() (map[string]any, error) {
				record := hkVouchedRecord()
				record["harness"] = "claude"
				return record, nil
			},
			task: taskActive,
			live: "sess-old",
			logs: 1,
		},
		{
			name: "hk down",
			hk:   func() (map[string]any, error) { return nil, os.ErrNotExist },
			task: taskActive,
			live: "sess-old",
			logs: 1,
		},
		{
			name: "task-less job",
			hk: func() (map[string]any, error) {
				record := hkVouchedRecord()
				delete(record, "task_id")
				return record, nil
			},
			live:     "sess-old",
			examined: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newRoleStopRig(t, "implementer", 96*time.Hour, roleStopTask)
			resolvePastWindowRoot(t, rig)
			started := rig.now.Add(-96 * time.Hour).UTC().Format(time.RFC3339)
			writePastWindowJob(t, rig.q.root, "job-active", "workspace:pane-1", started, "sess-old")
			rig.observe("idle", 1, rig.now)

			var notes []string
			rig.q.hk.note = func(_ context.Context, key, job, body string) error {
				notes = append(notes, key+" "+job)
				return nil
			}
			rig.q.hk.job = func(context.Context, string) (map[string]any, error) { return tc.hk() }
			if tc.task != "" {
				rig.q.hk.task = func(context.Context, string) ([]byte, error) { return []byte(tc.task), nil }
			}

			m, err := newStallSafetyManager(rig.q, rig.manager, 30*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			reads := 0
			m.busy = func(context.Context, HubActiveJob, HerdrAgentState) (bool, error) { return false, nil }
			m.session = func(_ context.Context, pane, kind string) fleet.HarnessSession {
				return fleet.HarnessSession{Kind: kind, ID: tc.live, Source: "herdr:codex"}
			}
			m.read = func(context.Context, string) (string, error) {
				reads++
				return "last screen lines", nil
			}
			m.explain = func(context.Context, string) (string, error) { return `{"agent":"codex"}`, nil }
			m.detect = func(context.Context, string, HerdrAgentState, string, string, time.Time) (quotastop.Stop, bool) {
				return quotastop.Stop{}, false
			}
			agent := HerdrAgentState{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Harness: "codex"}
			for _, at := range []time.Time{rig.now, rig.now.Add(31 * time.Minute), rig.now.Add(32 * time.Minute)} {
				if err := m.scan(context.Background(), []HerdrAgentState{agent}, at); err != nil {
					t.Fatal(err)
				}
			}

			want := 0
			if tc.examined {
				want = 1
			}
			if reads != want || len(notes) != want || len(rig.owner) != want {
				t.Fatalf("examined=%t: reads=%d notes=%v owner=%+v", tc.examined, reads, notes, rig.owner)
			}
			if tc.examined && !strings.Contains(notes[0], "job-active") {
				t.Fatalf("examination recorded for another job: %v", notes)
			}
			if got := countStallUnexaminedLogs(&rig.logs); got != tc.logs {
				t.Fatalf("unexamined log lines=%d want %d: %s", got, tc.logs, rig.logs.String())
			}
		})
	}
}

// TestStallSafetyPastActiveWindowBoundsHKReads gives one pass more old journals
// than its hk budget: only stallSafetyConfirmPerPass hk job records are read,
// the rest wait for the next pass, and the cached verdicts are not re-asked.
func TestStallSafetyPastActiveWindowBoundsHKReads(t *testing.T) {
	t.Setenv("PANEWIRE_JOB_ACTIVE_MAX_AGE", "72h")
	rig := newRoleStopRig(t, "implementer", 96*time.Hour, roleStopTask)
	resolvePastWindowRoot(t, rig)
	started := rig.now.Add(-96 * time.Hour).UTC().Format(time.RFC3339)
	writePastWindowJob(t, rig.q.root, "job-active", "workspace:pane-1", started, "sess-old")
	sessions := map[string]string{"workspace:pane-1": "sess-old"}
	agents := []HerdrAgentState{{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Harness: "codex"}}
	panes := map[string]string{"job-active": "workspace:pane-1"}
	for i := 2; i <= 6; i++ {
		job := fmt.Sprintf("job-old-%d", i)
		pane := fmt.Sprintf("workspace:pane-%d", i)
		writePastWindowJob(t, rig.q.root, job, pane, started, "sess-"+job)
		sessions[pane], panes[job] = "sess-"+job, pane
		if err := rig.manager.Observe(context.Background(), HerdrAgentState{PaneID: pane, WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Revision: 1}, rig.now); err != nil {
			t.Fatal(err)
		}
		agents = append(agents, HerdrAgentState{PaneID: pane, WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Harness: "codex"})
	}
	rig.observe("idle", 1, rig.now)

	reads := 0
	rig.q.hk.job = func(_ context.Context, job string) (map[string]any, error) {
		reads++
		return map[string]any{"state": "spawned", "pane_id": panes[job], "harness": "codex"}, nil
	}

	m, err := newStallSafetyManager(rig.q, rig.manager, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	m.busy = func(context.Context, HubActiveJob, HerdrAgentState) (bool, error) { return false, nil }
	m.session = func(_ context.Context, pane, kind string) fleet.HarnessSession {
		return fleet.HarnessSession{Kind: kind, ID: sessions[pane], Source: "herdr:codex"}
	}
	m.read = func(context.Context, string) (string, error) { return "screen", nil }
	for pass := range 2 {
		if err := m.scan(context.Background(), agents, rig.now.Add(time.Duration(pass)*time.Minute)); err != nil {
			t.Fatal(err)
		}
		want := stallSafetyConfirmPerPass * (pass + 1)
		if pass == 1 {
			want = len(agents)
		}
		if reads != want {
			t.Fatalf("pass %d read %d hk job records, want %d", pass, reads, want)
		}
	}
}
