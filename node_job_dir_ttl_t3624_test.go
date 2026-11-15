package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/fleetevent"
)

// hk 3624: the node's sweep loop runs the job dir TTL only when the hosts
// config sets [reap] job_dir_ttl; unset, a deploy changes nothing. The jobs
// root here is under t.TempDir(), never the real ~/work/herdr-inbox.

type ttlNodeJobs struct{ records map[string]map[string]any }

func (s ttlNodeJobs) Claim(context.Context, string, string, string) error { return nil }
func (s ttlNodeJobs) Put(context.Context, string, map[string]any) error   { return nil }
func (s ttlNodeJobs) Get(_ context.Context, id string) (map[string]any, error) {
	if rec, ok := s.records[id]; ok {
		return rec, nil
	}
	return nil, fleet.ErrJobNotFound
}

type ttlNodeHerdr struct{}

func (ttlNodeHerdr) Run(_ context.Context, c fleet.Command) fleet.Result {
	if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
		return fleet.Result{Out: `{"result":{"agents":[]}}`}
	}
	if c.Name == "herdr" && strings.Join(c.Args, " ") == "pane list" {
		return fleet.Result{Out: `{"result":{"panes":[]}}`}
	}
	return fleet.Result{Code: 1, Err: "unexpected command"}
}

// ttlNodeApp is a fleet app over a temp jobs root holding one job dir that
// every gate condition passes for, 73 h after its last write.
func ttlNodeApp(t *testing.T) (*fleet.App, string, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil || !strings.HasPrefix(root, tmp+string(os.PathSeparator)) {
		t.Fatalf("test root %s is not under the temp dir %s (%v)", root, tmp, err)
	}
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(root, "absent-hk.env"))
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	jobs := filepath.Join(root, "work", "herdr-inbox", "jobs")
	dir := filepath.Join(jobs, "j1")
	if err := os.MkdirAll(filepath.Join(dir, "events"), 0700); err != nil {
		t.Fatal(err)
	}
	for i, ev := range []struct {
		kind    string
		payload map[string]any
	}{
		{"job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "role": "implementer"}},
		{"job.spawned", map[string]any{"pane_id": "w:j1", "label": "j1", "herdr_session": "default"}},
		{"job.completed", map[string]any{}},
		{"job.reaped", map[string]any{"pane_id": "w:j1"}},
	} {
		b, _ := json.Marshal(map[string]any{"kind": ev.kind, "payload": ev.payload, "created_at": base.Add(time.Duration(i)).Format(time.RFC3339Nano)})
		if err := os.WriteFile(filepath.Join(dir, "events", fmt.Sprintf("%04d-%s.json", i, ev.kind)), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "brief.md"), []byte("brief\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := filepath.WalkDir(dir, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Chtimes(path, base, base)
	}); err != nil {
		t.Fatal(err)
	}
	app := &fleet.App{
		Runner:   ttlNodeHerdr{},
		Jobs:     jobs,
		Home:     root,
		Out:      io.Discard,
		Err:      io.Discard,
		Now:      func() time.Time { return base.Add(73 * time.Hour) },
		Bins:     map[string]string{},
		JobStore: ttlNodeJobs{records: map[string]map[string]any{"j1": {"job_id": "j1", "state": "completed"}}},
	}
	return app, dir, filepath.Join(root, ".local", "state", "panewire", "events", fleetevent.FileName)
}

// fleetEvents reads the state dir's fleet event file; none is an empty list.
func fleetEvents(t *testing.T, path string) []fleetevent.Event {
	t.Helper()
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []fleetevent.Event
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var ev fleetevent.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("fleet event line %q: %v", line, err)
		}
		out = append(out, ev)
	}
	return out
}

func TestNodeJobDirTTLOffWithoutConfig(t *testing.T) {
	app, dir, state := ttlNodeApp(t)
	client := newTaskPaneSweepClient(t, time.Minute)
	client.expireJobDirs(context.Background(), app, &fleet.JobDirTTL{})
	if _, err := os.Stat(dir); err != nil || len(fleetEvents(t, state)) != 0 {
		t.Fatalf("an unset [reap] job_dir_ttl touched the job dir: %v events=%+v", err, fleetEvents(t, state))
	}
}

func TestNodeJobDirTTLDeletesWhenConfigured(t *testing.T) {
	app, dir, state := ttlNodeApp(t)
	app.Config.Reap = map[string]any{"job_dir_ttl": "72h"}
	client := newTaskPaneSweepClient(t, time.Minute)
	expiry := &fleet.JobDirTTL{}
	client.expireJobDirs(context.Background(), app, expiry)
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the eligible dir survived the node pass: %v", err)
	}
	// The record lands in the state dir, not in the deleted job dir.
	if events := fleetEvents(t, state); len(events) != 1 || events[0].Kind != "job_dir_expired" || events[0].Job != "j1" || events[0].Reason != "confirmed" || !strings.HasPrefix(events[0].Detail, "bytes=") {
		t.Fatalf("fleet events = %+v, want one job_dir_expired for j1", events)
	}
	if !client.nodeCommandMu.TryLock() {
		t.Fatal("the node command lock stayed held after the pass")
	}
	client.nodeCommandMu.Unlock()
}

// A longer TTL than the dir's age keeps it.
func TestNodeJobDirTTLHonoursTheConfiguredAge(t *testing.T) {
	app, dir, _ := ttlNodeApp(t)
	app.Config.Reap = map[string]any{"job_dir_ttl": "4d"}
	client := newTaskPaneSweepClient(t, time.Minute)
	client.expireJobDirs(context.Background(), app, &fleet.JobDirTTL{})
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("a 73h-old dir was deleted under a 4d TTL: %v", err)
	}
}

// hk 4670: the TTL loop is the daemon's own hourly loop, not a step of the
// pane sweep — a node with --task-pane-sweep-interval 0 still runs it, and
// a node with no jobs root runs nothing.
func TestNodeJobDirTTLLoopRunsWithoutPaneSweep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTaskPaneSweepClient(t, 0)
	ticks := make(chan time.Time)
	var passes atomic.Int32
	client.jobDirTTLTicks = ticks
	client.jobDirTTLPass = func(context.Context) { passes.Add(1) }
	done := make(chan struct{})
	go func() { client.runJobDirTTL(ctx); close(done) }()
	ticks <- time.Now()
	eventually(t, time.Second, "job dir ttl pass", func() bool { return passes.Load() == 1 })
	cancel()
	eventually(t, time.Second, "ttl loop shutdown", func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})

	client = newTaskPaneSweepClient(t, 0)
	client.jobsInboxRoot = ""
	client.jobDirTTLTicks = make(chan time.Time)
	client.jobDirTTLPass = func(context.Context) { t.Error("ttl pass ran without a jobs root") }
	finished := make(chan struct{})
	go func() { client.runJobDirTTL(context.Background()); close(finished) }()
	eventually(t, time.Second, "ttl loop returns without a jobs root", func() bool {
		select {
		case <-finished:
			return true
		default:
			return false
		}
	})
}
