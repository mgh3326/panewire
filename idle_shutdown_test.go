package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type idleShutdownFixture struct {
	sessions    []HerdrAgentState
	sessionsErr error
	jobs        []HubActiveJob
	jobsErr     error
	procs       []string
	procsErr    error
	users       int
	usersErr    error
	keepAwake   bool
	shutdownErr error
	shutdowns   int
	writeErr    error
	records     []emitRecord
	enqueued    []hubScannedRelayEvent
	now         time.Time
}

func (fake *idleShutdownFixture) deps() idleShutdownDeps {
	return idleShutdownDeps{
		machineID: "desktop",
		sessions: func(context.Context) ([]HerdrAgentState, error) {
			return append([]HerdrAgentState(nil), fake.sessions...), fake.sessionsErr
		},
		jobLeases: func(context.Context) ([]HubActiveJob, error) {
			return append([]HubActiveJob(nil), fake.jobs...), fake.jobsErr
		},
		procs: func(context.Context) ([]string, error) {
			return append([]string(nil), fake.procs...), fake.procsErr
		},
		interactive: func(context.Context) (int, error) {
			return fake.users, fake.usersErr
		},
		keepAwake: func() bool { return fake.keepAwake },
		writeRecord: func(root string, record emitRecord) (string, error) {
			if fake.writeErr != nil {
				return "", fake.writeErr
			}
			fake.records = append(fake.records, record)
			return filepath.Join(root, "events-lane", record.EventID+".json"), nil
		},
		enqueue: func(event hubScannedRelayEvent) bool {
			fake.enqueued = append(fake.enqueued, event)
			return true
		},
		shutdown: func(context.Context) error {
			fake.shutdowns++
			return fake.shutdownErr
		},
		now: func() time.Time { return fake.now },
	}
}

func newIdleShutdownFixture(t *testing.T, fake *idleShutdownFixture) *idleShutdownManager {
	t.Helper()
	manager := newIdleShutdownManager(
		IdleShutdownConfig{Enabled: true, After: 30 * time.Minute, PollInterval: time.Minute, Command: []string{"fixture", "poweroff"}, Lane: "operator-desk"},
		t.TempDir(), fake.deps(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	manager.cfg.NotifySettle = 0
	return manager
}

func TestIdleShutdownBlockedReasons(t *testing.T) {
	for name, mutate := range map[string]func(*idleShutdownFixture){
		"clear":        func(*idleShutdownFixture) {},
		"working pane": func(f *idleShutdownFixture) { f.sessions = []HerdrAgentState{{PaneID: "p1", Status: "working"}} },
		"blocked pane": func(f *idleShutdownFixture) { f.sessions = []HerdrAgentState{{PaneID: "p1", Status: "blocked"}} },
		"idle done pane": func(f *idleShutdownFixture) {
			f.sessions = []HerdrAgentState{{PaneID: "p1", Status: "done"}, {PaneID: "p2", Status: "idle"}}
		},
		"idle holding job": func(f *idleShutdownFixture) {
			f.sessions = []HerdrAgentState{{PaneID: "p1", Status: "idle"}}
			f.jobs = []HubActiveJob{{JobID: "job-a", Pane: "p1"}}
		},
		"idle holding name": func(f *idleShutdownFixture) {
			f.sessions = []HerdrAgentState{{PaneID: "p1", Status: "idle", AgentName: "wrk-a"}}
			f.jobs = []HubActiveJob{{JobID: "job-a", AgentLabel: "wrk-a"}}
		},
		"sessions error": func(f *idleShutdownFixture) { f.sessionsErr = errors.New("herdr down") },
		"jobs error":     func(f *idleShutdownFixture) { f.jobsErr = errors.New("inbox gone") },
		"job leases":     func(f *idleShutdownFixture) { f.jobs = []HubActiveJob{{JobID: "job-a"}} },
		"ci runner": func(f *idleShutdownFixture) {
			f.procs = []string{"/usr/bin/sleep 5", "/opt/runner/bin/Runner.Worker run"}
		},
		"procs error":  func(f *idleShutdownFixture) { f.procsErr = errors.New("ps failed") },
		"user session": func(f *idleShutdownFixture) { f.users = 1 },
		"users error":  func(f *idleShutdownFixture) { f.usersErr = errors.New("who failed") },
		"keep awake":   func(f *idleShutdownFixture) { f.keepAwake = true },
	} {
		fake := &idleShutdownFixture{procs: []string{"/usr/bin/sleep 5"}}
		mutate(fake)
		manager := newIdleShutdownFixture(t, fake)
		reason := manager.blockedReason(context.Background())
		want := map[string]string{
			"clear":             "",
			"working pane":      "pane_active",
			"blocked pane":      "pane_active",
			"idle done pane":    "",
			"idle holding job":  "pane_holding_job",
			"idle holding name": "pane_holding_job",
			"sessions error":    "sessions_unavailable",
			"jobs error":        "jobs_unavailable",
			"job leases":        "jobs_active",
			"ci runner":         "ci_runner",
			"procs error":       "procs_unavailable",
			"user session":      "user_session",
			"users error":       "users_unavailable",
			"keep awake":        "keep_awake",
		}[name]
		if reason != want {
			t.Fatalf("%s reason=%q want %q", name, reason, want)
		}
	}
}

func TestIdleShutdownCommitsOnceAfterWindow(t *testing.T) {
	start := time.Date(2026, 9, 5, 5, 0, 0, 0, time.UTC)
	fake := &idleShutdownFixture{procs: []string{"/usr/bin/sleep 5"}, now: start}
	manager := newIdleShutdownFixture(t, fake)
	manager.tick(context.Background())
	if fake.shutdowns != 0 {
		t.Fatal("first clear observation fired the command")
	}
	fake.now = start.Add(29 * time.Minute)
	manager.tick(context.Background())
	if fake.shutdowns != 0 || len(fake.records) != 0 || len(fake.enqueued) != 0 {
		t.Fatal("window shorter than After fired the command")
	}
	fake.now = start.Add(30 * time.Minute)
	manager.tick(context.Background())
	if fake.shutdowns != 1 || len(fake.records) != 1 || len(fake.enqueued) != 1 {
		t.Fatalf("commit did not emit once and fire once: shutdowns=%d records=%d enqueued=%d", fake.shutdowns, len(fake.records), len(fake.enqueued))
	}
	fake.now = start.Add(time.Hour)
	manager.tick(context.Background())
	manager.tick(context.Background())
	if fake.shutdowns != 1 || len(fake.records) != 1 {
		t.Fatal("committed manager fired a second time")
	}
}

func TestIdleShutdownWindowResetsOnBusy(t *testing.T) {
	start := time.Date(2026, 9, 5, 6, 0, 0, 0, time.UTC)
	fake := &idleShutdownFixture{procs: []string{"/usr/bin/sleep 5"}, now: start}
	manager := newIdleShutdownFixture(t, fake)
	manager.tick(context.Background())
	fake.now = start.Add(20 * time.Minute)
	manager.tick(context.Background())
	// Activity inside the window discards it; a fresh window is required.
	fake.jobs = []HubActiveJob{{JobID: "job-a"}}
	fake.now = start.Add(25 * time.Minute)
	manager.tick(context.Background())
	fake.jobs = nil
	fake.now = start.Add(40 * time.Minute)
	manager.tick(context.Background())
	if fake.shutdowns != 0 {
		t.Fatal("interrupted window fired the command")
	}
	fake.now = start.Add(40*time.Minute + 30*time.Minute)
	manager.tick(context.Background())
	if fake.shutdowns != 1 {
		t.Fatal("full fresh window did not fire")
	}
}

func TestIdleShutdownCommandFailureRestartsWindow(t *testing.T) {
	start := time.Date(2026, 9, 5, 7, 0, 0, 0, time.UTC)
	fake := &idleShutdownFixture{procs: []string{"/usr/bin/sleep 5"}, now: start, shutdownErr: errors.New("refused")}
	manager := newIdleShutdownFixture(t, fake)
	manager.tick(context.Background())
	fake.now = start.Add(30 * time.Minute)
	manager.tick(context.Background())
	if fake.shutdowns != 1 {
		t.Fatal("committed decision did not run the command")
	}
	// A refused command restarts the window, so the next attempt needs another
	// full idle stretch and emits a fresh event.
	fake.now = start.Add(59 * time.Minute)
	manager.tick(context.Background())
	if fake.shutdowns != 1 {
		t.Fatal("failed command retried inside the restarted window")
	}
	fake.shutdownErr = nil
	fake.now = start.Add(60 * time.Minute)
	manager.tick(context.Background())
	if fake.shutdowns != 2 || len(fake.records) != 2 || len(fake.enqueued) != 2 {
		t.Fatalf("retry after fresh window: shutdowns=%d records=%d", fake.shutdowns, len(fake.records))
	}
}

func TestIdleShutdownLaneEventShape(t *testing.T) {
	start := time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC)
	fake := &idleShutdownFixture{procs: []string{"/usr/bin/sleep 5"}, now: start}
	manager := newIdleShutdownFixture(t, fake)
	manager.tick(context.Background())
	fake.now = start.Add(30 * time.Minute)
	manager.tick(context.Background())
	if len(fake.records) != 1 || len(fake.enqueued) != 1 {
		t.Fatalf("decision did not produce exactly one event: %+v", fake.records)
	}
	record, event := fake.records[0], fake.enqueued[0]
	if record.Type != "lane.event" || record.OwnerLane != "operator-desk" || record.EventID == "" || record.EventID != event.EventID || record.Epoch != 1 {
		t.Fatalf("record=%+v event=%+v", record, event)
	}
	if event.Kind != "lane.event" || event.OwnerLane != "operator-desk" || event.JobID != laneEventTransportID("operator-desk", record.EventID) {
		t.Fatalf("enqueued event=%+v", event)
	}
	var text map[string]any
	if json.Unmarshal([]byte(record.Text), &text) != nil || text["kind"] != "idle_shutdown" || text["machine"] != "desktop" || text["idle_seconds"] != float64(1800) {
		t.Fatalf("event text=%q", record.Text)
	}
}

func TestIdleShutdownRunnerProc(t *testing.T) {
	for line, want := range map[string]bool{
		"/opt/runner/bin/Runner.Worker run": true,
		"Runner.Worker":                     true,
		"vim Runner.Worker.log":             false,
		"/usr/bin/SomeRunner.Worker":        false,
		"Runner.Listener":                   false,
		"":                                  false,
	} {
		if got := idleShutdownRunnerProc(line); got != want {
			t.Fatalf("%q got=%t want=%t", line, got, want)
		}
	}
}

func TestIdleShutdownKeepAwake(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "keep-awake")
	if idleShutdownKeepAwake(marker) {
		t.Fatal("absent marker counted as present")
	}
	// A stat failure that is not a plain miss (ENOTDIR) counts as present.
	blocked := filepath.Join(root, "file", "keep-awake")
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if !idleShutdownKeepAwake(blocked) {
		t.Fatal("unreadable marker path did not fail toward awake")
	}
	if err := os.WriteFile(marker, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if !idleShutdownKeepAwake(marker) {
		t.Fatal("present marker ignored")
	}
}

func TestDaemonIdleShutdownFlagValidation(t *testing.T) {
	root := t.TempDir()
	envPath := filepath.Join(root, "node.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=node-b\nHUB_TOKEN="+r6NodeBToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	hub := []string{"--socket", filepath.Join(root, "daemon.sock"), "--hub-url", "ws://fixture.invalid", "--hub-token-env", envPath}
	deps := daemonCLIDeps{AllowInsecureForTests: true}
	// 0 is off and needs no other flags.
	daemon, code, err := newDaemonForCLI([]string{"--socket", filepath.Join(root, "off.sock"), "--idle-shutdown", "0"}, deps)
	if err != nil || code != ExitOK || daemon.cfg.IdleShutdown.Enabled {
		t.Fatalf("--idle-shutdown 0: code=%d err=%v enabled=%t", code, err, daemon != nil && daemon.cfg.IdleShutdown.Enabled)
	}
	for name, args := range map[string][]string{
		"enabled no hub":     {"--idle-shutdown", "30m", "--idle-shutdown-command", "fixture poweroff"},
		"enabled no command": append(append([]string{}, hub...), "--idle-shutdown", "30m"),
		"bad lane":           append(append([]string{}, hub...), "--idle-shutdown", "30m", "--idle-shutdown-command", "fixture poweroff", "--idle-shutdown-lane", "BAD LANE"),
		"bad poll":           append(append([]string{}, hub...), "--idle-shutdown", "30m", "--idle-shutdown-command", "fixture poweroff", "--idle-shutdown-poll", "0"),
		"command alone":      {"--idle-shutdown-command", "fixture poweroff"},
	} {
		if _, code, err := newDaemonForCLI(args, deps); err == nil || code != ExitConditionInvalid {
			t.Fatalf("%s accepted: code=%d err=%v", name, code, err)
		}
	}
	daemon, code, err = newDaemonForCLI(append(append([]string{}, hub...), "--idle-shutdown", "30m", "--idle-shutdown-command", "sudo -n /usr/sbin/poweroff", "--idle-shutdown-keep-awake", filepath.Join(root, "marker")), deps)
	if err != nil || code != ExitOK || !daemon.cfg.IdleShutdown.Enabled || len(daemon.cfg.IdleShutdown.Command) != 3 || daemon.cfg.IdleShutdown.KeepAwakePath != filepath.Join(root, "marker") {
		t.Fatalf("valid idle shutdown config: code=%d err=%v cfg=%+v", code, err, daemon.cfg.IdleShutdown)
	}
}
