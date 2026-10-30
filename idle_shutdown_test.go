package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type idleShutdownFixture struct {
	mu          sync.Mutex
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
	failWrites  int
	writeCalls  int
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
			fake.mu.Lock()
			defer fake.mu.Unlock()
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
			fake.writeCalls++
			if fake.writeErr != nil {
				return "", fake.writeErr
			}
			if fake.writeCalls <= fake.failWrites {
				return "", errors.New("transient write failure")
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
		"unknown pane":      func(f *idleShutdownFixture) { f.sessions = []HerdrAgentState{{PaneID: "p1", Status: "unknown"}} },
		"unrecognised pane": func(f *idleShutdownFixture) { f.sessions = []HerdrAgentState{{PaneID: "p1", Status: "frobnicate"}} },
		"empty pane status": func(f *idleShutdownFixture) { f.sessions = []HerdrAgentState{{PaneID: "p1"}} },
		"done holding job": func(f *idleShutdownFixture) {
			f.sessions = []HerdrAgentState{{PaneID: "p1", Status: "done"}}
			f.jobs = []HubActiveJob{{JobID: "job-a", Pane: "p1"}}
		},
		"idle holding job": func(f *idleShutdownFixture) {
			f.sessions = []HerdrAgentState{{PaneID: "p1", Status: "idle"}}
			f.jobs = []HubActiveJob{{JobID: "job-a", Pane: "p1"}}
		},
		"idle holding name": func(f *idleShutdownFixture) {
			f.sessions = []HerdrAgentState{{PaneID: "p1", Status: "idle", AgentName: "agent-a"}}
			f.jobs = []HubActiveJob{{JobID: "job-a", AgentLabel: "agent-a"}}
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
			"unknown pane":      "pane_unknown",
			"unrecognised pane": "pane_unknown",
			"empty pane status": "pane_unknown",
			"done holding job":  "pane_holding_job",
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

func TestIdleShutdownPostSettleBlockAborts(t *testing.T) {
	start := time.Date(2026, 9, 5, 9, 0, 0, 0, time.UTC)
	fake := &idleShutdownFixture{procs: []string{"/usr/bin/sleep 5"}, now: start}
	manager := newIdleShutdownFixture(t, fake)
	manager.cfg.NotifySettle = time.Second
	manager.tick(context.Background())
	fake.now = start.Add(30 * time.Minute)
	// Work appears during the settle window: the commit emits the decision
	// record but the final probe vetoes the command itself.
	go func() {
		time.Sleep(50 * time.Millisecond)
		fake.mu.Lock()
		fake.jobs = []HubActiveJob{{JobID: "job-a"}}
		fake.mu.Unlock()
	}()
	manager.tick(context.Background())
	if fake.shutdowns != 0 || len(fake.enqueued) == 0 {
		t.Fatalf("post-settle blocker did not veto: shutdowns=%d enqueued=%d", fake.shutdowns, len(fake.enqueued))
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
		"/opt/runner/bin/Runner.Worker run":         true,
		"/opt/actions runner/bin/Runner.Worker run": true,
		"Runner.Worker":              true,
		"vim Runner.Worker.log":      false,
		"/usr/bin/SomeRunner.Worker": false,
		"Runner.Listener":            false,
		"":                           false,
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
	hub := []string{"--socket", filepath.Join(root, "daemon.sock"), "--herdr-socket", filepath.Join(root, "herdr.sock"), "--hub-url", "ws://fixture.invalid", "--hub-token-env", envPath}
	deps := daemonCLIDeps{AllowInsecureForTests: true}
	// 0 is off and needs no other flags.
	daemon, code, err := newDaemonForCLI([]string{"--socket", filepath.Join(root, "off.sock"), "--idle-shutdown", "0"}, deps)
	if err != nil || code != ExitOK || daemon.cfg.IdleShutdown.Enabled {
		t.Fatalf("--idle-shutdown 0: code=%d err=%v enabled=%t", code, err, daemon != nil && daemon.cfg.IdleShutdown.Enabled)
	}
	for name, args := range map[string][]string{
		"enabled no hub":     {"--idle-shutdown", "30m", "--idle-shutdown-command", "fixture poweroff"},
		"enabled no herdr":   {"--socket", filepath.Join(root, "n.sock"), "--hub-url", "ws://fixture.invalid", "--hub-token-env", envPath, "--idle-shutdown", "30m", "--idle-shutdown-command", "fixture poweroff"},
		"negative duration":  {"--idle-shutdown", "-30m"},
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

// A transient record-write failure gets exactly one retry before the node
// accepts the gap and proceeds; the retry success still reaches the relay.
func TestIdleShutdownEmitRetry(t *testing.T) {
	start := time.Date(2026, 9, 5, 9, 30, 0, 0, time.UTC)
	fake := &idleShutdownFixture{procs: []string{"/usr/bin/sleep 5"}, now: start, failWrites: 1}
	manager := newIdleShutdownFixture(t, fake)
	manager.tick(context.Background())
	fake.now = start.Add(30 * time.Minute)
	manager.tick(context.Background())
	if fake.writeCalls != 2 || len(fake.records) != 1 || len(fake.enqueued) != 1 || fake.shutdowns != 1 {
		t.Fatalf("emit retry: writeCalls=%d records=%d enqueued=%d shutdowns=%d", fake.writeCalls, len(fake.records), len(fake.enqueued), fake.shutdowns)
	}
}

// Unreadable or unparsable job event records have unknown lease state: they
// count as an active lease instead of vanishing from the scan.
func TestIdleShutdownJobLeasesUnreadable(t *testing.T) {
	root := t.TempDir()
	// Legible but unclaimed: parseable JSON, no claim events -> no lease.
	legible := filepath.Join(root, "jobs", "job-legible", "events")
	if err := os.MkdirAll(legible, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legible, "1-note.json"), []byte(`{"kind":"note"}`), 0600); err != nil {
		t.Fatal(err)
	}
	leases, err := idleShutdownJobLeases(root, nil, context.Background())
	if err != nil || len(leases) != 0 {
		t.Fatalf("legible jobs: leases=%v err=%v", leases, err)
	}
	// Unparsable event JSON inside a job dir -> synthetic lease.
	bad := filepath.Join(root, "jobs", "job-bad", "events")
	if err := os.MkdirAll(bad, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, "2-broken.json"), []byte(`{truncated`), 0600); err != nil {
		t.Fatal(err)
	}
	leases, err = idleShutdownJobLeases(root, nil, context.Background())
	if err != nil || len(leases) != 1 || leases[0].JobID != "job-bad" {
		t.Fatalf("unparsable events: leases=%v err=%v", leases, err)
	}
	// A job dir with no readable events dir at all -> synthetic lease.
	if err := os.MkdirAll(filepath.Join(root, "jobs", "job-noevents"), 0700); err != nil {
		t.Fatal(err)
	}
	leases, err = idleShutdownJobLeases(root, nil, context.Background())
	if err != nil || len(leases) != 1 {
		t.Fatalf("missing events dir: leases=%v err=%v", leases, err)
	}
	// An existing but unreadable events dir -> synthetic lease.
	sealed := filepath.Join(root, "jobs", "job-sealed", "events")
	if err := os.MkdirAll(sealed, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sealed, 0000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sealed, 0700) })
	leases, err = idleShutdownJobLeases(root, nil, context.Background())
	if err != nil || len(leases) != 1 {
		t.Fatalf("unreadable events dir: leases=%v err=%v", leases, err)
	}
}

// agent.list entries herdr reports but this build cannot classify must reach
// the idle-shutdown probe as "unknown" rather than disappearing.
func TestHerdrAgentStatesAllKeepsUnclassifiable(t *testing.T) {
	agents := []map[string]any{
		{"pane_id": "ws:p1", "workspace_id": "ws", "name": "ok", "agent_status": "idle", "revision": 1, "state_change_seq": 1},
		{"pane_id": "ws:p2", "workspace_id": "ws", "name": "odd", "agent_status": "frobnicate", "revision": 2, "state_change_seq": 2},
		{"pane_id": "ws:p3\ninvalid", "workspace_id": "ws", "name": "bad", "agent_status": "idle", "revision": 3, "state_change_seq": 3},
	}
	socket := task268HerdrAgentListSocket(t, agents)
	herdr, err := NewHerdrClient(socket)
	if err != nil {
		t.Fatal(err)
	}
	strict, err := herdr.AgentStates(context.Background())
	if err != nil || len(strict) != 1 {
		t.Fatalf("AgentStates kept unclassifiable entries: states=%+v err=%v", strict, err)
	}
	herdr.Close()
	herdr, err = NewHerdrClient(socket)
	if err != nil {
		t.Fatal(err)
	}
	all, err := herdr.AgentStatesAll(context.Background())
	herdr.Close()
	if err != nil || len(all) != 3 {
		t.Fatalf("AgentStatesAll dropped entries: states=%+v err=%v", all, err)
	}
	if all[0].Status != "idle" || all[1].Status != "unknown" || all[1].PaneID != "ws:p2" || all[2].Status != "unknown" {
		t.Fatalf("AgentStatesAll classification: %+v", all)
	}
}
