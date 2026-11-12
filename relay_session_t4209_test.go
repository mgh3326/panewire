package panewire

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// fakeFleetHerdr creates a fake herdr script that only answers when
// HERDR_SESSION matches wantSession (e.g. "fleet"). If HERDR_SESSION does not
// match, it fails with exit status 1.
func fakeFleetHerdr(t *testing.T, dir, wantSession string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell fake needs a POSIX shell")
	}
	script := `#!/bin/sh
d='` + dir + `'
if [ "$HERDR_SESSION" != "` + wantSession + `" ]; then
  echo "herdr: wrong session: $HERDR_SESSION (want ` + wantSession + `)" >> "$d/errors"
  exit 1
fi
echo "$2" >> "$d/calls"
case "$2" in
get)
  printf '{"result":{"agent":{"agent":"codex","agent_status":"idle","name":"` + t3276Worker + `","pane_id":"w1:pW"}}}\n' ;;
list)
  printf '{"result":{"agents":[{"agent":"codex","agent_status":"idle","name":"` + t3276Worker + `","pane_id":"w1:pW","focused":false}]}}\n' ;;
prompt)
  printf '%s' "$4" > "$d/typed"
  touch "$d/prompted" ;;
close)
  echo "closed" > "$d/closed" ;;
read)
  src=visible; lines=60; prev=
  for a in "$@"; do [ "$prev" = "--source" ] && src=$a; [ "$prev" = "--lines" ] && lines=$a; prev=$a; done
  if [ -f "$d/prompted" ]; then
    typed=$(cat "$d/typed")
    echo "output before"
    echo "$typed"
  else
    echo "Ask Codex to do anything"
  fi ;;
esac
`
	binary := filepath.Join(dir, "herdr")
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	previous := nodeHerdrExecutable
	nodeHerdrExecutable = binary
	t.Cleanup(func() { nodeHerdrExecutable = previous })
}

func setupSpawnedJobSession(t *testing.T, inbox, jobID, paneID, session string) {
	t.Helper()
	eventsDir := filepath.Join(inbox, "jobs", jobID, "events")
	if err := os.MkdirAll(eventsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{
		"pane_id":       paneID,
		"herdr_session": session,
		"label":         jobID,
		"role":          "worker",
	}
	ev := map[string]any{
		"kind":       "job.spawned",
		"payload":    payload,
		"created_at": "2026-11-12T00:00:00Z",
	}
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(eventsDir, "0001-job.spawned.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// Desired 2: A test with a fake herdr that only answers in session "fleet":
// delivery to a job spawned there reads and sends successfully; red on the base.
func TestRelayDeliveryFleetSession_t4209(t *testing.T) {
	fakeDir := t.TempDir()
	fakeFleetHerdr(t, fakeDir, "fleet")
	t.Setenv("HERDR_SESSION", "default") // daemon's own host environment is default

	inbox := t.TempDir()
	setupSpawnedJobSession(t, inbox, "job-fleet-1", "w1:pW", "fleet")

	spawnJSON := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(spawnJSON, []byte(`{"cwd_map":{"repo-a":"/work/repo-a"},"workspace":"w","herdr_session":"fleet","enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}

	rig := t3276Start(t)
	rig.node.jobsInboxRoot = inbox
	rig.node.spawnConfigPath = spawnJSON

	directive := rig.send("ev-fleet-1", "findings", "[mergewake] findings for worker")
	directive.JobID = "job-fleet-1"
	rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.delivered") != 1 {
		t.Fatalf("expected relay.delivered in session fleet, got kinds=%v payloads=%v", kinds, payloads)
	}
	if t3276Count(kinds, "relay.dropped") != 0 {
		t.Fatalf("unexpected relay.dropped: %v", payloads["relay.dropped"])
	}
}

// Desired 3: Hosts whose session is "default" behave as today (preservation test).
func TestRelayDeliveryDefaultSessionPreservation_t4209(t *testing.T) {
	fakeDir := t.TempDir()
	fakeFleetHerdr(t, fakeDir, "default")
	t.Setenv("HERDR_SESSION", "default")

	inbox := t.TempDir()
	setupSpawnedJobSession(t, inbox, "job-default-1", "w1:pW", "default")

	rig := t3276Start(t)
	rig.node.jobsInboxRoot = inbox

	directive := rig.send("ev-default-1", "findings", "[mergewake] findings for worker")
	directive.JobID = "job-default-1"
	rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.delivered") != 1 {
		t.Fatalf("expected relay.delivered in session default, got kinds=%v payloads=%v", kinds, payloads)
	}
	if t3276Count(kinds, "relay.dropped") != 0 {
		t.Fatalf("unexpected relay.dropped: %v", payloads["relay.dropped"])
	}
}

// Desired 1: Job's recorded session overrides node's configured session.
func TestRelayDeliveryJobSessionOverridesNode_t4209(t *testing.T) {
	fakeDir := t.TempDir()
	fakeFleetHerdr(t, fakeDir, "fleet")
	t.Setenv("HERDR_SESSION", "default")

	inbox := t.TempDir()
	setupSpawnedJobSession(t, inbox, "job-override-1", "w1:pW", "fleet")

	spawnJSON := filepath.Join(t.TempDir(), "spawn.json")
	// Node is explicitly configured with "default" session, but the job's receipt specifies "fleet".
	if err := os.WriteFile(spawnJSON, []byte(`{"cwd_map":{"repo-a":"/work/repo-a"},"workspace":"w","herdr_session":"default","enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}

	rig := t3276Start(t)
	rig.node.jobsInboxRoot = inbox
	rig.node.spawnConfigPath = spawnJSON

	directive := rig.send("ev-override-1", "findings", "[mergewake] findings for worker")
	directive.JobID = "job-override-1"
	rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.delivered") != 1 {
		t.Fatalf("expected relay.delivered in session fleet, got kinds=%v payloads=%v", kinds, payloads)
	}
	if t3276Count(kinds, "relay.dropped") != 0 {
		t.Fatalf("unexpected relay.dropped: %v", payloads["relay.dropped"])
	}
}

// Desired 1: Every herdr call the node daemon makes for a job pane (read, close)
// uses that job's recorded herdr_session.
func TestNodeCommandsUseJobSession_t4209(t *testing.T) {
	fakeDir := t.TempDir()
	fakeFleetHerdr(t, fakeDir, "fleet")
	t.Setenv("HERDR_SESSION", "default")

	inbox := t.TempDir()
	setupSpawnedJobSession(t, inbox, "job-cmd-1", "w1:pW", "fleet")

	client := &HubClient{
		jobsInboxRoot: inbox,
	}

	// pane.read
	res, code := client.nodePaneRead(t.Context(), nodePaneReadArgs{
		PaneID: "w1:pW",
		Name:   t3276Worker,
	})
	if code != "" {
		t.Fatalf("nodePaneRead failed with code: %s", code)
	}
	if res == nil {
		t.Fatal("nodePaneRead returned nil result")
	}

	// pane.close
	closeRes, code := client.nodePaneClose(t.Context(), nodePaneCloseArgs{
		PaneID:      "w1:pW",
		Name:        t3276Worker,
		AgentStatus: "idle",
	})
	if code != "" {
		t.Fatalf("nodePaneClose failed with code: %s", code)
	}
	if closeRes == nil {
		t.Fatal("nodePaneClose returned nil result")
	}
}

// Review Finding 1: Destination pane session wins over emitting job session.
func TestRelayDeliveryPaneSessionWinsOverEmitterSession_t4209(t *testing.T) {
	fakeDir := t.TempDir()
	fakeFleetHerdr(t, fakeDir, "fleet")
	t.Setenv("HERDR_SESSION", "default")

	inbox := t.TempDir()
	// Emitting job is in session "default"
	setupSpawnedJobSession(t, inbox, "job-emitter-1", "w1:pE", "default")
	// Target worker pane is in session "fleet"
	setupSpawnedJobSession(t, inbox, "job-target-1", "w1:pW", "fleet")

	spawnJSON := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(spawnJSON, []byte(`{"cwd_map":{"repo-a":"/work/repo-a"},"workspace":"w","herdr_session":"default","enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}

	rig := t3276Start(t)
	rig.node.jobsInboxRoot = inbox
	rig.node.spawnConfigPath = spawnJSON

	directive := rig.send("ev-emitter-1", "findings", "[mergewake] findings for worker")
	// Directive's JobID is the emitting job!
	directive.JobID = "job-emitter-1"
	rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.delivered") != 1 {
		t.Fatalf("expected relay.delivered in target session fleet, got kinds=%v payloads=%v", kinds, payloads)
	}
	if t3276Count(kinds, "relay.dropped") != 0 {
		t.Fatalf("unexpected relay.dropped: %v", payloads["relay.dropped"])
	}
}

// Review Finding 2: QuotaStopManager q.read and q.send use the target pane's session.
func TestQuotaStopUsesPaneHerdrSession_t4209(t *testing.T) {
	fakeDir := t.TempDir()
	fakeFleetHerdr(t, fakeDir, "fleet")
	t.Setenv("HERDR_SESSION", "default")

	inbox := t.TempDir()
	setupSpawnedJobSession(t, inbox, "job-quota-1", "w1:pW", "fleet")

	store := NewMemoryStore(t)
	t.Cleanup(func() { _ = store.Close() })

	q, err := newQuotaStopManager(store, inbox, true, slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	screen, err := q.read(t.Context(), "w1:pW")
	if err != nil {
		t.Fatalf("q.read failed: %v", err)
	}
	if !strings.Contains(screen, "Ask Codex") {
		t.Fatalf("unexpected screen content: %q", screen)
	}
}

// Review Finding 3: relaySessionFromContext resolves pane's session when unset on ctx.
func TestRelaySessionFromContextResolvesPane_t4209(t *testing.T) {
	fakeDir := t.TempDir()
	fakeFleetHerdr(t, fakeDir, "fleet")
	t.Setenv("HERDR_SESSION", "default")

	inbox := t.TempDir()
	setupSpawnedJobSession(t, inbox, "job-pane-1", "w1:pW", "fleet")

	client := &HubClient{
		jobsInboxRoot: inbox,
	}
	// Initializing or resolving on client sets activePaneSessionResolver
	client.herdrSessionFor("", "w1:pW")

	ansi, err := relayReadPaneANSI(t.Context(), "w1:pW")
	if err != nil {
		t.Fatalf("relayReadPaneANSI failed: %v", err)
	}
	if !strings.Contains(ansi, "Ask Codex") {
		t.Fatalf("unexpected ansi content: %q", ansi)
	}
}

// Review Finding 5 & 6: nodeJobHerdrSessionByPane prefers live un-ended jobs and binds strictly to matching event.
func TestNodeJobHerdrSessionByPanePrefersLiveAndNewest_t4209(t *testing.T) {
	inbox := t.TempDir()

	// 1. Stale completed job referencing pane w1:pReuse in session "old-session"
	setupSpawnedJobSession(t, inbox, "job-1-ended", "w1:pReuse", "old-session")
	endedFile := filepath.Join(inbox, "jobs", "job-1-ended", "events", "0002-job.completed.json")
	if err := os.WriteFile(endedFile, []byte(`{"kind":"job.completed","created_at":"2026-11-12T01:00:00Z"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	// 2. Active live job referencing same pane w1:pReuse in session "live-session"
	setupSpawnedJobSession(t, inbox, "job-2-live", "w1:pReuse", "live-session")

	session := nodeJobHerdrSessionByPaneFromRoot(inbox, "w1:pReuse")
	if session != "live-session" {
		t.Fatalf("expected live-session, got %q", session)
	}

	// 3. Multi-event job: ensure session is tied to the matching event, not the last event
	eventsDir := filepath.Join(inbox, "jobs", "job-3-multi", "events")
	if err := os.MkdirAll(eventsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	ev1 := `{"seq":1,"created_at":"2026-11-12T02:00:00Z","kind":"job.spawned","payload":{"pane_id":"w1:pFirst","herdr_session":"sess-first"}}`
	ev2 := `{"seq":2,"created_at":"2026-11-12T02:05:00Z","kind":"job.spawned","payload":{"pane_id":"w1:pSecond","herdr_session":"sess-second"}}`
	if err := os.WriteFile(filepath.Join(eventsDir, "0001-job.spawned.json"), []byte(ev1), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(eventsDir, "0002-job.spawned.json"), []byte(ev2), 0o600); err != nil {
		t.Fatal(err)
	}

	if got := nodeJobHerdrSessionByPaneFromRoot(inbox, "w1:pFirst"); got != "sess-first" {
		t.Fatalf("expected sess-first, got %q", got)
	}
	if got := nodeJobHerdrSessionByPaneFromRoot(inbox, "w1:pSecond"); got != "sess-second" {
		t.Fatalf("expected sess-second, got %q", got)
	}

	// 4. Cache test on client
	client := &HubClient{jobsInboxRoot: inbox}
	if got := client.nodeJobHerdrSessionByPane("w1:pSecond"); got != "sess-second" {
		t.Fatalf("expected sess-second, got %q", got)
	}
	// Modify event on disk; cache should still return "sess-second" within TTL
	if err := os.Remove(filepath.Join(eventsDir, "0002-job.spawned.json")); err != nil {
		t.Fatal(err)
	}
	if got := client.nodeJobHerdrSessionByPane("w1:pSecond"); got != "sess-second" {
		t.Fatalf("expected cached sess-second, got %q", got)
	}
}

// Review Finding 7: herdrExecutable honors HERDR_BIN when nodeHerdrExecutable is unset.
func TestHerdrExecutableHonorsHerdrBin_t4209(t *testing.T) {
	nodeHerdrExecutable = ""
	t.Setenv("HERDR_BIN", "/custom/bin/herdr")
	if got := herdrExecutable(); got != "/custom/bin/herdr" {
		t.Fatalf("expected /custom/bin/herdr, got %q", got)
	}
}
