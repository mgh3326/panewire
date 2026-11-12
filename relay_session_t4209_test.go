package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
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
