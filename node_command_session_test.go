package panewire

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// fakeHerdrBySession points node commands at a fake herdr that lists one agent only when it
// runs in session "fleet", the way a real herdr scopes `agent list` by
// HERDR_SESSION.
func fakeHerdrBySession(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell fake needs a POSIX shell")
	}
	bin := t.TempDir()
	fleetList := nodeAgentList(nodeAgentJSON("wR:p11", "job-a", "working", "/work/repo-a"))
	script := "#!/bin/sh\nif [ \"$HERDR_SESSION\" = fleet ]; then printf '%s' '" + fleetList + "'; else printf '%s' '" + nodeAgentList() + "'; fi\n"
	fake := filepath.Join(bin, "herdr")
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	// An absolute path, not PATH: a real herdr must never run in a test
	// (one did and hung a full run for minutes waiting on session "fleet").
	previous := nodeHerdrExecutable
	nodeHerdrExecutable = fake
	t.Cleanup(func() { nodeHerdrExecutable = previous })
	t.Setenv("HERDR_SESSION", "default") // the daemon's own session
}

// A node whose spawns use herdr session "fleet" reads its panes there: the
// hub's post-landing pane.read must find a landed pane, not report it gone
// (mac-work rc 75 reconciling, hk 2966).
func TestNodeCommandsReadTheSpawnSession(t *testing.T) {
	fakeHerdrBySession(t)
	config := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(config, []byte(`{"cwd_map":{"repo-a":"/work/repo-a"},"workspace":"w","herdr_session":"fleet","enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: config}
	outcome, row, err := client.nodePaneMatch(boundedTestContext(t), "wR:p11", "job-a", "", "/work/repo-a", false)
	if err != nil || outcome != "" || row.PaneID != "wR:p11" {
		t.Fatalf("pane in the spawn session: outcome=%q row=%+v err=%v", outcome, row, err)
	}
}

// Without spawn.json the node reads the default session, as before.
func TestNodeCommandsDefaultSessionWithoutSpawnConfig(t *testing.T) {
	fakeHerdrBySession(t)
	client := &HubClient{spawnConfigPath: filepath.Join(t.TempDir(), "missing.json")}
	if got := client.nodeSpawnSession(); got != hubSpawnDefaultSession {
		t.Fatalf("session=%q", got)
	}
	outcome, _, err := client.nodePaneMatch(boundedTestContext(t), "wR:p11", "job-a", "", "/work/repo-a", false)
	if err != nil || outcome != nodePaneGone {
		t.Fatalf("default session must not see the fleet pane: outcome=%q err=%v", outcome, err)
	}
}

// boundedTestContext fails fast instead of hanging if the fake is bypassed.
func boundedTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
