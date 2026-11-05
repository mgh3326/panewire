package panewire

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// fakeHerdrBySession puts a herdr on PATH that lists one agent only when it
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
	if err := os.WriteFile(filepath.Join(bin, "herdr"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
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
	outcome, row, err := client.nodePaneMatch(t.Context(), "wR:p11", "job-a", "", "/work/repo-a", false)
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
	outcome, _, err := client.nodePaneMatch(t.Context(), "wR:p11", "job-a", "", "/work/repo-a", false)
	if err != nil || outcome != nodePaneGone {
		t.Fatalf("default session must not see the fleet pane: outcome=%q err=%v", outcome, err)
	}
}
