package panewire

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// hk 4542: the observation classifies a devin pane from the devin CLI's own
// session record first, and logs the basis it used. The screen here shows no
// limit at all, so only the record can find the stop.
func TestQuotaStopDevinSessionRecordT4542(t *testing.T) {
	store := NewMemoryStore(t)
	t.Cleanup(func() { _ = store.Close() })
	root := t.TempDir()
	events := filepath.Join(root, "jobs", "job-active", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC().Format(time.RFC3339)
	for name, body := range map[string]string{
		"00001-job.claimed.json": `{"kind":"job.claimed","type":"job.claimed","created_at":"` + stamp + `","agent_label":"worker-synthetic","owner_lane":"owner","epoch":1}`,
		"00002-job.spawned.json": `{"kind":"job.spawned","type":"job.spawned","created_at":"` + stamp + `","pane_id":"workspace:pane-1"}`} {
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var logged bytes.Buffer
	q, err := newQuotaStopManager(store, root, false, slog.New(slog.NewTextHandler(&logged, nil)))
	if err != nil {
		t.Fatal(err)
	}
	q.read = func(context.Context, string) (string, error) {
		return "⏺ Done. The review is written.\n\n❭\n────\n", nil
	}
	policy := "## Values\n- default_expected: 1d\n- urgent_priorities: [1, 2]\n- max_waits: 1\n- resume_text: \"continue\"\n"
	var records int
	q.hk = quotaStopHKFixture(quotaPolicyAnswer(policy), `{"tasks":[{"priority":3,"expected":"4h","refs":{"job_id":"job-active"}}]}`, &records)

	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("HERDR_BIN", writeDevinHerdrStub(t, "workspace:pane-1", "rich-noodle"))
	start := time.Date(2026, 10, 20, 9, 59, 0, 0, time.UTC)
	writeDevinCLILog(t, filepath.Join(dataHome, "devin", "cli"), "rich-noodle", 4242, start)

	q.observeBounded(context.Background(), idleWakeCandidate{EventID: "wake-devin", PaneID: "workspace:pane-1", Label: "worker-synthetic"}, start.Add(time.Hour))

	var pool, reset string
	if err := store.db.QueryRow(`SELECT pool,reset FROM quota_stop_decisions WHERE event_id='wake-devin'`).Scan(&pool, &reset); err != nil {
		t.Fatal(err)
	}
	if pool != "devin-free" || reset != "2026-10-20T09:59:59Z" {
		t.Fatalf("pool=%s reset=%s, want the session record's devin-free stop", pool, reset)
	}
	if !strings.Contains(logged.String(), "outcome=quota") || !strings.Contains(logged.String(), "basis="+quotastop.BasisStructured) {
		t.Fatalf("observation log = %q, want the structured basis recorded", logged.String())
	}
}

func writeDevinHerdrStub(t *testing.T, pane, session string) string {
	t.Helper()
	stub := filepath.Join(t.TempDir(), "herdr")
	script := `#!/bin/sh
case "$*" in
  *"agent explain"*) echo '{"agent":"devin","matched_rule":null}' ;;
  *"agent list"*) echo '{"result":{"agents":[{"pane_id":"` + pane + `","agent":"devin","agent_session":{"kind":"id","value":"` + session + `"}}]}}' ;;
  *) echo '{}' ;;
esac
`
	if err := os.WriteFile(stub, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return stub
}

func writeDevinCLILog(t *testing.T, cli, session string, pid int, start time.Time) {
	t.Helper()
	logs, locks := filepath.Join(cli, "logs"), filepath.Join(cli, "session_locks")
	for _, dir := range []string{logs, locks} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	body, err := os.ReadFile("internal/quotastop/testdata/devin_session_free_limit.log")
	if err != nil {
		t.Fatal(err)
	}
	name := "devin_" + start.Local().Format("20060102-150405") + "_" + strconv.Itoa(pid) + ".log"
	if err := os.WriteFile(filepath.Join(logs, name), body, 0o600); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(locks, session+".lock")
	if err := os.WriteFile(lock, []byte(strconv.Itoa(pid)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(lock, start, start); err != nil {
		t.Fatal(err)
	}
}
