package panewire

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// TestQuotaStatusTablePeopleRows: the text form of `quota status` is one row
// per pool and window with the columns a person reads (host, pool, account,
// window, used, resets in, freshness, band); a reading without windows and a
// stale reading still get a row.
func TestQuotaStatusTablePeopleRows(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	hosts := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(hosts, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLEET_HOSTS_CONFIG", hosts)
	hk := newClaudeHK(t)
	reset := claudeTestNow.Add(3 * time.Hour)
	stored, _ := json.Marshal(shared.Observation{Kind: shared.KindAuto, MeasuredAt: claudeTestNow.Add(-time.Minute), Buckets: []shared.Bucket{{Window: "5h", UsedPct: 42, ResetsAt: &reset, Scope: "account"}}})
	hk.latest = stored
	spawn := claudeStore(t, hk.server.URL)
	previous := quotaCLISource
	t.Cleanup(func() { quotaCLISource = previous })
	quotaCLISource = func(pool string) *nativeQuotaSource {
		if pool == "claude" {
			return claudeTestSource(spawn, claudeLoginFiles(claudeSecret, claudeTestNow.Add(time.Hour)), claudeAPIReply(500, ""))
		}
		return &nativeQuotaSource{Pool: pool, Now: func() time.Time { return claudeTestNow }}
	}
	var out, errOut bytes.Buffer
	if code := runQuotaCLI([]string{"status"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("status table has no rows: %q", out.String())
	}
	if got := strings.Join(strings.Fields(lines[0]), " "); got != "HOST POOL ACCOUNT WINDOW USED RESETS IN FRESHNESS BAND" {
		t.Fatalf("header = %q", got)
	}
	rows := map[string][]string{}
	for _, line := range lines[1:] {
		f := strings.Fields(line)
		if len(f) < 2 {
			t.Fatalf("malformed row %q", line)
		}
		rows[f[1]] = f
	}
	claude := rows["claude"]
	if strings.Join(claude[2:], " ") != claudeFPForTest+" 5h 42.0% 3h 0m fresh ranked" {
		t.Fatalf("claude row = %v", claude)
	}
	codex := rows["codex"]
	if strings.Join(codex[2:], " ") != "- - - - stale unknown" {
		t.Fatalf("codex row = %v", codex)
	}
	if strings.Contains(out.String(), "source=") || strings.Contains(out.String(), "buckets=") {
		t.Fatalf("status still prints the key=value form: %q", out.String())
	}
}
