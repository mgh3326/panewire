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

// quotaStatusFixture stages claude with a stored 42% five-hour window, leaves
// every other native pool unmeasured, and runs `quota status` with args. It
// returns the exit code, stdout and stderr.
func quotaStatusFixture(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
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
	code := runQuotaCLI(append([]string{"status"}, args...), &out, &errOut)
	return code, out.String(), errOut.String()
}

// TestQuotaStatusTablePeopleRows: the text form of `quota status` is one row
// per pool and window with the columns a person reads (host, pool, account,
// window, used, resets in, freshness, band); a reading without windows and a
// stale reading still get a row.
func TestQuotaStatusTablePeopleRows(t *testing.T) {
	code, text, errText := quotaStatusFixture(t)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errText)
	}
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) < 2 {
		t.Fatalf("status table has no rows: %q", text)
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
	if strings.Contains(text, "source=") || strings.Contains(text, "buckets=") {
		t.Fatalf("status still prints the key=value form: %q", text)
	}
}

// TestQuotaStatusJSONIgnoresPoolPolicy: `--json` is the machine contract and
// reads no pool policy, so an unreadable PANEWIRE_POOL_POLICY must not stop it.
// The text table needs the policy for its band column and refuses instead.
func TestQuotaStatusJSONIgnoresPoolPolicy(t *testing.T) {
	t.Setenv("PANEWIRE_POOL_POLICY", filepath.Join(t.TempDir(), "missing.toml"))
	code, out, errText := quotaStatusFixture(t, "--json")
	if code != 0 {
		t.Fatalf("--json exit %d with a bad pool policy: %s", code, errText)
	}
	var doc struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil || doc.Schema != "panewire.quota.status.v2" {
		t.Fatalf("--json did not print the v2 document: %v %q", err, out)
	}
	if code, _, _ := quotaStatusFixture(t); code == 0 {
		t.Fatal("the text table must refuse without a readable pool policy")
	}
}

// TestQuotaStatusTableExcludedPoolBand: a native pool's band is its admission
// class. An operator-excluded claude reads `excluded` on every row, the band
// placement gives it, so the table cannot show it as ranked.
func TestQuotaStatusTableExcludedPoolBand(t *testing.T) {
	config := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(config, []byte("[pools.claude]\nclass = \"exclude\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PANEWIRE_CONFIG", config)
	code, out, errOut := quotaStatusFixture(t)
	if code != ExitOK {
		t.Fatalf("exit %d, stderr %q", code, errOut)
	}
	rows := 0
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[1] != "claude" {
			continue
		}
		rows++
		if band := fields[len(fields)-1]; band != "excluded" {
			t.Fatalf("claude row band = %q, want excluded: %q", band, line)
		}
	}
	if rows == 0 {
		t.Fatalf("no claude row in %q", out)
	}
}
