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

// TestQuotaStatusNativeOnly reads native stored measurements without invoking a quota CLI.
func TestQuotaStatusNativeOnly(t *testing.T) {
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
	if code := runQuotaCLI([]string{"status", "--json"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	var doc struct {
		Schema string `json:"schema"`
		Hosts  []struct {
			Readings []quotaReading `json:"readings"`
		} `json:"hosts"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil || doc.Schema != "panewire.quota.status.v2" || len(doc.Hosts) != 1 {
		t.Fatalf("status JSON: %v %s", err, out.String())
	}
	host := doc.Hosts[0]
	byPool := map[string]quotaReading{}
	for _, r := range host.Readings {
		byPool[r.Pool] = r
	}
	claudeReading := byPool["claude"]
	if claudeReading.Account != claudeFPForTest || claudeReading.Source != "handoffkeep" || claudeReading.Stale || claudeReading.AgeSeconds == nil || *claudeReading.AgeSeconds != 60 || len(claudeReading.Buckets) != 1 || *claudeReading.Buckets[0].UsedPct != 42 {
		t.Fatalf("claude reading = %+v", claudeReading)
	}
	if codex, ok := byPool["codex"]; !ok || codex.Source != "unavailable" || !codex.Stale || len(codex.Buckets) != 0 {
		t.Fatalf("codex without a store reads unavailable: %+v (%s)", codex, out.String())
	}
	if strings.Contains(out.String()+errOut.String(), claudeSecret) || strings.Contains(out.String(), "pacelever") {
		t.Fatalf("stderr = %q", errOut.String())
	}
	hk.mu.Lock()
	calls := append([]string(nil), hk.calls...)
	hk.mu.Unlock()
	for _, call := range calls {
		if !strings.HasPrefix(call, "GET ") {
			t.Fatalf("status must only read the store, got %v", calls)
		}
	}
	if len(calls) == 0 {
		t.Fatal("status did not read the shared store")
	}
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"refresh", "grok"}, &out, &errOut); code != ExitConditionInvalid || !strings.Contains(errOut.String(), "native 측정 소스가 없습니다") {
		t.Fatalf("unmeasurable refresh code=%d stderr=%q", code, errOut.String())
	}

	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"status"}, &out, &errOut); code != 0 || !strings.Contains(out.String(), " claude account="+claudeFPForTest+" source=handoffkeep") {
		t.Fatalf("text status %d: %q %q", code, out.String(), errOut.String())
	}
}

func TestQuotaObserveDevinFreeLimitUsesNativeWriter(t *testing.T) {
	home := t.TempDir()
	cache := filepath.Join(t.TempDir(), "cache")
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", cache)
	reset := time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
	var out, errOut bytes.Buffer
	if code := runQuotaCLI([]string{"observe", "devin-free-limit", "--reset", reset, "--rung", "swe-2-high", "--source", "job:test"}, &out, &errOut); code != 0 {
		t.Fatalf("observe code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
	path := filepath.Join(cache, "panewire", "devin-free-limit.json")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("record info=%v error=%v; want mode 0600", info, err)
	}
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"observe", "devin-free-limit", "--clear"}, &out, &errOut); code != 0 || !strings.Contains(out.String(), "cleared devin-free-limit") {
		t.Fatalf("clear code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"observe", "devin-free-limit", "--reset", "not-a-time"}, &out, &errOut); code != ExitUsage || !strings.Contains(errOut.String(), "reset 시간이") {
		t.Fatalf("invalid reset code=%d stderr=%q", code, errOut.String())
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"observe", "devin-free-limit", "--clear", "--account-key", "devin:0123456789abcdef"}, &out, &errOut); code != ExitInternal || !strings.Contains(errOut.String(), "without --account-key") {
		t.Fatalf("account-scoped corrupt clear code=%d stderr=%q", code, errOut.String())
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("account-scoped clear removed the entire corrupt cache: %v", err)
	}
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"observe", "devin-free-limit", "--clear"}, &out, &errOut); code != 0 {
		t.Fatalf("clear corrupt record code=%d stderr=%q", code, errOut.String())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("corrupt record remains after clear: %v", err)
	}
	cacheObstacle := filepath.Join(t.TempDir(), "cache-is-a-file")
	if err := os.WriteFile(cacheObstacle, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CACHE_HOME", cacheObstacle)
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"observe", "devin-free-limit", "--unparsed"}, &out, &errOut); code != ExitInternal {
		t.Fatalf("storage failure exit=%d stderr=%q, want %d", code, errOut.String(), ExitInternal)
	}
}
