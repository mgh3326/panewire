package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

func quotaFake(t *testing.T, script string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pacelever"), []byte("#!/bin/sh\n"+script+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestQuotaCLIStatus(t *testing.T) {
	quotaFake(t, `if [ "$1" = "--version" ]; then echo 'pacelever 1.2'; exit 0; fi
 echo 'warning: secret warning' >&2
 echo '{"schema":"pace.v1","generated_at":"2026-01-01T00:00:00Z","providers":[{"id":"codex","source":"cache","age_s":12,"account_fp":"abc","account_fp_kind":"account","buckets":[{"window":"5h","used_pct":42}]}]}'`)
	var out, errout bytes.Buffer
	if code := runQuotaCLI([]string{"status", "--json"}, &out, &errout); code != 0 {
		t.Fatalf("code %d: %s", code, errout.String())
	}
	for _, want := range []string{`"schema":"panewire.quota.status.v1"`, `"pool":"codex"`, `"account":"abc"`, `"age_seconds":12`, `"source":"cache"`, `"used_pct":42`, `"pacelever_version":"pacelever 1.2"`} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %s: %s", want, out.String())
		}
	}
	if strings.Contains(out.String()+errout.String(), "warning") {
		t.Fatal("warning leaked")
	}
}
func TestQuotaCLIRefusalAndPlainCommands(t *testing.T) {
	quotaFake(t, `echo 'warning: do not leak' >&2
 if [ "$1" = "--json" ]; then echo '{"reason":"quota unavailable"}'; exit 3; fi
 if [ "$1" = "observe" ]; then printf '%s\n' "$@"; echo 'actual failure' >&2; exit 7; fi
 echo refreshed`)
	var out, errout bytes.Buffer
	if code := runQuotaCLI([]string{"status"}, &out, &errout); code != 3 || !strings.Contains(errout.String(), "quota unavailable") || strings.Contains(errout.String(), "do not leak") {
		t.Fatalf("status %d %s", code, errout.String())
	}
	out.Reset()
	errout.Reset()
	if code := runQuotaCLI([]string{"refresh", "kimi"}, &out, &errout); code != 0 || out.String() != "refreshed\n" {
		t.Fatalf("refresh %d %q %s", code, out.String(), errout.String())
	}
	out.Reset()
	errout.Reset()
	if code := runQuotaCLI([]string{"observe", "devin-free-limit", "--foo", "bar"}, &out, &errout); code != 7 || !strings.Contains(errout.String(), "exit 7: actual failure") || strings.Contains(errout.String(), "warning") {
		t.Fatalf("observe %d %s", code, errout.String())
	}
}
func TestQuotaTimeout(t *testing.T) {
	quotaFake(t, `exec sleep 2`)
	_, code, err := quotaRun(context.Background(), "quota status", 20*time.Millisecond, true, "--json")
	if code != ExitTimeout || err == nil || !strings.Contains(err.Error(), "quota status: timed out after 20ms") {
		t.Fatalf("%d %v", code, err)
	}
}
func TestQuotaObserveArguments(t *testing.T) {
	quotaFake(t, `printf '%s\n' "$@"`)
	var out, errout bytes.Buffer
	if code := runQuotaCLI([]string{"observe", "devin-free-limit", "--foo", "bar"}, &out, &errout); code != 0 || out.String() != "observe\ndevin-free-limit\n--foo\nbar\n" {
		t.Fatalf("%d %q %s", code, out.String(), errout.String())
	}
}

func TestQuotaTimeoutInheritedPipe(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "child.pid")
	t.Setenv("QUOTA_TEST_PIDFILE", pidfile)
	quotaFake(t, `sleep 30 & echo $! > "$QUOTA_TEST_PIDFILE"; wait`)
	t.Cleanup(func() {
		data, err := os.ReadFile(pidfile)
		if err != nil {
			t.Errorf("read child pid: %v", err)
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			t.Error(err)
			return
		}
		p, err := os.FindProcess(pid)
		if err == nil {
			_ = p.Kill()
		}
	})
	start := time.Now()
	_, code, err := quotaRun(context.Background(), "quota status", time.Second, true, "--json")
	if elapsed := time.Since(start); code != ExitTimeout || err == nil || elapsed > 4*time.Second {
		t.Fatalf("code=%d err=%v elapsed=%s", code, err, elapsed)
	}
}

func TestQuotaSuccessfulProcessWithInheritedPipe(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "child.pid")
	t.Setenv("QUOTA_TEST_PIDFILE", pidfile)
	quotaFake(t, `sleep 30 & echo $! > "$QUOTA_TEST_PIDFILE"; echo done`)
	t.Cleanup(func() {
		data, err := os.ReadFile(pidfile)
		if err != nil {
			t.Error(err)
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			t.Error(err)
			return
		}
		if p, err := os.FindProcess(pid); err == nil {
			_ = p.Kill()
		}
	})
	var out, errout bytes.Buffer
	if code := runQuotaCLI([]string{"observe", "devin-free-limit"}, &out, &errout); code != 0 || out.String() != "done\n" {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), errout.String())
	}

}

func TestNodeRunnerSuccessfulProcessWithInheritedPipe(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "child.pid")
	t.Setenv("QUOTA_TEST_PIDFILE", pidfile)
	quotaFake(t, `sleep 30 & echo $! > "$QUOTA_TEST_PIDFILE"; echo done`)
	t.Cleanup(func() {
		data, err := os.ReadFile(pidfile)
		if err != nil {
			t.Error(err)
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			t.Error(err)
			return
		}
		if p, err := os.FindProcess(pid); err == nil {
			_ = p.Kill()
		}
	})
	data, err := defaultNodeCommandRunner("pacelever")(context.Background(), "refresh", "codex")
	if err != nil || string(data) != "done\n" {
		t.Fatalf("node out=%q err=%v", data, err)
	}
}

func TestQuotaObserveSignalFailure(t *testing.T) {
	quotaFake(t, `kill -KILL $$`)
	var out, errout bytes.Buffer
	if code := runQuotaCLI([]string{"observe", "devin-free-limit"}, &out, &errout); code != ExitInternal || !strings.Contains(errout.String(), "exit 70") {
		t.Fatalf("code=%d stderr=%q", code, errout.String())
	}
}

// TestQuotaCLIStatusWithoutPacelever: with no pacelever binary, `panewire
// quota status` reports the stored readings of the pools this host measures
// natively and says pacelever is absent, instead of failing. It reads the
// shared store only: no measurement, no lease, no publication.
func TestQuotaCLIStatusWithoutPacelever(t *testing.T) {
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
			Pacelever string         `json:"pacelever"`
			Readings  []quotaReading `json:"readings"`
		} `json:"hosts"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil || doc.Schema != "panewire.quota.status.v1" || len(doc.Hosts) != 1 {
		t.Fatalf("status JSON: %v %s", err, out.String())
	}
	host := doc.Hosts[0]
	if host.Pacelever != "absent" {
		t.Fatalf("status must say pacelever is absent: %s", out.String())
	}
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
	if !strings.Contains(errOut.String(), "pacelever absent") || strings.Contains(out.String()+errOut.String(), claudeSecret) {
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
	if code := runQuotaCLI([]string{"status"}, &out, &errOut); code != 0 || !strings.Contains(out.String(), " claude account="+claudeFPForTest+" source=handoffkeep") {
		t.Fatalf("text status %d: %q %q", code, out.String(), errOut.String())
	}
}
