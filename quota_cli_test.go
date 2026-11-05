package panewire

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	if code := runQuotaCLI([]string{"refresh", "codex"}, &out, &errout); code != 0 || out.String() != "refreshed\n" {
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
