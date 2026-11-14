package fleet

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHKClaimRejectsTerminalStateFromTolerantAnswer(t *testing.T) {
	hk := useFakeHK(t)
	hk.Handle(func(r hkRequest) (int, string, bool) {
		if r.Route() != "POST /v1/tasks/42/claim" {
			return 0, "", false
		}
		return http.StatusOK, `{"id":42,"state":"merged","claimed_by":"worker","refs":{"job_id":"job-1"},"priority":"not-a-number"}`, true
	})
	task, err := hkClaimTask(context.Background(), "42", "worker", "job-1")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	a := &App{Home: root, Jobs: filepath.Join(root, "jobs"), Out: &bytes.Buffer{}, Now: func() time.Time { return time.Now() }}
	if err := a.taskLink(context.Background(), Spawn{Task: "42", Job: "job-1", Label: "worker"}, task, false); err == nil || !strings.Contains(err.Error(), "task claim did not confirm") {
		t.Fatalf("terminal claim answer accepted: %v", err)
	}
}

func TestHKTaskAnswerRejectsInvalidUTF8(t *testing.T) {
	hk := useFakeHK(t)
	hk.Handle(func(r hkRequest) (int, string, bool) {
		if r.Route() != "GET /v1/tasks/42" {
			return 0, "", false
		}
		return http.StatusOK, "{\"id\":42,\"state\":\"ready\",\"title\":\"x\xff\"}", true
	})
	if _, err := hkGetTask(context.Background(), "42"); !errors.Is(err, errHKUnreadable) {
		t.Fatalf("invalid UTF-8 task answer error=%v", err)
	}
}

func TestTrustDoesNotRequireHandoffkeepBinary(t *testing.T) {
	// trust() validates the canonical launch-home paths; TestMain pins
	// XDG_DATA_HOME to a temp dir, which trust() refuses, so clear it here.
	t.Setenv("XDG_DATA_HOME", "")
	home := testLaunchHome
	if home == "" {
		var err error
		home, err = os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
	}
	if filepath.Clean(home) != filepath.Clean(mustCurrentHome(t)) {
		t.Skip("user home lookup differs from canonical current user")
	}
	path := filepath.Join(t.TempDir(), "panewire")
	if err := os.WriteFile(path, []byte("fixture"), 0700); err != nil {
		t.Fatal(err)
	}
	a := &App{Home: home, Jobs: filepath.Join(home, "work", "herdr-inbox", "jobs"), Bins: map[string]string{"handoffkeep": filepath.Join(t.TempDir(), "missing")}}
	if err := TrustBinary(path, uint32(os.Getuid())); err != nil {
		t.Skipf("fixture executable cannot satisfy platform trust checks: %v", err)
	}
	if err := a.trust(); err != nil {
		t.Fatalf("trust required handoffkeep: %v", err)
	}
}

func mustCurrentHome(t *testing.T) string {
	t.Helper()
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	return u.HomeDir
}
