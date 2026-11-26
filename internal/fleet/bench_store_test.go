package fleet

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestRepsPathUsesStateDirForRelativeXDGStateHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_STATE_HOME", "relative-state")
	a := &App{Home: home}

	want := filepath.Join(home, ".local", "state", "panewire", "reps.json")
	if got := a.repsPath(); got != want {
		t.Fatalf("repsPath() = %q, want %q", got, want)
	}
}

func TestRepsAddRequiresAbsoluteStateDirForWriteAheadIntent(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := testApp(t)
	a.Home = "relative-home"
	a.StateDir = ""
	t.Setenv("XDG_STATE_HOME", "")
	a.BenchStore = &BenchStore{
		hk:          &hkQuota{base: srv.URL, token: "test-token", now: a.Now},
		now:         a.Now,
		pendingPath: a.repsPath(),
	}
	err := a.repsAdd(context.Background(), []string{
		"--profile", "omp-opus", "--model", "anthropic/claude-opus-5-5",
		"--task", "hk6613", "--tier", "T1", "--role", "impl", "--grade", "C",
		"--rounds", "1", "--blockers-found", "0", "--completed", "1",
	}, &strings.Builder{})
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != exitConfig || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("repsAdd error = %v, want configuration error requiring an absolute state dir", err)
	}
	if len(fake.repBodies) != 0 {
		t.Fatalf("submitted %d reps without a write-ahead intent", len(fake.repBodies))
	}
}
