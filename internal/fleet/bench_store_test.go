package fleet

import (
	"path/filepath"
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
