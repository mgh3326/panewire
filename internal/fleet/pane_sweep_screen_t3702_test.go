package fleet

import (
	"os"
	"path/filepath"
	"testing"
)

// hk 3702: a swept pane's screen lives in hk. A successful upload writes no
// local pane-screen.txt; a failed upload keeps the local file as the only copy.

// localScreenFile reports whether job's local pane-screen.txt exists.
func (f *paneSweepFixture) localScreenFile(job string) bool {
	f.t.Helper()
	_, err := os.Lstat(filepath.Join(f.a.Jobs, job, PaneScreenFile))
	switch {
	case err == nil:
		return true
	case os.IsNotExist(err):
		return false
	}
	f.t.Fatalf("local screen: %v", err)
	return false
}

// (a) An upload that hk stores leaves no local screen; the pane closes and
// the screen document holds the screen.
func TestPaneSweepKeepsThePaneWhenItsReadFailsBeforeUpload(t *testing.T) {
	f := newPaneSweepFixture(t)
	hk := useFakeHK(t)
	f.job("j", "implementer", "42")
	f.tasks["42"] = "merged"
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "herdr" && len(c.Args) > 1 && c.Args[1] == "read" {
			return Result{Code: 1}
		}
		return base(c)
	}
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 {
		t.Fatalf("pane closed without its screen read")
	}
	if put := screenDocPut(t, hk, "j"); put != nil {
		t.Fatalf("an unread screen was uploaded: %v", put)
	}
}

// hk 3623: screen upload must not retain the exclusive node-command lock.
