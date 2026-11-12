package fleet

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
func TestPaneSweepWritesNoLocalScreenWhenTheUploadIsStored(t *testing.T) {
	f := newPaneSweepFixture(t)
	hk := useFakeHK(t)
	pane := f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if got := f.closed(); !slices.Equal(got, []string{pane}) {
		t.Fatalf("closed panes = %v, want [%s]", got, pane)
	}
	if f.localScreenFile("j") {
		t.Fatalf("a stored screen left %s in the job dir", PaneScreenFile)
	}
	put := screenDocPut(t, hk, "j")
	if put == nil || put["body"] != "last screen of "+pane {
		t.Fatalf("screen put = %v, want the screen document with the pane's screen", put)
	}
	if errs := f.a.Err.(*bytes.Buffer).String(); strings.Contains(errs, paneScreenUploadWarning) {
		t.Fatalf("warnings = %q, want none after a stored upload", errs)
	}
}

// A stored upload leaves an existing local screen from an earlier pass alone.
func TestPaneSweepLeavesAnEarlierLocalScreenAloneWhenTheUploadIsStored(t *testing.T) {
	f := newPaneSweepFixture(t)
	useFakeHK(t)
	pane := f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	if err := os.WriteFile(filepath.Join(f.a.Jobs, "j", PaneScreenFile), []byte("earlier\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f.at(0)
	f.at(11)
	if got := f.closed(); !slices.Equal(got, []string{pane}) {
		t.Fatalf("closed panes = %v, want [%s]", got, pane)
	}
	if got := f.screen("j"); got != "earlier\n" {
		t.Fatalf("earlier local screen = %q, want it untouched", got)
	}
}

// (b) An upload hk does not store writes the whole screen locally at mode
// 0600, logs one warning, and the pane still closes.
func TestPaneSweepWritesTheLocalScreenWhenTheUploadFails(t *testing.T) {
	f := newPaneSweepFixture(t)
	useFakeHK(t).Handle(hkFails("PUT /v1/documents/", http.StatusInternalServerError, `{"error":"internal_error"}`))
	pane := f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if got := f.closed(); !slices.Equal(got, []string{pane}) {
		t.Fatalf("closed panes = %v, want [%s]", got, pane)
	}
	if f.screen("j") != "last screen of "+pane {
		t.Fatalf("local screen = %q, want the whole screen", f.screen("j"))
	}
	info, err := os.Stat(filepath.Join(f.a.Jobs, "j", PaneScreenFile))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("local screen mode = %v, %v; want 0600", info, err)
	}
	if errs := f.a.Err.(*bytes.Buffer).String(); strings.Count(errs, paneScreenUploadWarning) != 1 {
		t.Fatalf("warnings = %q, want one %q", errs, paneScreenUploadWarning)
	}
}

// (c) An upload hk does not store and a local write that fails keep the pane
// with screen-not-saved. The local write fails because a directory sits at
// pane-screen.txt, which the atomic rename cannot replace.
func TestPaneSweepKeepsThePaneWhenNeitherUploadNorLocalWriteSucceeds(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	if err := os.Mkdir(filepath.Join(f.a.Jobs, "j", PaneScreenFile), 0700); err != nil {
		t.Fatal(err)
	}
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("pane closed although neither hk nor the job dir kept its screen")
	}
	if !f.infoHas("reason=screen-not-saved") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A pane read failure keeps the pane whether or not hk is configured.
func TestPaneSweepKeepsThePaneWhenItsReadFailsBeforeUpload(t *testing.T) {
	f := newPaneSweepFixture(t)
	hk := useFakeHK(t)
	f.job("j", "worker", "42")
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
func TestPaneSweepUploadsScreenOutsideExclusiveLock(t *testing.T) {
	f := newPaneSweepFixture(t)
	hk := useFakeHK(t)
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	hk.Handle(func(req hkRequest) (int, string, bool) {
		if req.Method != http.MethodPut || !strings.HasPrefix(req.Path, "/v1/documents/") {
			return 0, "", false
		}
		close(started)
		<-release
		return http.StatusOK, `{"document":{},"changed":true}`, true
	})
	pane := f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	var exclusive atomic.Bool
	f.sweeper.Exclusive = func() func() {
		exclusive.Store(true)
		return func() { exclusive.Store(false) }
	}
	f.at(0)
	f.now = time.Date(2026, 3, 1, 12, 11, 0, 0, time.UTC)
	done := make(chan error, 1)
	go func() { done <- f.sweeper.Pass(context.Background(), f.a) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("sweep did not start the screen upload")
	}
	lockHeldDuringUpload := exclusive.Load()
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sweep did not finish after screen upload returned")
	}
	if lockHeldDuringUpload {
		t.Fatal("screen upload held the exclusive node-command lock")
	}
	if got := f.closed(); !slices.Equal(got, []string{pane}) {
		t.Fatalf("closed panes = %v, want [%s]", got, pane)
	}
}

// A screen over hk's document cap is stored as its tail, so the stored copy is
// not the whole screen: the full screen stays locally, and the document holds
// the cut (PR 377 review). The pane still closes.
func TestPaneSweepKeepsTheWholeOversizedScreenLocally(t *testing.T) {
	f := newPaneSweepFixture(t)
	hk := useFakeHK(t)
	pane := f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	full := "first line\n" + strings.Repeat("x", PaneScreenDocMaxBytes+100) + "\nlast line\n"
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "pane read "+pane {
			return Result{Out: full}
		}
		return base(c)
	}
	f.at(0)
	f.at(11)
	if got := f.closed(); !slices.Equal(got, []string{pane}) {
		t.Fatalf("closed panes = %v, want [%s]", got, pane)
	}
	if got := f.screen("j"); got != full {
		t.Fatalf("local screen is %d bytes, want the whole %d", len(got), len(full))
	}
	put := screenDocPut(t, hk, "j")
	if put == nil {
		t.Fatalf("hk routes = %q, want the screen document PUT", hk.Routes())
	}
	if body, _ := put["body"].(string); body == full || len(body) > PaneScreenDocMaxBytes {
		t.Fatalf("stored body is %d bytes, want the cut copy at most %d", len(body), PaneScreenDocMaxBytes)
	}
}
