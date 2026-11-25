package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// laneFixture is a node checkout spawn at the launcher: cmdFixture's tree is
// the prepared worktree W, beside a sibling job-b nothing may launch in.
type laneFixture struct {
	a        *App
	s        Spawn
	f        *fakeRunner
	w, other string
}

func newLaneFixture(t *testing.T) *laneFixture {
	t.Helper()
	a, s, f, _ := cmdFixture(t)
	// A live lane, unlike a mock one, needs no MCP descriptor.
	s.ExecutionLane = "live"
	l := &laneFixture{a: a, s: s, f: f, w: s.CWD, other: filepath.Join(filepath.Dir(s.CWD), "job-b")}
	mkdirs(t, l.other)
	return l
}

// reportPaneAt makes herdr and the harness banner name dir, where the pane runs.
func (l *laneFixture) reportPaneAt(dir string) {
	previous := l.f.Do
	l.f.Do = func(c Command) Result {
		r := previous(c)
		r.Out = strings.ReplaceAll(r.Out, l.w, dir)
		return r
	}
}

// bind resolves the lane as the node does after prepare.
func (l *laneFixture) bind(t *testing.T) LaunchDir {
	t.Helper()
	dir, err := BindLaunchDir(l.w, l.s.ExecutionLane)
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// launch runs the node's launcher boundary on a bound directory.
func (l *laneFixture) launch(ctx context.Context, dir LaunchDir) error {
	return l.a.Spawn(ctx, bindNodeSpawn(l.s, dir, l.s.Prompt, l.s.Workspace))
}

// herdrCalls lists the herdr commands starting with prefix, in order.
func (l *laneFixture) herdrCalls(prefix string) []string {
	l.f.mu.Lock()
	defer l.f.mu.Unlock()
	var calls []string
	for _, c := range l.f.Calls {
		if args := strings.Join(c.Args, " "); c.Name == "herdr" && strings.HasPrefix(args, prefix) {
			calls = append(calls, args)
		}
	}
	return calls
}

// tabDirs is the --cwd of every tab create herdr received.
func (l *laneFixture) tabDirs() []string {
	l.f.mu.Lock()
	defer l.f.mu.Unlock()
	var dirs []string
	for _, c := range l.f.Calls {
		if c.Name == "herdr" && strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			dirs = append(dirs, c.Args[slices.Index(c.Args, "--cwd")+1])
		}
	}
	return dirs
}

func mkdirs(t *testing.T, paths ...string) {
	t.Helper()
	for _, path := range paths {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
}

func symlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
}

// repoint replaces the link at link with one to target.
func repoint(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	symlink(t, target, link)
}

func TestBindLaunchDirResolvesOnce_t6404(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lane  string
		setup func(t *testing.T, w, other string)
		// want is the bound path below the worktree; refused means the lane
		// leads out of it.
		want    string
		refused bool
	}{
		{name: "no lane", lane: "", setup: func(t *testing.T, w, _ string) { mkdirs(t, filepath.Join(w, "live")) }, want: "."},
		{name: "absent lane", lane: "live", setup: func(*testing.T, string, string) {}, want: "."},
		{name: "lane is a file", lane: "live", setup: func(t *testing.T, w, _ string) {
			if err := os.WriteFile(filepath.Join(w, "live"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}, want: "."},
		{name: "lane is a directory", lane: "mock", setup: func(t *testing.T, w, _ string) { mkdirs(t, filepath.Join(w, "mock")) }, want: "mock"},
		{name: "relative in-tree link", lane: "live", setup: func(t *testing.T, w, _ string) {
			mkdirs(t, filepath.Join(w, "sub", "live-impl"))
			symlink(t, "sub/live-impl", filepath.Join(w, "live"))
		}, want: "sub/live-impl"},
		{name: "absolute in-tree link", lane: "live", setup: func(t *testing.T, w, _ string) {
			mkdirs(t, filepath.Join(w, "sub"))
			symlink(t, filepath.Join(w, "sub"), filepath.Join(w, "live"))
		}, want: "sub"},
		{name: "link to the worktree itself", lane: "live", setup: func(t *testing.T, w, _ string) { symlink(t, ".", filepath.Join(w, "live")) }, want: "."},
		{name: "link to an in-tree file", lane: "live", setup: func(t *testing.T, w, _ string) {
			if err := os.WriteFile(filepath.Join(w, "file"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			symlink(t, "file", filepath.Join(w, "live"))
		}, want: "."},
		{name: "link to a sibling job", lane: "live", setup: func(t *testing.T, w, _ string) { symlink(t, "../job-b", filepath.Join(w, "live")) }, refused: true},
		{name: "link to a file outside", lane: "live", setup: func(t *testing.T, w, other string) {
			if err := os.WriteFile(filepath.Join(other, "file"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			symlink(t, "../job-b/file", filepath.Join(w, "live"))
		}, refused: true},
		{name: "link to a name-prefix sibling", lane: "live", setup: func(t *testing.T, w, _ string) {
			mkdirs(t, w+"-copy")
			symlink(t, w+"-copy", filepath.Join(w, "live"))
		}, refused: true},
		{name: "link to the parent", lane: "live", setup: func(t *testing.T, w, _ string) { symlink(t, "..", filepath.Join(w, "live")) }, refused: true},
		{name: "dangling link", lane: "live", setup: func(t *testing.T, w, _ string) { symlink(t, "missing", filepath.Join(w, "live")) }, refused: true},
		{name: "link loop", lane: "live", setup: func(t *testing.T, w, _ string) { symlink(t, "live", filepath.Join(w, "live")) }, refused: true},
		// The node binds before the launcher validates the -L value.
		{name: "lane value naming a sibling directory", lane: "../job-b", setup: func(*testing.T, string, string) {}, refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			w, other := filepath.Join(root, "job-a"), filepath.Join(root, "job-b")
			mkdirs(t, w, other)
			tc.setup(t, w, other)
			dir, err := BindLaunchDir(w, tc.lane)
			if tc.refused {
				if !errors.Is(err, ErrLaneOutsideWorktree) {
					t.Fatalf("bound %q err=%v, want ErrLaneOutsideWorktree", dir.Path, err)
				}
				return
			}
			if want := filepath.Join(w, tc.want); err != nil || dir.Path != want {
				t.Fatalf("bound %q err=%v, want %q", dir.Path, err, want)
			}
			if err := dir.verify(dir.Path); err != nil {
				t.Fatalf("a fresh binding does not verify: %v", err)
			}
		})
	}
}

// A binding names one directory, not whatever later answers to its path.
func TestLaunchDirVerifyNamesOneDirectory_t6404(t *testing.T) {
	for _, tc := range []struct {
		name string
		swap func(t *testing.T, w string)
	}{
		{"another directory", func(t *testing.T, w string) {
			// Renamed, not removed: the old inode stays taken.
			if err := os.Rename(w, w+".old"); err != nil {
				t.Fatal(err)
			}
			mkdirs(t, w)
		}},
		{"a link to the bound directory itself", func(t *testing.T, w string) {
			if err := os.Rename(w, w+".old"); err != nil {
				t.Fatal(err)
			}
			symlink(t, w+".old", w)
		}},
		{"a file", func(t *testing.T, w string) {
			if err := os.Rename(w, w+".old"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(w, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"nothing", func(t *testing.T, w string) {
			if err := os.Remove(w); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := filepath.Join(t.TempDir(), "job-a")
			mkdirs(t, w)
			dir, err := BindLaunchDir(w, "")
			if err != nil {
				t.Fatal(err)
			}
			tc.swap(t, w)
			if err := dir.verify(dir.Path); ErrorCode(err) != exitStateConflict || !strings.HasPrefix(err.Error(), "target_changed:") {
				t.Fatalf("err=%v, want target_changed", err)
			}
		})
	}
	// A path that was no directory when bound matches none that appears later.
	late := filepath.Join(t.TempDir(), "job-late")
	dir, err := BindLaunchDir(late, "")
	if err != nil {
		t.Fatal(err)
	}
	mkdirs(t, late)
	if err := dir.verify(late); err == nil {
		t.Fatal("a directory that appeared after the binding verified")
	}
	// An unbound LaunchDir has nothing to verify.
	if err := (LaunchDir{Path: late}).verify(late); err != nil {
		t.Fatal(err)
	}
}

func TestInTreeLaneLinkAccepted_t6404(t *testing.T) {
	l := newLaneFixture(t)
	target := filepath.Join(l.w, "sub", "live-impl")
	mkdirs(t, target)
	symlink(t, "sub/live-impl", filepath.Join(l.w, "live"))
	l.reportPaneAt(target)
	var receipt ExecutionReceipt
	l.a.ObserveReceipt = func(r ExecutionReceipt) { receipt = r }
	if err := l.launch(t.Context(), l.bind(t)); err != nil {
		t.Fatal(err)
	}
	if dirs := l.tabDirs(); !slices.Equal(dirs, []string{target}) {
		t.Fatalf("tab create --cwd %q, want %q", dirs, target)
	}
	// The receipt names where the pane runs, which the hub checks it by.
	if receipt.CWD != target || !strings.HasPrefix(receipt.CWD, l.w+string(filepath.Separator)) {
		t.Fatalf("receipt cwd %q, want %q", receipt.CWD, target)
	}
}

func TestLaneLinkSwappedAfterResolveRefused_t6404(t *testing.T) {
	l := newLaneFixture(t)
	target := filepath.Join(l.w, "sub", "live-impl")
	mkdirs(t, target)
	link := filepath.Join(l.w, "live")
	symlink(t, "sub/live-impl", link)
	l.reportPaneAt(target)
	dir := l.bind(t)
	repoint(t, "../job-b", link)
	err := l.launch(t.Context(), dir)
	// The bound target or a refusal; never the directory the link names now.
	dirs := l.tabDirs()
	if err == nil && !slices.Equal(dirs, []string{target}) || err != nil && (len(dirs) != 0 || !strings.Contains(err.Error(), "target_changed:")) {
		t.Fatalf("tab create --cwd %q err=%v, want %q or target_changed with no tab", dirs, err, target)
	}
}

// A lane that is a real directory binds as W/live; a link put there afterwards
// is another target.
func TestLaneDirectorySwappedForLinkRefused_t6404(t *testing.T) {
	l := newLaneFixture(t)
	lane := filepath.Join(l.w, "live")
	mkdirs(t, lane)
	dir := l.bind(t)
	if err := os.Rename(lane, lane+".old"); err != nil {
		t.Fatal(err)
	}
	symlink(t, "../job-b", lane)
	err := l.launch(t.Context(), dir)
	if ErrorCode(err) != exitStateConflict || !strings.Contains(err.Error(), "target_changed:") || len(l.tabDirs()) != 0 {
		t.Fatalf("tab create --cwd %q err=%v, want target_changed with no tab", l.tabDirs(), err)
	}
}

// The target is swapped while herdr answers the first tab create, which loses
// its workspace: the spawn recreates the workspace, and the check before the
// second create refuses. The tab that came with the new workspace is closed.
func TestTargetSwappedBeforeTabCreateRefused_t6367(t *testing.T) {
	for _, tc := range []struct {
		name string
		swap func(t *testing.T, l *laneFixture, lane string)
	}{
		{"another directory", func(t *testing.T, _ *laneFixture, lane string) {
			if err := os.Rename(lane, lane+".old"); err != nil {
				t.Fatal(err)
			}
			mkdirs(t, lane)
		}},
		{"a link to a sibling job", func(t *testing.T, l *laneFixture, lane string) {
			if err := os.Rename(lane, lane+".old"); err != nil {
				t.Fatal(err)
			}
			symlink(t, l.other, lane)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newLaneFixture(t)
			lane := filepath.Join(l.w, "live")
			mkdirs(t, lane)
			l.reportPaneAt(lane)
			l.s.Workspace = "workers"
			lost, created := false, false
			previous := l.f.Do
			l.f.Do = func(c Command) Result {
				switch args := strings.Join(c.Args, " "); {
				case c.Name != "herdr":
				case args == "workspace list" && !lost:
					return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w1","label":"workers"}]}}`}
				case args == "workspace list" && !created:
					return Result{Out: noWorkspaces}
				case args == "workspace list":
					return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w2","label":"workers"}]}}`}
				case args == "workspace create --label workers":
					created = true
					return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"},"tab":{"tab_id":"w2:t1"},"root_pane":{"pane_id":"w2:p0"}}}`}
				case strings.HasPrefix(args, "tab create") && !lost:
					lost = true
					tc.swap(t, l, lane)
					return Result{Code: 1, Err: "workspace_not_found"}
				}
				return previous(c)
			}
			retrySafe := false
			l.a.ObserveRetryable = func(safe bool) { retrySafe = safe }
			err := l.launch(t.Context(), l.bind(t))
			if ErrorCode(err) != exitStateConflict || !strings.Contains(err.Error(), "target_changed:") {
				t.Fatalf("err=%v, want target_changed", err)
			}
			if dirs := l.tabDirs(); len(dirs) != 1 {
				t.Fatalf("tab creates %q, want only the one that lost its workspace", dirs)
			}
			if closed := l.herdrCalls("tab close"); !slices.Equal(closed, []string{"tab close w2:t1"}) {
				t.Fatalf("closed %q, want the created workspace's tab", closed)
			}
			if !retrySafe {
				t.Fatal("a refusal that created no pane was not safe to retry")
			}
		})
	}
}

// A target already swapped when trust is granted refuses right after the grant,
// as target_changed even when no launch window is left, and starts no tab.
func TestTargetSwappedBeforeTrustRefused_t6404(t *testing.T) {
	l := newLaneFixture(t)
	lane := filepath.Join(l.w, "live")
	mkdirs(t, lane)
	l.s.Workspace = "workers"
	created := false
	previous := l.f.Do
	l.f.Do = func(c Command) Result {
		switch args := strings.Join(c.Args, " "); {
		case c.Name != "herdr":
		case args == "workspace list" && !created:
			return Result{Out: noWorkspaces}
		case args == "workspace list":
			return Result{Out: `{"result":{"workspaces":[{"workspace_id":"w2","label":"workers"}]}}`}
		case args == "workspace create --label workers":
			created = true
			if err := os.Rename(lane, lane+".old"); err != nil {
				t.Error(err)
			}
			mkdirs(t, lane)
			return Result{Out: `{"result":{"workspace":{"workspace_id":"w2"},"tab":{"tab_id":"w2:t1"},"root_pane":{"pane_id":"w2:p0"}}}`}
		}
		return previous(c)
	}
	var stages []string
	l.a.ObserveStage = func(stage string, _ time.Duration) { stages = append(stages, stage) }
	// Less than the brief reserve: the launch-window refusal comes next.
	ctx, cancel := context.WithTimeout(t.Context(), briefSubmitReserve-time.Second)
	defer cancel()
	err := l.launch(ctx, l.bind(t))
	if ErrorCode(err) != exitStateConflict || !strings.Contains(err.Error(), "target_changed:") {
		t.Fatalf("err=%v, want target_changed", err)
	}
	if len(l.tabDirs()) != 0 || !slices.Equal(l.herdrCalls("tab close"), []string{"tab close w2:t1"}) {
		t.Fatalf("tab creates %q, closed %q", l.tabDirs(), l.herdrCalls("tab close"))
	}
	if len(stages) == 0 || stages[len(stages)-1] != "launch" {
		t.Fatalf("stages %q, want the refusal in the launch stage", stages)
	}
}

func TestLauncherDoesNotRejoinLane_t6367(t *testing.T) {
	l := newLaneFixture(t)
	lane := filepath.Join(l.w, "live")
	mkdirs(t, filepath.Join(lane, "live"))
	l.reportPaneAt(lane)
	if err := l.launch(t.Context(), l.bind(t)); err != nil {
		t.Fatal(err)
	}
	if dirs := l.tabDirs(); !slices.Equal(dirs, []string{lane}) {
		t.Fatalf("tab create --cwd %q, want %q", dirs, lane)
	}
}

// A cwd_map spawn binds nothing: the launcher still enters its lane, and a
// local spawn does the same.
func TestMappedSpawnKeepsLauncherLaneJoin_t6404(t *testing.T) {
	l := newLaneFixture(t)
	lane := filepath.Join(l.w, "live")
	mkdirs(t, lane)
	l.reportPaneAt(lane)
	if err := l.launch(t.Context(), LaunchDir{Path: l.w}); err != nil {
		t.Fatal(err)
	}
	if dirs := l.tabDirs(); !slices.Equal(dirs, []string{lane}) {
		t.Fatalf("tab create --cwd %q, want %q", dirs, lane)
	}
}

// Skipping the join does not skip the -L value check.
func TestBoundLaunchStillRefusesUnknownLane_t6404(t *testing.T) {
	l := newLaneFixture(t)
	l.s.ExecutionLane = "other"
	err := l.launch(t.Context(), l.bind(t))
	if ErrorCode(err) != exitUsage || !strings.Contains(err.Error(), "-L must be live or mock") || len(l.tabDirs()) != 0 {
		t.Fatalf("err=%v tabs=%q", err, l.tabDirs())
	}
}

// The hub classifies the node's refusal text; the path in it must not hand the
// refusal to a broader row.
func TestTargetChangedRefusalCode_t6404(t *testing.T) {
	err := LaunchDir{Path: "x", bound: true}.verify("/work/trust-brief-workspace/job")
	if got := SpawnRefusalCode(err.Error(), ErrorCode(err)); got != "target_changed" {
		t.Fatalf("refusal code %q for %v", got, err)
	}
	if fix := SpawnRefusalFix(err); !strings.Contains(fix, "launch directory changed") {
		t.Fatalf("fix %q", fix)
	}
}
