package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const oneWorkersWorkspace = `{"result":{"workspaces":[{"workspace_id":"w1","label":"workers"}]}}`

// testLockRoot is a workspace lock root two levels below a temp dir, as the
// default is below /tmp: both levels are made by the code under test.
func testLockRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "panewire", "herdr-locks")
}

// workspaceLockFile is the lock file name both lock paths use, spelled out
// here because a binary before hk 6434 computes it this way.
func workspaceLockFile(session, label string) string {
	sum := sha256.Sum256([]byte(session + "\x00" + label))
	return hex.EncodeToString(sum[:8]) + ".lock"
}

// openLockFile opens a lock path as another process would, on its own file
// description.
func openLockFile(t *testing.T, path string) *os.File {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// Writers with different HOME and state directories share the host-fixed lock,
// so an absent label is created once (hk 4821 W1c).
func TestResolveWorkspaceLockSharedAcrossStateDirs_t6367(t *testing.T) {
	const k = 8
	root := testLockRoot(t)
	var mu sync.Mutex
	var workspaces []string
	creates, absent := 0, 0
	gathered := make(chan struct{})
	runner := &fakeRunner{Do: func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch args {
		case "workspace list":
			mu.Lock()
			rows := make([]string, 0, len(workspaces))
			for _, id := range workspaces {
				rows = append(rows, `{"workspace_id":"`+id+`","label":"workers"}`)
			}
			if len(rows) == 0 {
				if absent++; absent == k {
					close(gathered)
				}
			}
			mu.Unlock()
			if len(rows) == 0 {
				// A window that proves nothing more happens: every resolve that
				// can see the label absent at the same time arrives here before
				// any of them creates.
				select {
				case <-gathered:
				case <-time.After(500 * time.Millisecond):
				}
			}
			return Result{Out: `{"result":{"workspaces":[` + strings.Join(rows, ",") + `]}}`}
		case "workspace create --label workers":
			mu.Lock()
			defer mu.Unlock()
			creates++
			id := "w" + strconv.Itoa(creates)
			workspaces = append(workspaces, id)
			return Result{Out: `{"result":{"workspace":{"workspace_id":"` + id + `"},"tab":{"tab_id":"` + id + `:t1"},"root_pane":{"pane_id":"` + id + `:p0"}}}`}
		}
		t.Errorf("unexpected herdr call: %v", c.Args)
		return Result{}
	}}
	apps := make([]*App, k)
	homes, states := map[string]bool{}, map[string]bool{}
	for i := range apps {
		a := testApp(t)
		a.HerdrSession, a.StateDir, a.workspaceLockRoot = "worker", t.TempDir(), root
		a.Runner = runner
		apps[i], homes[a.Home], states[a.StateDir] = a, true, true
	}
	if len(homes) != k || len(states) != k {
		t.Fatalf("fixture: %d homes and %d state dirs, want %d of each", len(homes), len(states), k)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := make(chan struct{})
	got := make([]string, k)
	var wg sync.WaitGroup
	for i, a := range apps {
		wg.Go(func() {
			<-start
			id, _, err := a.resolveWorkspace(ctx, "workers")
			if err != nil {
				t.Errorf("resolve %d: %v", i, err)
			}
			got[i] = id
		})
	}
	close(start)
	wg.Wait()
	if creates != 1 {
		t.Fatalf("creates=%d workspaces=%v, want one create", creates, got)
	}
	for i, id := range got {
		if id != "w1" {
			t.Fatalf("resolve %d got %q, want w1 (all %v)", i, id, got)
		}
	}
}

func TestDefaultWorkspaceLockRootIgnoresEnv_t6404(t *testing.T) {
	uid := os.Getuid()
	want := "/tmp/panewire-" + strconv.Itoa(uid) + "/herdr-locks"
	for _, set := range []string{"/one", "/two/deeper", ""} {
		for _, name := range []string{"HOME", "XDG_STATE_HOME", "TMPDIR"} {
			value := set
			if value != "" {
				value = filepath.Join(set, strings.ToLower(name))
			}
			t.Setenv(name, value)
		}
		if got := defaultWorkspaceLockRoot(uid); got != want {
			t.Fatalf("env %q: root=%q, want %q", set, got, want)
		}
		// An App without the test override uses it, whatever its own state
		// directory. Nothing is created here.
		a := &App{Home: "/home-" + set, StateDir: set}
		if got := a.workspaceLockDir(); got != want {
			t.Fatalf("env %q: app lock dir=%q, want %q", set, got, want)
		}
	}
	if got := defaultWorkspaceLockRoot(0); got != "/tmp/panewire-0/herdr-locks" {
		t.Fatalf("uid 0 root=%q", got)
	}
}

// During a rollout a binary before hk 6434 holds only the state-dir lock. The
// two exclude each other, and a new writer waits for it already holding the
// host-fixed lock: that is the fixed order.
func TestWorkspaceLockNewAndLegacyHoldersExclude_t6404(t *testing.T) {
	t.Run("legacy holder first", func(t *testing.T) {
		a := testApp(t)
		a.HerdrSession = "worker"
		var calls atomic.Int32
		a.Runner = &fakeRunner{Do: func(Command) Result {
			calls.Add(1)
			return Result{Out: oneWorkersWorkspace}
		}}
		name := workspaceLockFile("worker", "workers")
		legacy := openLockFile(t, filepath.Join(a.stateDir(), "workspace-locks", name))
		if !tryLock(legacy) {
			t.Fatal("fixture: legacy lock not free")
		}
		short, cancelShort := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancelShort()
		if _, _, err := a.resolveWorkspace(short, "workers"); err == nil {
			t.Fatal("resolved while a legacy holder had the lock")
		}
		opened := make(chan string, 1)
		a.workspaceLockOpened = func(path string) { opened <- path }
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { _, _, err := a.resolveWorkspace(ctx, "workers"); done <- err }()
		select {
		case <-opened:
		case <-time.After(5 * time.Second):
			t.Fatal("the waiter never reached the host-fixed lock while the legacy lock was held")
		}
		// A window that proves nothing more happens while the legacy lock is held.
		select {
		case err := <-done:
			t.Fatalf("resolved while a legacy holder had the lock: %v", err)
		case <-time.After(300 * time.Millisecond):
		}
		if n := calls.Load(); n != 0 {
			t.Fatalf("%d herdr calls while the legacy lock was held", n)
		}
		// The fixed order: a waiter for the legacy lock already holds the
		// host-fixed one.
		if tryLock(openLockFile(t, filepath.Join(a.workspaceLockRoot, name))) {
			t.Fatal("the waiter does not hold the host-fixed lock while it waits for the legacy one")
		}
		if err := syscall.Flock(int(legacy.Fd()), syscall.LOCK_UN); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatalf("resolve after release: %v", err)
		}
	})
	t.Run("new holder first", func(t *testing.T) {
		a := testApp(t)
		a.HerdrSession = "worker"
		entered, release := make(chan struct{}), make(chan struct{})
		a.Runner = &fakeRunner{Do: func(Command) Result {
			close(entered)
			<-release
			return Result{Out: oneWorkersWorkspace}
		}}
		done := make(chan error, 1)
		go func() { _, _, err := a.resolveWorkspace(context.Background(), "workers"); done <- err }()
		<-entered
		legacy := openLockFile(t, filepath.Join(a.stateDir(), "workspace-locks", workspaceLockFile("worker", "workers")))
		held := !tryLock(legacy)
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !held {
			t.Fatal("a legacy-path flock succeeded while a new writer was resolving")
		}
		if !tryLock(legacy) {
			t.Fatal("the legacy lock is still held after the resolve returned")
		}
	})
}

func TestWorkspaceLockOrderNoDeadlock_t6404(t *testing.T) {
	const resolvers, legacyLockers, rounds = 16, 4, 3
	root, state := testLockRoot(t), t.TempDir()
	runner := &fakeRunner{Do: func(Command) Result {
		time.Sleep(time.Millisecond) // simulated latency: holds overlap
		return Result{Out: oneWorkersWorkspace}
	}}
	apps := make([]*App, resolvers)
	for i := range apps {
		a := testApp(t)
		a.HerdrSession, a.StateDir, a.workspaceLockRoot, a.Runner = "worker", state, root, runner
		a.Out = &bytes.Buffer{}
		apps[i] = a
	}
	legacyPath := filepath.Join(state, "workspace-locks", workspaceLockFile("worker", "workers"))
	files := make([]*os.File, legacyLockers)
	for i := range files {
		files[i] = openLockFile(t, legacyPath)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i, a := range apps {
		wg.Go(func() {
			if _, _, err := a.resolveWorkspace(ctx, "workers"); err != nil {
				t.Errorf("resolve %d: %v", i, err)
			}
		})
	}
	for i, f := range files {
		wg.Go(func() {
			for range rounds {
				if err := waitLock(ctx, f); err != nil {
					t.Errorf("legacy locker %d: %v", i, err)
					return
				}
				time.Sleep(time.Millisecond) // simulated latency: a legacy hold
				if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
					t.Errorf("legacy unlock %d: %v", i, err)
				}
			}
		})
	}
	wg.Wait()
}

func TestWorkspaceLockRefreshesTimesOnAcquire_t6404(t *testing.T) {
	a := testApp(t)
	a.HerdrSession, a.workspaceLockRoot = "worker", testLockRoot(t)
	release, err := a.lockWorkspace(context.Background(), "workers")
	if err != nil {
		t.Fatal(err)
	}
	release()
	paths := []string{
		filepath.Join(a.workspaceLockRoot, workspaceLockFile("worker", "workers")),
		a.workspaceLockRoot,
		filepath.Dir(a.workspaceLockRoot),
	}
	old := time.Now().Add(-30 * 24 * time.Hour)
	for _, path := range paths {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	// Truncated for a filesystem with coarse timestamps.
	start := time.Now().Truncate(time.Second)
	if release, err = a.lockWorkspace(context.Background(), "workers"); err != nil {
		t.Fatal(err)
	}
	defer release()
	for _, path := range paths {
		var st unix.Stat_t
		if err := unix.Lstat(path, &st); err != nil {
			t.Fatal(err)
		}
		atime, mtime := time.Unix(st.Atim.Unix()), time.Unix(st.Mtim.Unix())
		if atime.Before(start) || mtime.Before(start) {
			t.Errorf("%s: atime=%s mtime=%s, want both at or after %s", path, atime, mtime, start)
		}
	}
}

func TestWorkspaceLockRetriesStaleFd_t6404(t *testing.T) {
	for name, disturb := range map[string]func(path string) error{
		"replaced": func(path string) error {
			if err := os.WriteFile(path+".new", nil, 0o600); err != nil {
				return err
			}
			return os.Rename(path+".new", path)
		},
		"removed": os.Remove,
	} {
		t.Run(name, func(t *testing.T) {
			a := testApp(t)
			a.workspaceLockRoot = testLockRoot(t)
			opens := 0
			a.workspaceLockOpened = func(path string) {
				if opens++; opens == 1 {
					if err := disturb(path); err != nil {
						t.Error(err)
					}
				}
			}
			lock, err := a.lockHostWorkspace(context.Background(), "x.lock")
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Close()
			held, err := lock.Stat()
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(a.workspaceLockRoot, "x.lock")
			named, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(held, named) {
				t.Fatal("the held file is not the one at the path")
			}
			if tryLock(openLockFile(t, path)) {
				t.Fatal("another description locked the path while it was held")
			}
			if opens != 2 {
				t.Fatalf("opens=%d, want the stale one and its retry", opens)
			}
		})
	}
	t.Run("replaced every time", func(t *testing.T) {
		a := testApp(t)
		a.HerdrSession = "worker"
		a.Runner = &fakeRunner{Do: func(c Command) Result {
			t.Errorf("herdr %v ran without the lock", c.Args)
			return Result{}
		}}
		opens := 0
		a.workspaceLockOpened = func(path string) {
			opens++
			if err := os.Remove(path); err != nil {
				t.Error(err)
			}
		}
		_, _, err := a.resolveWorkspace(context.Background(), "workers")
		if exitCode(err) != exitConfig || !strings.Contains(err.Error(), "replaced or removed") {
			t.Fatalf("exit=%d err=%v", exitCode(err), err)
		}
		if opens != workspaceLockAttempts {
			t.Fatalf("opens=%d, want %d", opens, workspaceLockAttempts)
		}
	})
}

func TestWorkspaceLockRootSquatRefused_t6404(t *testing.T) {
	mkdir := func(t *testing.T, path string, mode os.FileMode) {
		t.Helper()
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		// Chmod, so the umask does not narrow the fixture's mode.
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}
	link := func(t *testing.T, path string) string {
		t.Helper()
		target := filepath.Join(t.TempDir(), "target")
		mkdir(t, target, 0o700)
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
		return target
	}
	refused := func(t *testing.T, root, named string) {
		t.Helper()
		a := testApp(t)
		a.HerdrSession, a.workspaceLockRoot = "worker", root
		a.Runner = &fakeRunner{Do: func(c Command) Result {
			t.Errorf("herdr %v ran under an unsafe lock root", c.Args)
			return Result{}
		}}
		_, _, err := a.resolveWorkspace(context.Background(), "workers")
		if err == nil {
			t.Fatal("an unsafe lock root was accepted")
		}
		for _, want := range []string{
			"workspace_lock_root_unsafe: workspace lock root " + named + " is not a private directory of uid " + strconv.Itoa(os.Getuid()),
			"remove it as its owner or root, then retry",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("err=%v, want %q", err, want)
			}
		}
		if exitCode(err) != exitConfig {
			t.Errorf("exit=%d, want %d", exitCode(err), exitConfig)
		}
		if got := SpawnRefusalCode(err.Error(), exitCode(err)); got != "workspace_lock_root_unsafe" {
			t.Errorf("refusal code=%q", got)
		}
	}
	t.Run("lock dir is a link", func(t *testing.T) {
		root := testLockRoot(t)
		mkdir(t, filepath.Dir(root), 0o700)
		link(t, root)
		refused(t, root, root)
	})
	t.Run("lock dir is world-writable", func(t *testing.T) {
		root := testLockRoot(t)
		mkdir(t, filepath.Dir(root), 0o700)
		mkdir(t, root, 0o777)
		refused(t, root, root)
	})
	t.Run("parent is a link", func(t *testing.T) {
		root := testLockRoot(t)
		target := link(t, filepath.Dir(root))
		refused(t, root, filepath.Dir(root))
		if entries, err := os.ReadDir(target); err != nil || len(entries) != 0 {
			t.Fatalf("entries=%v err=%v, want nothing made through the link", entries, err)
		}
	})
	t.Run("parent is group-writable", func(t *testing.T) {
		root := testLockRoot(t)
		mkdir(t, filepath.Dir(root), 0o770)
		refused(t, root, filepath.Dir(root))
	})
	t.Run("another owner", func(t *testing.T) {
		if os.Getuid() != 0 {
			t.Skip("giving a directory to another uid needs root")
		}
		root := testLockRoot(t)
		mkdir(t, filepath.Dir(root), 0o700)
		mkdir(t, root, 0o700)
		if err := os.Chown(root, 65534, 65534); err != nil {
			t.Skipf("this root cannot chown (user namespace): %v", err)
		}
		refused(t, root, root)
	})
}

// duplicateHerdr answers a spawn's resolve of the absent label "workers" while
// other writers create it too: after this call's create (workspace w3, tab
// w3:t1) herdr also holds the foreign workspaces. It records every close.
type duplicateHerdr struct {
	*fakeHerdr
	foreign []string
	// refuseTabClose, when set, is herdr's answer to every tab close.
	refuseTabClose string
	// afterTabClose runs once the tab close was answered.
	afterTabClose func()
	created       bool
	lists         int
	closes        []string
}

func (h *duplicateHerdr) run(t *testing.T) func(Command) Result {
	return func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch args {
		case "workspace list":
			h.lists++
			if !h.created {
				return Result{Out: noWorkspaces}
			}
			return Result{Out: h.list()}
		case "workspace create --label workers":
			h.created = true
			h.add("w3", "w3:t1")
			for _, id := range h.foreign {
				h.add(id, id+":t1")
			}
			return Result{Out: `{"result":{"workspace":{"workspace_id":"w3"},"tab":{"tab_id":"w3:t1"},"root_pane":{"pane_id":"w3:t1-pane"}}}`}
		}
		switch {
		case strings.HasPrefix(args, "tab close "):
			h.closes = append(h.closes, args)
			if h.afterTabClose != nil {
				defer h.afterTabClose()
			}
			if h.refuseTabClose != "" {
				return Result{Code: 1, Out: h.refuseTabClose}
			}
			h.closeTab(strings.TrimPrefix(args, "tab close "))
			return Result{}
		case strings.HasPrefix(args, "pane close "):
			h.closes = append(h.closes, args)
			h.closePane(strings.TrimPrefix(args, "pane close "))
			return Result{}
		case strings.HasPrefix(args, "workspace close "):
			h.closes = append(h.closes, args)
			h.closeWorkspace(strings.TrimPrefix(args, "workspace close "))
			return Result{}
		}
		t.Errorf("unexpected herdr call: %v", c.Args)
		return Result{}
	}
}

func resolveWithDuplicates(t *testing.T, h *duplicateHerdr) (*App, string, *createdWorkspace, error) {
	t.Helper()
	a := testApp(t)
	a.Runner = &fakeRunner{Do: h.run(t)}
	id, created, err := a.resolveWorkspace(context.Background(), "workers")
	return a, id, created, err
}

func TestCreateWorkspaceAdoptsSoleRemainingAfterOwnClose_t6404(t *testing.T) {
	h := &duplicateHerdr{fakeHerdr: newFakeHerdr(), foreign: []string{"w4"}}
	a, id, created, err := resolveWithDuplicates(t, h)
	if err != nil || id != "w4" || created != nil {
		t.Fatalf("workspace=%q created=%+v err=%v, want the foreign w4 adopted with no created tab", id, created, err)
	}
	if got := strings.Join(h.closes, ";"); got != "tab close w3:t1" || h.lists != 3 {
		t.Fatalf("closes=%q lists=%d", got, h.lists)
	}
	if out := a.Out.(*bytes.Buffer).String(); !strings.Contains(out, "workspace_label=workers workspace=w4 after_duplicate_create=w3") || strings.Contains(out, "workspace_created") {
		t.Fatalf("log=%q", out)
	}
}

func TestCreateWorkspaceRefusesDuplicateAfterCreate_t6367(t *testing.T) {
	h := &duplicateHerdr{fakeHerdr: newFakeHerdr(), foreign: []string{"w4", "w5"}}
	_, id, created, err := resolveWithDuplicates(t, h)
	want := `workspace "workers" matched 2 entries; run herdr workspace list on the executing host and pass -w WORKSPACE_ID`
	if err == nil || err.Error() != want || exitCode(err) != exitUsage || id != "" || created != nil {
		t.Fatalf("workspace=%q created=%+v exit=%d err=%v", id, created, exitCode(err), err)
	}
	if got := strings.Join(h.closes, ";"); got != "tab close w3:t1" || h.lists != 3 {
		t.Fatalf("closes=%q lists=%d", got, h.lists)
	}
}

func TestResolveWorkspaceNeverClosesForeignTab_t6367(t *testing.T) {
	for _, test := range []struct {
		name    string
		foreign []string
		refuse  string
		closes  string
	}{
		{"adopts", []string{"w4"}, "", "tab close w3:t1"},
		{"refuses", []string{"w4", "w5"}, "", "tab close w3:t1"},
		{"old herdr last tab", []string{"w4"}, `{"error":{"message":"cannot close the last tab in a workspace"}}`, "tab close w3:t1;pane close w3:t1-pane"},
		{"close fails", []string{"w4"}, `{"error":{"code":"internal"}}`, "tab close w3:t1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := &duplicateHerdr{fakeHerdr: newFakeHerdr(), foreign: test.foreign, refuseTabClose: test.refuse}
			resolveWithDuplicates(t, h)
			if got := strings.Join(h.closes, ";"); got != test.closes {
				t.Fatalf("closes=%q, want %q", got, test.closes)
			}
			for _, id := range test.foreign {
				if len(h.tabs[id]) != 1 {
					t.Fatalf("foreign workspace %s tabs=%v", id, h.tabs[id])
				}
			}
		})
	}
}

// A close that fails leaves this call's tab: it is warned about with its id,
// and stays reported to the spawn only while the spawn goes on.
func TestCreateWorkspaceOwnCloseFails_t6404(t *testing.T) {
	const failure = `{"error":{"code":"internal"}}`
	t.Run("duplicate remains", func(t *testing.T) {
		h := &duplicateHerdr{fakeHerdr: newFakeHerdr(), foreign: []string{"w4"}, refuseTabClose: failure}
		a, id, created, err := resolveWithDuplicates(t, h)
		if exitCode(err) != exitUsage || !strings.Contains(err.Error(), "matched 2 entries") || id != "" || created != nil {
			t.Fatalf("workspace=%q created=%+v err=%v", id, created, err)
		}
		if warned := a.Err.(*bytes.Buffer).String(); !strings.Contains(warned, "close tab w3:t1 (workspace w3)") {
			t.Fatalf("warnings=%q", warned)
		}
	})
	t.Run("only its own remains", func(t *testing.T) {
		h := &duplicateHerdr{fakeHerdr: newFakeHerdr(), foreign: []string{"w4"}, refuseTabClose: failure}
		h.afterTabClose = func() { h.closeWorkspace("w4") }
		_, id, created, err := resolveWithDuplicates(t, h)
		if err != nil || id != "w3" || created == nil || created.tab != "w3:t1" {
			t.Fatalf("workspace=%q created=%+v err=%v, want its own w3 still reported", id, created, err)
		}
	})
	t.Run("nothing remains", func(t *testing.T) {
		h := &duplicateHerdr{fakeHerdr: newFakeHerdr(), foreign: []string{"w4"}}
		h.afterTabClose = func() { h.closeWorkspace("w4") }
		_, id, created, err := resolveWithDuplicates(t, h)
		if exitCode(err) != exitUsage || !strings.Contains(err.Error(), "matched 0 entries") || id != "" || created != nil {
			t.Fatalf("workspace=%q created=%+v err=%v", id, created, err)
		}
	})
}

// The spawn's context may end between the create and the close: its own tab is
// still removed.
func TestCreateWorkspaceClosesOwnTabAfterContextEnds_t6404(t *testing.T) {
	h := &duplicateHerdr{fakeHerdr: newFakeHerdr(), foreign: []string{"w4"}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := testApp(t)
	inner := h.run(t)
	runner := &contextRunner{do: func(callCtx context.Context, c Command) Result {
		if callCtx.Err() != nil {
			return Result{Code: 1, Err: callCtx.Err().Error()}
		}
		result := inner(c)
		if h.created && h.lists == 2 && c.Args[1] == "list" {
			cancel()
		}
		return result
	}}
	a.Runner = runner
	_, _, err := a.resolveWorkspace(ctx, "workers")
	if err == nil {
		t.Fatal("resolved on an ended context")
	}
	if got := strings.Join(h.closes, ";"); got != "tab close w3:t1" {
		t.Fatalf("closes=%q, want the own tab removed", got)
	}
}

type contextRunner struct {
	do func(context.Context, Command) Result
}

func (r *contextRunner) Run(ctx context.Context, c Command) Result { return r.do(ctx, c) }
