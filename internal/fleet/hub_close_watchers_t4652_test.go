package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func listing(lines string) ProcessLister {
	return func(context.Context) ([]byte, error) { return []byte(lines), nil }
}

// holdLock takes name in dir for the test's life, as a running process would.
func holdLock(t *testing.T, dir, name string) {
	t.Helper()
	if e := os.MkdirAll(dir, 0o700); e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_RDWR, 0o600)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.Close() })
	if !tryLock(f) {
		t.Fatalf("%s already held", name)
	}
}

// Every live `_watch <job>` process under the jobs root must be the job's
// aware watcher: it holds the v2 hub-close lock, which only this binary
// takes and which lives exactly as long as its process.
func TestHubCloseWatchersReady_t4652(t *testing.T) {
	jobs := t.TempDir()
	holdLock(t, filepath.Join(jobs, "job-a"), jobWatchLock)
	holdLock(t, filepath.Join(jobs, "job-a"), hubCloseWatchLock)
	holdLock(t, filepath.Join(jobs, "job-v1"), jobWatchLock)
	holdLock(t, filepath.Join(jobs, "job-v1"), hubCloseWatchLockV1)
	holdLock(t, filepath.Join(jobs, "job-old"), jobWatchLock)
	if e := os.MkdirAll(filepath.Join(jobs, "job-gone"), 0o700); e != nil {
		t.Fatal(e)
	}
	for name, test := range map[string]struct {
		ps   string
		want bool
	}{
		"aware watcher":           {"  101 /usr/local/bin/panewire _watch job-a --rearmed\n", true},
		"renamed binary, aware":   {"  102 /usr/local/bin/panewire.prev _watch job-a\n", true},
		"v1 watcher":              {"  202 /old/panewire _watch job-v1\n", false},
		"older watcher":           {"  203 /old/panewire _watch job-old\n", false},
		"_watch holding no lock":  {"  204 /old/panewire _watch job-gone\n", false},
		"another root's job":      {"  404 /usr/local/bin/panewire _watch job-zz\n", true},
		"no watchers":             {"  1 /sbin/init\n", true},
		"unrelated _watch string": {"  505 vim _watch\n", true},
	} {
		got, err := HubCloseWatchersReady(context.Background(), jobs, listing(test.ps))
		if err != nil || got != test.want {
			t.Errorf("%s: ready=%v err=%v, want %v", name, got, err, test.want)
		}
	}
}

// A running watcher of this binary holds the v2 lock: the probe reads it as
// aware while it runs, and as no watcher once it ended.
func TestWatcherHoldsTheV2Lock_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	dir := filepath.Join(a.Jobs, "j")
	sleep := a.Sleep
	seen := false
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		if w, e := ProbeHubCloseWatcher(dir); e == nil && w == HubCloseWatcherAware {
			seen = true
		}
		return sleep(ctx, d)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if !seen {
		t.Fatal("watcher not aware while it ran")
	}
	if w, e := ProbeHubCloseWatcher(dir); e != nil || w != HubCloseWatcherNone {
		t.Fatalf("after the watcher: %v %v, want none", w, e)
	}
}

// The node's fence keeps every watcher out: a watcher started while it is
// held exits at once and writes nothing; once released one runs.
func TestFenceKeepsWatchersOut_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	dir := filepath.Join(a.Jobs, "j")
	release, ok, err := FenceWatchers(dir)
	if err != nil || !ok {
		t.Fatalf("fence: ok=%v err=%v", ok, err)
	}
	before := len(eventKinds(t, a))
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if after := len(eventKinds(t, a)); after != before {
		t.Fatalf("fenced watcher wrote %d events", after-before)
	}
	if _, ok2, _ := FenceWatchers(dir); ok2 {
		t.Fatal("a second fence was taken while the first is held")
	}
	release()
	if w, e := ProbeHubCloseWatcher(dir); e != nil || w != HubCloseWatcherNone {
		t.Fatalf("after release: %v %v", w, e)
	}
}

// A listing the scanner cannot read to its end (a line over its limit) is not
// ready: the rest of the processes were never checked.
func TestHubCloseWatchersReadyFailsClosedOnAScanError_t4652(t *testing.T) {
	long := "  1 /bin/sh " + strings.Repeat("x", 2<<20) + "\n  202 /old/panewire _watch job-a\n"
	got, err := HubCloseWatchersReady(context.Background(), t.TempDir(), listing(long))
	if got || err == nil {
		t.Fatalf("ready=%v err=%v, want not ready with an error", got, err)
	}
}

// Advertising refuses without the barrier record.
func TestAdvertiseNeedsTheBarrier_t4632(t *testing.T) {
	if e := AdvertiseCloseOps(t.TempDir(), time.Now()); e != ErrCloseOpsNoBarrier {
		t.Fatalf("advertise without the barrier: %v", e)
	}
}

// The barrier is not ready without its record, even with no older process.
func TestCloseOpsReadyNeedsTheBarrier_t4632(t *testing.T) {
	jobs := t.TempDir()
	clean := listing("  1 /sbin/init\n")
	if CloseOpsReady(context.Background(), jobs, clean) {
		t.Fatal("ready without the barrier record")
	}
	if e := EnableCloseOps(context.Background(), jobs, listing("  9 /old/panewire _watch "+filepath.Base(t.TempDir())+"\n"), time.Now()); e != nil {
		t.Fatalf("a watcher of another root blocked the barrier: %v", e)
	}
	if CloseOpsReady(context.Background(), jobs, clean) {
		t.Fatal("ready after the barrier alone: advertising is off by default")
	}
	if e := AdvertiseCloseOps(jobs, time.Now()); e != nil {
		t.Fatal(e)
	}
	if !CloseOpsReady(context.Background(), jobs, clean) {
		t.Fatal("not ready after the barrier and the advertise step")
	}
}
