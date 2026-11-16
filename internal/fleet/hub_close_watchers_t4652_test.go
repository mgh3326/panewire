package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func listing(lines string) ProcessLister {
	return func(context.Context) ([]byte, error) { return []byte(lines), nil }
}

// Every live `_watch <job>` process under the jobs root must carry its own
// pid's token; an older binary never writes one, locked or not.
func TestHubCloseWatchersReady_t4652(t *testing.T) {
	jobs := t.TempDir()
	if e := os.MkdirAll(filepath.Join(jobs, "job-a", watchCapsDir), 0o700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(jobs, "job-a", watchCapsDir, "101"), []byte(WatcherCapsToken+"\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	for name, test := range map[string]struct {
		ps   string
		want bool
	}{
		"aware watcher":           {"  101 /usr/local/bin/panewire _watch job-a --rearmed\n", true},
		"older binary, same job":  {"  101 /usr/local/bin/panewire _watch job-a\n  202 /old/panewire _watch job-a\n", false},
		"stale token, other pid":  {"  303 /old/panewire _watch job-a\n", false},
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

// A watcher of this binary writes its token before taking any lock and
// removes it when it ends.
func TestWatcherRecordsItsCapabilityToken_t4652(t *testing.T) {
	a, _ := hubCloseFixture(t)
	path := filepath.Join(a.Jobs, "j", watchCapsDir, strconv.Itoa(os.Getpid()))
	sleep := a.Sleep
	seen := false
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		if b, e := os.ReadFile(path); e == nil && string(b) == WatcherCapsToken+"\n" {
			seen = true
		}
		return sleep(ctx, d)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if !seen {
		t.Fatal("no capability token while the watcher ran")
	}
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatalf("token left after the watcher ended: %v", e)
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

// #572 r4575956172, r4575962305: a launcher that can start a watcher later
// must carry this binary's launcher token; an older one never writes it.
func TestHubCloseWatchersReadyChecksLaunchers_t4632(t *testing.T) {
	jobs := t.TempDir()
	if e := os.MkdirAll(filepath.Join(jobs, launcherCapsDir), 0o700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(jobs, launcherCapsDir, "606"), []byte(LauncherCapsToken+"\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	for name, test := range map[string]struct {
		ps   string
		want bool
	}{
		"aware launcher":          {"  606 /usr/local/bin/panewire spawn -p b\n", true},
		"older spawn":             {"  707 /old/panewire spawn -p b\n", false},
		"older work":              {"  708 panewire work 12\n", false},
		"older remote stdin":      {"  709 panewire _spawn-stdin\n", false},
		"non-launcher subcommand": {"  710 /old/panewire emit --kind x\n", true},
		"spawn word elsewhere":    {"  711 vim spawn notes\n", true},
	} {
		got, err := HubCloseWatchersReady(context.Background(), jobs, listing(test.ps))
		if err != nil || got != test.want {
			t.Errorf("%s: ready=%v err=%v, want %v", name, got, err, test.want)
		}
	}
}

// A launcher of this binary writes its token while it runs and removes it.
func TestLauncherRecordsItsCapabilityToken_t4632(t *testing.T) {
	a := &App{Jobs: t.TempDir()}
	path := filepath.Join(a.Jobs, launcherCapsDir, strconv.Itoa(os.Getpid()))
	remove := a.recordLauncherCaps()
	if b, e := os.ReadFile(path); e != nil || string(b) != LauncherCapsToken+"\n" {
		t.Fatalf("token %q, %v", b, e)
	}
	remove()
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatalf("token left: %v", e)
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
	if !CloseOpsReady(context.Background(), jobs, clean) {
		t.Fatal("not ready after the barrier")
	}
}
