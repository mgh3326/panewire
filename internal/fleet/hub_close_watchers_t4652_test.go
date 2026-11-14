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
