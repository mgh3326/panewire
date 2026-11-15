package fleet

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// testStart is the start time a test listing gives every process.
const testStart = "Sun Nov 15 12:00:00 2026"

// withStart inserts testStart after each line's pid, the column ps's lstart
// prints there; a line without a leading pid, or with its own start time
// (a year field), is kept as is.
func withStart(lines string) string {
	out := strings.SplitAfter(lines, "\n")
	pid := regexp.MustCompile(`^(\s*\d+) `)
	stamped := regexp.MustCompile(`^\s*\d+ \w{3} \w{3} +\d+ \d\d:\d\d:\d\d \d{4} `)
	for i, line := range out {
		if !stamped.MatchString(line) {
			out[i] = pid.ReplaceAllString(line, "${1} "+testStart+" ")
		}
	}
	return strings.Join(out, "")
}

func listing(lines string) ProcessLister {
	return func(context.Context) ([]byte, error) { return []byte(withStart(lines)), nil }
}

// Every live `_watch <job>` process under the jobs root must carry its own
// pid's token; an older binary never writes one, locked or not.
func TestHubCloseWatchersReady_t4652(t *testing.T) {
	jobs := t.TempDir()
	if e := os.MkdirAll(filepath.Join(jobs, "job-a", watchCapsDir), 0o700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(jobs, "job-a", watchCapsDir, "101"), []byte(watcherCapsLine(testStart)), 0o600); e != nil {
		t.Fatal(e)
	}
	for name, test := range map[string]struct {
		ps   string
		want bool
	}{
		"aware watcher":           {"  101 /usr/local/bin/panewire _watch job-a --rearmed\n", true},
		"older binary, same job":  {"  101 /usr/local/bin/panewire _watch job-a\n  202 /old/panewire _watch job-a\n", false},
		"stale token, other pid":  {"  303 /old/panewire _watch job-a\n", false},
		"pid reused after a kill": {"  101 Mon Nov 16 09:00:00 2026 /old/panewire _watch job-a\n", false},
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
		if b, e := os.ReadFile(path); e == nil && strings.HasPrefix(string(b), WatcherCapsToken+" ") {
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
